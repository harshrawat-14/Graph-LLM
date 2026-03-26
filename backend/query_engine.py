#!/usr/bin/env python3
"""
Query Engine
============
5-layer NL→SQL query engine with guardrails, entity extraction, schema-aware
SQL generation via Gemini, and result formatting.

Layers:
1. Domain guardrail (keyword check — blocks off-topic before LLM)
2. Entity extraction (regex IDs + fuzzy name matching)
3. Schema context builder (generates relevant DDL subset)
4. LLM SQL generator (Gemini structured output)
5. Result formatter (Gemini natural language)
"""

import json
import os
import re
import sys
import sqlite3
from pathlib import Path

# Ensure the backend directory is in the sys.path for internal imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from typing import Optional, Dict, List, Any, Tuple

try:
    from rapidfuzz import fuzz, process as rfprocess
except ImportError:
    fuzz = None
    rfprocess = None

try:
    import google.generativeai as genai
except ImportError:
    genai = None

try:
    from groq import Groq
except ImportError:
    Groq = None
# ─── Layer 1: Domain Guardrail ───────────────────────────────────────────────
#
# Design: Instead of maintaining a fragile whitelist of "allowed" keywords
# (which breaks every time we add a table), we use a small BLOCKLIST of
# clearly off-topic categories.  Everything else is passed through to the
# LLM, which already has its own OUT_OF_DOMAIN check in the system prompt
# and can make much smarter relevance decisions because it sees the full
# schema.
#
# This makes the guardrail:
#   ✓  Zero-maintenance when new tables are added
#   ✓  Very unlikely to block legitimate business queries
#   ✗  Slightly more LLM calls for obvious junk (acceptable trade-off)

OFF_TOPIC_PATTERNS = [
    r"\b(?:weather|forecast|temperature|rain|sunny)\b",
    r"\b(?:recipe|cook|ingredient|calories)\b",
    r"\b(?:sports|cricket|football|basketball|score|goal)\b",
    r"\b(?:movie|song|lyrics|actor|actress|netflix)\b",
    r"\b(?:capital of|president of|prime minister)\b",
    r"\b(?:translate|definition of|meaning of)\b",
    r"\b(?:write me a poem|tell me a joke|hello|hi there|how are you)\b",
]


def is_domain_query(question: str) -> Tuple[bool, str]:
    """
    Lightweight guardrail: only block questions that are OBVIOUSLY off-topic.
    Everything else goes to the LLM which has its own OUT_OF_DOMAIN check.
    """
    q_lower = question.lower().strip()

    # Very short queries (< 3 chars) are likely noise
    if len(q_lower) < 3:
        return False, "Query too short"

    # Check against off-topic blocklist
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, q_lower):
            return False, f"Off-topic pattern: {pattern}"

    # If it contains a numeric ID (6+ digits), it's almost certainly domain
    if re.search(r"\b\d{6,}\b", q_lower):
        return True, "Contains business ID pattern"

    # Default: let it through — the LLM will decide
    return True, "Passed to LLM for evaluation"


def classify_intent(question: str) -> str:
    """Classify the intent of a domain question."""
    q = question.lower()

    if any(w in q for w in ["trace", "follow", "path", "flow of", "chain",
                             "journey", "track", "connected to"]):
        return "flow_trace"

    if any(w in q for w in ["incomplete", "missing", "broken", "not billed",
                             "without payment", "no delivery", "not delivered",
                             "not paid", "unpaid"]):
        return "broken_flow"

    if any(w in q for w in ["most", "highest", "count", "total", "sum",
                             "average", "rank", "top", "lowest", "minimum",
                             "maximum", "how many"]):
        return "aggregation"

    return "entity_lookup"


# ─── Layer 2: Entity Extraction ──────────────────────────────────────────────

# Matches common business entity ID patterns
ID_PATTERNS = [
    (r'\b(\d{6,10})\b', None),  # Generic numeric ID (6-10 digits)
    (r'\b([A-Z]-\d{6,})\b', None),  # Prefixed like C-310000108
    (r'(?:order|sales order)\s*#?\s*(\d{5,})', "Order"),
    (r'(?:invoice|billing|billing doc|billing document)\s*#?\s*(\d{5,})', "Invoice"),
    (r'(?:delivery)\s*#?\s*(\d{5,})', "Delivery"),
    (r'(?:customer)\s*#?\s*(\d{5,})', "Customer"),
    (r'(?:payment)\s*#?\s*(\d{5,})', "Payment"),
    (r'(?:product)\s*#?\s*([A-Z0-9]{5,})', "Product"),
]


def extract_entity_references(question: str, db_conn: sqlite3.Connection) -> List[dict]:
    """
    Extract entity references from a question using regex + fuzzy matching.
    Returns list of {entity_type, entity_id, confidence, graph_node_id}.
    """
    refs = []
    q_upper = question.upper()
    q = question

    # 1. Regex-based ID extraction
    for pattern, entity_type in ID_PATTERNS:
        for match in re.finditer(pattern, q, re.IGNORECASE):
            entity_id = match.group(1)
            ref = _resolve_id(entity_id, entity_type, db_conn)
            if ref:
                refs.append(ref)

    # 2. Fuzzy name matching against customers and products
    if rfprocess and fuzz:
        refs.extend(_fuzzy_match_names(question, db_conn))

    # Deduplicate
    seen = set()
    unique_refs = []
    for r in refs:
        key = (r["entity_type"], r["entity_id"])
        if key not in seen:
            seen.add(key)
            unique_refs.append(r)

    return unique_refs


def _resolve_id(entity_id: str, hint_type: Optional[str],
                db_conn: sqlite3.Connection) -> Optional[dict]:
    """Try to resolve an ID to a specific entity in the database."""
    # Tables to search with their type names and graph prefixes
    search_order = [
        ("orders", "Order", "O"),
        ("invoices", "Invoice", "I"),
        ("deliveries", "Delivery", "D"),
        ("customers", "Customer", "C"),
        ("payments", "Payment", "PAY"),
        ("products", "Product", "P"),
    ]

    # If we have a type hint, search that table first
    if hint_type:
        for table, etype, prefix in search_order:
            if etype == hint_type:
                search_order.remove((table, etype, prefix))
                search_order.insert(0, (table, etype, prefix))
                break

    for table, etype, prefix in search_order:
        row = db_conn.execute(
            f"SELECT id FROM {table} WHERE id = ?", (entity_id,)
        ).fetchone()
        if row:
            return {
                "entity_type": etype,
                "entity_id": entity_id,
                "graph_node_id": f"{prefix}-{entity_id}",
                "confidence": 0.95,
            }

    # Also check accounting_document in invoices (for payment queries)
    row = db_conn.execute(
        "SELECT id FROM invoices WHERE accounting_document = ?", (entity_id,)
    ).fetchone()
    if row:
        return {
            "entity_type": "Invoice",
            "entity_id": row[0],
            "graph_node_id": f"I-{row[0]}",
            "confidence": 0.85,
            "note": f"Matched via accounting_document={entity_id}",
        }

    return None


def _fuzzy_match_names(question: str, db_conn: sqlite3.Connection) -> List[dict]:
    """Fuzzy match entity names (customers, products) in the question."""
    refs = []

    # Load entity names
    customers = {row[0]: row[1] for row in
                 db_conn.execute("SELECT id, name FROM customers WHERE name IS NOT NULL")}
    products = {row[0]: row[1] for row in
                db_conn.execute("SELECT id, name FROM products WHERE name IS NOT NULL")}

    q_lower = question.lower()

    # Check customers
    if customers:
        all_names = list(customers.values())
        all_ids = list(customers.keys())
        matches = rfprocess.extract(q_lower, all_names, scorer=fuzz.partial_ratio, limit=3)
        for name, score, idx in matches:
            if score >= 70:
                cid = all_ids[idx]
                refs.append({
                    "entity_type": "Customer",
                    "entity_id": cid,
                    "graph_node_id": f"C-{cid}",
                    "confidence": score / 100.0,
                    "matched_name": name,
                })

    # Check products
    if products:
        all_names = list(products.values())
        all_ids = list(products.keys())
        matches = rfprocess.extract(q_lower, all_names, scorer=fuzz.partial_ratio, limit=3)
        for name, score, idx in matches:
            if score >= 70:
                pid = all_ids[idx]
                refs.append({
                    "entity_type": "Product",
                    "entity_id": pid,
                    "graph_node_id": f"P-{pid}",
                    "confidence": score / 100.0,
                    "matched_name": name,
                })

    return refs


# ─── Layer 3: Schema Context Builder ────────────────────────────────────────

FULL_SCHEMA_DDL = """
-- customers: Business partners / buyers
CREATE TABLE customers (
    id TEXT PRIMARY KEY,
    name TEXT,
    country TEXT,
    city TEXT,
    address_id TEXT,
    created_at TEXT
);

-- products: Materials / items that can be ordered
CREATE TABLE products (
    id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    unit_price REAL,
    currency TEXT
);

-- addresses: Physical addresses for business partners
CREATE TABLE addresses (
    id TEXT PRIMARY KEY,
    street TEXT,
    city TEXT,
    country TEXT,
    postal_code TEXT
);

-- orders: Sales orders placed by customers
CREATE TABLE orders (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_date TEXT,
    status TEXT,
    total_amount REAL,
    currency TEXT
);

-- order_items: Line items within a sales order
CREATE TABLE order_items (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    product_id TEXT,
    quantity REAL,
    unit_price REAL,
    line_amount REAL
);

-- deliveries: Outbound delivery documents
CREATE TABLE deliveries (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    plant TEXT,
    delivery_date TEXT,
    status TEXT
);

-- invoices: Billing documents
CREATE TABLE invoices (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    delivery_id TEXT,
    customer_id TEXT,
    amount REAL,
    currency TEXT,
    issue_date TEXT,
    status TEXT,
    accounting_document TEXT
);

-- payments: Accounts receivable payment records
CREATE TABLE payments (
    id TEXT PRIMARY KEY,
    invoice_id TEXT,
    amount REAL,
    payment_date TEXT,
    method TEXT
);

-- journal_entries: General ledger journal entries
CREATE TABLE journal_entries (
    id TEXT PRIMARY KEY,
    invoice_id TEXT,
    entry_date TEXT,
    debit_amount REAL,
    credit_amount REAL,
    account_code TEXT
);

-- ── Enrichment Tables ──

-- schedule_lines: Confirmed delivery dates and quantities per order item
CREATE TABLE schedule_lines (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    order_item_id TEXT,
    requested_date TEXT,
    confirmed_date TEXT,
    confirmed_quantity REAL,
    delivery_block TEXT
);

-- plants: Manufacturing or shipping locations
CREATE TABLE plants (
    id TEXT PRIMARY KEY,
    name TEXT,
    country TEXT,
    city TEXT,
    company_code TEXT
);

-- storage_locations: Specific warehouse slots within a plant
CREATE TABLE storage_locations (
    id TEXT PRIMARY KEY,
    plant_id TEXT,
    location_code TEXT,
    name TEXT
);

-- sales_areas: Sales organization, distribution channel, and division
CREATE TABLE sales_areas (
    id TEXT PRIMARY KEY,
    sales_org TEXT,
    dist_channel TEXT,
    division TEXT,
    name TEXT
);

-- customer_sales_areas: Customer specific settings for a sales area
CREATE TABLE customer_sales_areas (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    sales_org TEXT,
    dist_channel TEXT,
    division TEXT,
    currency TEXT,
    payment_terms TEXT
);

-- pricing_conditions: Discounts, surcharges, and taxes for orders
CREATE TABLE pricing_conditions (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    condition_type TEXT,
    amount REAL,
    currency TEXT
);

-- credit_management: Customer credit limits and blocks
CREATE TABLE credit_management (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    credit_limit REAL,
    currency TEXT,
    credit_exposure REAL,
    credit_block TEXT,
    last_check_date TEXT
);

-- material_docs: Goods movements (GI/GR) documents
CREATE TABLE material_docs (
    id TEXT PRIMARY KEY,
    delivery_id TEXT,
    posting_date TEXT,
    movement_type TEXT,
    quantity REAL,
    plant TEXT,
    storage_loc TEXT
);

-- customer_material_info: Maps customer part numbers to SAP material IDs
CREATE TABLE customer_material_info (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    product_id TEXT,
    customer_mat_num TEXT,
    sales_org TEXT,
    dist_channel TEXT
);

-- partner_functions: Payer, Ship-to, and Bill-to roles for an order
CREATE TABLE partner_functions (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    partner_func TEXT,
    partner_id TEXT
);
"""

# Table-level schema for targeted context
TABLE_SCHEMAS = {}
_current_table = None
for line in FULL_SCHEMA_DDL.strip().split("\n"):
    if line.strip().startswith("CREATE TABLE"):
        _current_table = re.search(r"CREATE TABLE (\w+)", line).group(1)
        TABLE_SCHEMAS[_current_table] = ""
    if _current_table:
        TABLE_SCHEMAS[_current_table] += line + "\n"
    if line.strip() == ");":
        _current_table = None

# Map of question keywords → relevant tables
TABLE_RELEVANCE = {
    "customer": ["customers", "orders", "addresses", "credit_management", "customer_sales_areas", "customer_material_info"],
    "order": ["orders", "order_items", "customers", "schedule_lines", "pricing_conditions", "partner_functions"],
    "delivery": ["deliveries", "orders", "material_docs"],
    "invoice": ["invoices", "orders", "deliveries", "payments", "journal_entries"],
    "billing": ["invoices", "orders", "deliveries", "payments"],
    "payment": ["payments", "invoices"],
    "product": ["products", "order_items", "orders", "customer_material_info"],
    "plant": ["plants", "deliveries", "storage_locations", "material_docs"],
    "warehouse": ["storage_locations", "plants", "material_docs"],
    "credit": ["credit_management", "customers"],
    "price": ["pricing_conditions", "order_items"],
    "discount": ["pricing_conditions"],
    "schedule": ["schedule_lines", "orders"],
    "confirmed": ["schedule_lines"],
    "journal": ["journal_entries", "invoices"],
    "address": ["addresses", "customers"],
    "amount": ["orders", "invoices", "payments", "order_items"],
    "revenue": ["orders", "invoices", "payments"],
    "flow": ["orders", "deliveries", "invoices", "payments"],
    "trace": ["orders", "deliveries", "invoices", "payments"],
    "broken": ["orders", "deliveries", "invoices", "payments"],
    "missing": ["orders", "deliveries", "invoices", "payments"],
    "incomplete": ["orders", "deliveries", "invoices", "payments"],
}


def build_schema_context(question: str) -> str:
    """Build a filtered DDL context based on the question's content."""
    q_lower = question.lower()
    relevant_tables = set()

    for keyword, tables in TABLE_RELEVANCE.items():
        if keyword in q_lower:
            relevant_tables.update(tables)

    # If no specific tables detected, include all
    if not relevant_tables:
        return FULL_SCHEMA_DDL

    # Always include core bridge tables for joins
    if "orders" in relevant_tables:
        relevant_tables.add("customers")
    if "invoices" in relevant_tables:
        relevant_tables.update(["orders", "deliveries"])
    if "payments" in relevant_tables:
        relevant_tables.add("invoices")

    ddl_parts = []
    for table in relevant_tables:
        if table in TABLE_SCHEMAS:
            ddl_parts.append(TABLE_SCHEMAS[table])

    return "\n".join(ddl_parts)


# ─── Layer 4: LLM SQL Generator ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are a SQL query generator for a business operations database (SAP Order-to-Cash).

DATABASE SCHEMA:
{schema_ddl}

STRICT RULES:
1. Generate ONLY executable SQLite-compatible SQL.
2. ONLY use table names and column names that exist in the schema above.
3. If the question is not about the business dataset described above, output exactly: {{"error": "OUT_OF_DOMAIN"}}
4. If the question cannot be answered from the available schema, output: {{"error": "UNSUPPORTED_QUERY", "reason": "..."}}
5. NEVER invent data. NEVER answer from memory. ONLY translate to SQL.
6. Output ONLY a valid JSON object. No explanation text outside the JSON.
7. For date filtering, dates are in YYYY-MM-DD format.
8. The invoices.status can be 'PAID', 'UNPAID', or 'CANCELLED'.
9. When joining payments to invoices, use payments.invoice_id = invoices.id.
10. When counting or aggregating, always use appropriate GROUP BY.

OUTPUT FORMAT:
{{
  "intent": "aggregation|flow_trace|entity_lookup|broken_flow",
  "sql": "<SQLite query>",
  "tables_used": ["table1"],
  "explanation": "<one sentence describing what the SQL computes>",
  "requires_graph_traversal": false
}}

FEW-SHOT EXAMPLES:

Q: "Which products appear in the most billing documents?"
A: {{"intent":"aggregation","sql":"SELECT p.name, p.id, COUNT(DISTINCT i.id) AS invoice_count FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id JOIN invoices i ON i.order_id = o.id GROUP BY p.id, p.name ORDER BY invoice_count DESC LIMIT 10","tables_used":["products","order_items","orders","invoices"],"explanation":"Counts distinct invoices per product through order item joins","requires_graph_traversal":false}}

Q: "Find orders delivered but never billed"
A: {{"intent":"broken_flow","sql":"SELECT o.id, o.customer_id, o.order_date, o.total_amount FROM orders o JOIN deliveries d ON d.order_id = o.id LEFT JOIN invoices i ON i.order_id = o.id WHERE i.id IS NULL","tables_used":["orders","deliveries","invoices"],"explanation":"Orders with a delivery but no corresponding invoice","requires_graph_traversal":false}}

Q: "Show all plants and their cities"
A: {{"intent":"entity_lookup","sql":"SELECT id, name, city, country FROM plants","tables_used":["plants"],"explanation":"List of all manufacturing/shipping plants and their locations","requires_graph_traversal":false}}

Q: "Which orders have a confirmed quantity less than requested?"
A: {{"intent":"broken_flow","sql":"SELECT o.id, oi.product_id, oi.quantity as requested, sl.confirmed_quantity FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN schedule_lines sl ON sl.order_item_id = oi.id WHERE sl.confirmed_quantity < oi.quantity","tables_used":["orders","order_items","schedule_lines"],"explanation":"Identifies orders where SAP has not confirmed the full requested quantity","requires_graph_traversal":false}}

Q: "Check credit exposure for customer 105001"
A: {{"intent":"entity_lookup","sql":"SELECT c.name, cm.credit_limit, cm.credit_exposure, cm.credit_block FROM customers c JOIN credit_management cm ON cm.customer_id = c.id WHERE c.id = '105001'","tables_used":["customers","credit_management"],"explanation":"Lookup credit limit and exposure for customer 105001","requires_graph_traversal":false}}

Q: "What is the weather in Delhi?"
A: {{"error": "OUT_OF_DOMAIN"}}

Q: "Show details of order 740506"
A: {{"intent":"entity_lookup","sql":"SELECT o.*, c.name as customer_name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.id = '740506'","tables_used":["orders","customers"],"explanation":"Lookup order 740506 with customer info","requires_graph_traversal":false}}

A: {{"error": "OUT_OF_DOMAIN"}}

Q: "How many orders does each customer have?"
A: {{"intent":"aggregation","sql":"SELECT c.id, c.name, COUNT(o.id) as order_count, SUM(o.total_amount) as total_value FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.id, c.name ORDER BY order_count DESC","tables_used":["customers","orders"],"explanation":"Count of orders and total value per customer","requires_graph_traversal":false}}

Q: "Show all unpaid invoices"
A: {{"intent":"entity_lookup","sql":"SELECT i.id, i.amount, i.currency, i.issue_date, i.customer_id, c.name as customer_name FROM invoices i LEFT JOIN customers c ON c.id = i.customer_id WHERE i.status = 'UNPAID' ORDER BY i.amount DESC","tables_used":["invoices","customers"],"explanation":"All invoices with UNPAID status, ordered by amount","requires_graph_traversal":false}}

Q: "Trace the flow of order 740506"
A: {{"intent":"flow_trace","sql":"SELECT 'Order' as step, o.id, o.order_date as date, o.total_amount as amount, o.status FROM orders o WHERE o.id = '740506' UNION ALL SELECT 'Delivery', d.id, d.delivery_date, NULL, d.status FROM deliveries d WHERE d.order_id = '740506' UNION ALL SELECT 'Invoice', i.id, i.issue_date, i.amount, i.status FROM invoices i WHERE i.order_id = '740506' UNION ALL SELECT 'Payment', p.id, p.payment_date, p.amount, NULL FROM payments p JOIN invoices i ON p.invoice_id = i.id WHERE i.order_id = '740506'","tables_used":["orders","deliveries","invoices","payments"],"explanation":"Full O2C flow trace of order 740506","requires_graph_traversal":true}}
"""

FORMATTER_PROMPT = """You are a data response formatter for a business operations system.
Here is a SQL query result: {json_rows}
The user asked: {original_question}
The query computed: {explanation}

Write a clear, factual 2-3 sentence answer using ONLY the data provided.
Do NOT add any information not present in the data.
If the result is empty, say "No matching records found in the dataset."
Format numbers with appropriate separators. Use bullet points for lists of more than 3 items.
Mention specific IDs and names when relevant."""


class QueryEngine:
    def __init__(self, db_path: str, unused_api_key_param: Optional[str] = None):
        self.db_path = db_path
        self.gemini_model = None
        self.groq_client = None
        self.sessions = {}

        from config import LLM_API_KEY, LLM_MODEL, LLM_PROVIDER
        self.provider = LLM_PROVIDER
        self.model_name = LLM_MODEL

        if LLM_PROVIDER == "groq" and Groq and LLM_API_KEY:
            self.groq_client = Groq(api_key=LLM_API_KEY)
        elif LLM_PROVIDER == "gemini" and genai and LLM_API_KEY:
            genai.configure(api_key=LLM_API_KEY)
            self.gemini_model = genai.GenerativeModel(LLM_MODEL)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def process_query(self, question: str, session_id: str = "") -> dict:
        """
        Main entry point: process a natural language question through all 5 layers.
        """
        # Layer 1: Domain guardrail
        is_domain, reason = is_domain_query(question)
        if not is_domain:
            return {
                "answer": "That doesn't seem related to the SAP business dataset. Try asking about orders, customers, deliveries, invoices, payments, products, plants, schedules, or credit management.",
                "sql_used": None,
                "data": [],
                "highlighted_node_ids": [],
                "intent": "out_of_domain",
                "confidence": "rejected",
                "error": "OUT_OF_DOMAIN",
            }

        # Layer 2: Entity extraction
        conn = self._get_conn()
        try:
            entity_refs = extract_entity_references(question, conn)
        finally:
            conn.close()

        intent = classify_intent(question)

        # Layer 3: Schema context
        schema_context = build_schema_context(question)

        # Initialize or fetch session context
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                "last_entity_type": None,
                "last_entity_id": None,
                "last_query_tables": [],
                "history": []
            }
        
        session_ctx = self.sessions[session_id]
        if entity_refs:
            session_ctx["last_entity_type"] = entity_refs[0]["entity_type"]
            session_ctx["last_entity_id"] = entity_refs[0]["entity_id"]

        session_ctx_str = json.dumps(session_ctx)

        # Layer 4: Generate SQL via LLM
        if not self.gemini_model and not self.groq_client:
            return self._fallback_query(question, intent, entity_refs)

        try:
            llm_result = self._generate_sql(question, schema_context, session_ctx_str)
            session_ctx["last_query_tables"] = llm_result.get("tables_used", [])

        except Exception as e:
            return {
                "answer": f"Error generating query: {str(e)}",
                "sql_used": None,
                "data": [],
                "highlighted_node_ids": [r["graph_node_id"] for r in entity_refs],
                "intent": intent,
                "confidence": "error",
                "error": str(e),
            }

        # Handle LLM errors
        if "error" in llm_result:
            error_type = llm_result["error"]
            if error_type == "OUT_OF_DOMAIN":
                return {
                    "answer": "This question doesn't appear to be about the business dataset. I can help with questions about customers, orders, deliveries, invoices, payments, products, plants, schedule lines, and credit management.",
                    "sql_used": None,
                    "data": [],
                    "highlighted_node_ids": [],
                    "intent": "out_of_domain",
                    "confidence": "rejected",
                    "error": "OUT_OF_DOMAIN",
                }
            return {
                "answer": f"Unable to process: {llm_result.get('reason', error_type)}",
                "sql_used": None,
                "data": [],
                "highlighted_node_ids": [r["graph_node_id"] for r in entity_refs],
                "intent": intent,
                "confidence": "error",
                "error": error_type,
            }

        # Execute SQL
        sql = llm_result.get("sql", "")
        explanation = llm_result.get("explanation", "")

        conn = self._get_conn()
        try:
            try:
                cursor = conn.execute(sql)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            except Exception as e:
                return {
                    "answer": f"SQL execution error: {str(e)}",
                    "sql_used": sql,
                    "data": [],
                    "highlighted_node_ids": [r["graph_node_id"] for r in entity_refs],
                    "intent": llm_result.get("intent", intent),
                    "confidence": "error",
                    "error": str(e),
                }
        finally:
            conn.close()

        # Collect highlighted node IDs from results
        highlighted = self._extract_node_ids_from_results(rows, entity_refs)

        # Layer 5: Format result
        if rows:
            answer = self._format_result(question, rows[:50], explanation)
            confidence = "data_backed"
        else:
            answer = "No matching records found in the dataset."
            confidence = "no_data"

        # Update visual representation history
        session_ctx["history"].append({"role": "user", "content": question})
        session_ctx["history"].append({"role": "assistant", "content": answer})
        if len(session_ctx["history"]) > 6:
            session_ctx["history"] = session_ctx["history"][-6:]

        return {
            "answer": answer,
            "sql_used": sql,
            "data": rows[:100],  # Cap at 100 rows for response
            "highlighted_node_ids": highlighted,
            "intent": llm_result.get("intent", intent),
            "confidence": confidence,
            "error": None,
        }

    def _generate_sql(self, question: str, schema_ddl: str, session_context: str) -> dict:
        """Call LLM to generate SQL from natural language."""
        prompt = SYSTEM_PROMPT.format(schema_ddl=schema_ddl)
        user_msg = f"USER QUESTION: {question}\nSESSION CONTEXT: {session_context}"

        if self.provider == "gemini" and self.gemini_model:
            response = self.gemini_model.generate_content(
                [{"role": "user", "parts": [prompt + "\n\n" + user_msg]}],
                generation_config=genai.types.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=512,
                    response_mime_type="application/json"
                ),
            )
            text = response.text.strip()
        elif self.provider == "groq" and self.groq_client:
            response = self.groq_client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_msg}
                ],
                temperature=0.0,
                max_tokens=1024,
                response_format={"type": "json_object"}
            )
            text = response.choices[0].message.content.strip()
        else:
            raise ValueError("No LLM provider configured")

        try:
            return json.loads(text)
        except Exception as e:
            raise ValueError(f"Could not parse LLM JSON: {e} | Raw: {text[:200]}")

    def _format_result(self, question: str, rows: list, explanation: str) -> str:
        """Call LLM to format SQL results into natural language."""
        if not self.gemini_model and not self.groq_client:
            return self._basic_format(rows, explanation)

        try:
            prompt = FORMATTER_PROMPT.format(
                json_rows=json.dumps(rows[:20], default=str),
                original_question=question,
                explanation=explanation,
            )
            
            if self.provider == "gemini" and self.gemini_model:
                response = self.gemini_model.generate_content(
                    [{"role": "user", "parts": [prompt]}],
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.3,
                        max_output_tokens=256,
                    ),
                )
                return response.text.strip()
            elif self.provider == "groq" and self.groq_client:
                response = self.groq_client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=512,
                )
                return response.choices[0].message.content.strip()
            else:
                return self._basic_format(rows, explanation)
        except Exception:
            return self._basic_format(rows, explanation)

    def _basic_format(self, rows: list, explanation: str) -> str:
        """Basic formatting fallback when Gemini is unavailable."""
        if not rows:
            return "No matching records found."
        result = f"Found {len(rows)} result(s). {explanation}\n\n"
        for i, row in enumerate(rows[:5]):
            parts = [f"{k}: {v}" for k, v in row.items() if v is not None]
            result += f"• {', '.join(parts)}\n"
        if len(rows) > 5:
            result += f"\n...and {len(rows) - 5} more rows."
        return result

    def _extract_node_ids_from_results(self, rows: list, entity_refs: list) -> list:
        """Extract graph node IDs from SQL result rows."""
        node_ids = set()

        # Add entity refs from question
        for ref in entity_refs:
            node_ids.add(ref["graph_node_id"])

        # Scan result rows for IDs that correspond to graph nodes
        prefix_map = {
            "id": None,  # Will try all prefixes
            "customer_id": "C",
            "order_id": "O",
            "product_id": "P",
            "delivery_id": "D",
            "invoice_id": "I",
        }

        for row in rows[:50]:  # Cap scanning
            for col, prefix in prefix_map.items():
                val = row.get(col)
                if val:
                    if prefix:
                        node_ids.add(f"{prefix}-{val}")
                    else:
                        # Try common prefixes for generic 'id' column
                        for p in ["O", "C", "I", "D", "P", "PAY"]:
                            node_ids.add(f"{p}-{val}")

            # Also check for customer_name → try to find customer
            # Check step column for flow traces
            step = row.get("step", "")
            rid = row.get("id")
            if step and rid:
                step_prefix = {
                    "Order": "O", "Delivery": "D",
                    "Invoice": "I", "Payment": "PAY"
                }.get(step)
                if step_prefix:
                    node_ids.add(f"{step_prefix}-{rid}")

        return list(node_ids)

    def _fallback_query(self, question: str, intent: str, entity_refs: list) -> dict:
        """Fallback when Gemini is not available — run simple SQL based on intent."""
        conn = self._get_conn()
        try:
            if intent == "broken_flow":
                rows = [dict(r) for r in conn.execute("""
                    SELECT 'Orders without delivery' as category, COUNT(*) as count
                    FROM orders o LEFT JOIN deliveries d ON d.order_id = o.id WHERE d.id IS NULL
                    UNION ALL
                    SELECT 'Deliveries without invoice', COUNT(*)
                    FROM deliveries d LEFT JOIN invoices i ON i.delivery_id = d.id WHERE i.id IS NULL
                    UNION ALL
                    SELECT 'Invoices without payment', COUNT(*)
                    FROM invoices i LEFT JOIN payments p ON p.invoice_id = i.id
                    WHERE p.id IS NULL AND i.status != 'CANCELLED'
                """)]
                return {
                    "answer": self._basic_format(rows, "Broken flow summary"),
                    "sql_used": "broken flow summary queries",
                    "data": rows,
                    "highlighted_node_ids": [r["graph_node_id"] for r in entity_refs],
                    "intent": intent,
                    "confidence": "data_backed",
                    "error": None,
                }

            if entity_refs:
                ref = entity_refs[0]
                etype = ref["entity_type"]
                eid = ref["entity_id"]
                table_map = {
                    "Customer": "customers", "Order": "orders",
                    "Invoice": "invoices", "Delivery": "deliveries",
                    "Payment": "payments", "Product": "products",
                }
                table = table_map.get(etype, "orders")
                rows = [dict(r) for r in conn.execute(
                    f"SELECT * FROM {table} WHERE id = ?", (eid,)
                )]
                return {
                    "answer": self._basic_format(rows, f"Details for {etype} {eid}"),
                    "sql_used": f"SELECT * FROM {table} WHERE id = '{eid}'",
                    "data": rows,
                    "highlighted_node_ids": [ref["graph_node_id"]],
                    "intent": intent,
                    "confidence": "data_backed" if rows else "no_data",
                    "error": None,
                }

            return {
                "answer": f"I understand your question but need the {self.provider.upper()} API to generate the appropriate query. Please set the appropriate API key environment variable.",
                "sql_used": None,
                "data": [],
                "highlighted_node_ids": [],
                "intent": intent,
                "confidence": "error",
                "error": "NO_LLM_CONFIGURED",
            }
        finally:
            conn.close()


# ─── Semantic Search ─────────────────────────────────────────────────────────

def search_entities(query: str, db_path: str, limit: int = 10) -> List[dict]:
    """Search for entities by name using fuzzy matching."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        results = []

        # Search customers
        for row in conn.execute("SELECT id, name FROM customers WHERE name IS NOT NULL"):
            r = dict(row)
            if rfprocess and fuzz:
                score = fuzz.partial_ratio(query.lower(), (r["name"] or "").lower())
            else:
                score = 100 if query.lower() in (r["name"] or "").lower() else 0
            if score >= 50:
                results.append({
                    "entity_type": "Customer",
                    "id": r["id"],
                    "name": r["name"],
                    "graph_node_id": f"C-{r['id']}",
                    "score": score,
                })

        # Search products
        for row in conn.execute("SELECT id, name FROM products WHERE name IS NOT NULL"):
            r = dict(row)
            if rfprocess and fuzz:
                score = fuzz.partial_ratio(query.lower(), (r["name"] or "").lower())
            else:
                score = 100 if query.lower() in (r["name"] or "").lower() else 0
            if score >= 50:
                results.append({
                    "entity_type": "Product",
                    "id": r["id"],
                    "name": r["name"],
                    "graph_node_id": f"P-{r['id']}",
                    "score": score,
                })

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:limit]
    finally:
        conn.close()
