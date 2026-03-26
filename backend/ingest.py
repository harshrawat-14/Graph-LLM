#!/usr/bin/env python3
"""
SAP Order-to-Cash Data Ingestion Pipeline
==========================================
Reads JSONL files from sap-o2c-data/ folders, normalizes SAP field names
to a clean relational schema, and loads everything into SQLite.
"""

import json
import glob
import os
import sqlite3
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Optional, List, Dict

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_SOURCE = BASE_DIR / "sap-o2c-data"
DB_PATH = BASE_DIR / "data" / "business.db"

# ─── JSONL Reader ────────────────────────────────────────────────────────────

def read_jsonl_folder(folder_name: str) -> List[dict]:
    """Read all part-*.jsonl files from a folder and concatenate records."""
    folder_path = DATA_SOURCE / folder_name
    records = []
    pattern = str(folder_path / "part-*.jsonl")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"  ⚠ No part-*.jsonl files found in {folder_name}/")
        return records
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"  ⚠ JSON parse error in {os.path.basename(filepath)} line {line_num}: {e}")
    print(f"  ✓ {folder_name}: {len(records)} records from {len(files)} file(s)")
    return records


# ─── Normalization Helpers ───────────────────────────────────────────────────

def normalize_str(val) -> Optional[str]:
    """Strip whitespace, convert empty strings to None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def normalize_id(val) -> Optional[str]:
    """Convert IDs to stripped strings."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def normalize_date(val) -> Optional[str]:
    """Convert ISO date strings to clean YYYY-MM-DD format."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        # Handle ISO format like "2025-03-31T00:00:00.000Z"
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return s


def normalize_datetime(val) -> Optional[str]:
    """Convert ISO datetime strings to clean format."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        return s


def normalize_time_obj(val) -> Optional[str]:
    """Convert {hours, minutes, seconds} object to HH:MM:SS string."""
    if val is None or not isinstance(val, dict):
        return None
    h = val.get("hours", 0)
    m = val.get("minutes", 0)
    s = val.get("seconds", 0)
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"


def normalize_amount(val) -> Optional[float]:
    """Convert amount strings to float."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def to_raw_json(record: dict) -> str:
    """Serialize a record to JSON for raw_source column."""
    return json.dumps(record, default=str, ensure_ascii=False)


# ─── Schema DDL ──────────────────────────────────────────────────────────────

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS customers (
    id TEXT PRIMARY KEY,
    name TEXT,
    country TEXT,
    city TEXT,
    address_id TEXT,
    created_at TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    unit_price REAL,
    currency TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS addresses (
    id TEXT PRIMARY KEY,
    street TEXT,
    city TEXT,
    country TEXT,
    postal_code TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES customers(id),
    order_date TEXT,
    status TEXT,
    total_amount REAL,
    currency TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(id),
    product_id TEXT REFERENCES products(id),
    quantity REAL,
    unit_price REAL,
    line_amount REAL,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(id),
    plant TEXT,
    delivery_date TEXT,
    status TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
    id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(id),
    delivery_id TEXT REFERENCES deliveries(id),
    customer_id TEXT REFERENCES customers(id),
    amount REAL,
    currency TEXT,
    issue_date TEXT,
    status TEXT,
    accounting_document TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY,
    invoice_id TEXT REFERENCES invoices(id),
    amount REAL,
    payment_date TEXT,
    method TEXT,
    raw_source TEXT
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id TEXT PRIMARY KEY,
    invoice_id TEXT,
    entry_date TEXT,
    debit_amount REAL,
    credit_amount REAL,
    account_code TEXT,
    raw_source TEXT
);
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_order ON deliveries(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_order ON invoices(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_delivery ON invoices(delivery_id);
CREATE INDEX IF NOT EXISTS idx_invoices_customer ON invoices(customer_id);
CREATE INDEX IF NOT EXISTS idx_invoices_accounting_doc ON invoices(accounting_document);
CREATE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id);
CREATE INDEX IF NOT EXISTS idx_journal_entries_invoice ON journal_entries(invoice_id);
"""


# ─── Ingestion Functions ─────────────────────────────────────────────────────

def ingest_addresses(conn: sqlite3.Connection) -> Dict[str, dict]:
    """Ingest business_partner_addresses → addresses table. Returns address lookup by businessPartner."""
    print("\n── Ingesting addresses ──")
    records = read_jsonl_folder("business_partner_addresses")
    address_by_bp = {}  # businessPartner → address record
    
    for r in records:
        bp = normalize_id(r.get("businessPartner"))
        addr_id = normalize_id(r.get("addressId"))
        if not addr_id:
            continue
        
        row = {
            "id": addr_id,
            "street": normalize_str(r.get("streetName")),
            "city": normalize_str(r.get("cityName")),
            "country": normalize_str(r.get("country")),
            "postal_code": normalize_str(r.get("postalCode")),
            "raw_source": to_raw_json(r),
        }
        
        conn.execute(
            "INSERT OR REPLACE INTO addresses (id, street, city, country, postal_code, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (row["id"], row["street"], row["city"], row["country"], row["postal_code"], row["raw_source"])
        )
        
        if bp:
            address_by_bp[bp] = row
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
    print(f"  → addresses: {count} rows")
    return address_by_bp


def ingest_customers(conn: sqlite3.Connection, address_by_bp: dict[str, dict]):
    """Ingest business_partners → customers table."""
    print("\n── Ingesting customers ──")
    records = read_jsonl_folder("business_partners")
    
    for r in records:
        bp_id = normalize_id(r.get("businessPartner"))
        if not bp_id:
            continue
        
        addr = address_by_bp.get(bp_id, {})
        
        conn.execute(
            "INSERT OR REPLACE INTO customers (id, name, country, city, address_id, created_at, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                bp_id,
                normalize_str(r.get("businessPartnerFullName")),
                addr.get("country"),
                addr.get("city"),
                addr.get("id"),
                normalize_date(r.get("creationDate")),
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"  → customers: {count} rows")


def ingest_products(conn: sqlite3.Connection):
    """Ingest products + product_descriptions → products table."""
    print("\n── Ingesting products ──")
    products = read_jsonl_folder("products")
    descriptions = read_jsonl_folder("product_descriptions")
    
    # Build description lookup (filter to English)
    desc_map = {}
    for d in descriptions:
        pid = normalize_id(d.get("product"))
        lang = normalize_str(d.get("language"))
        if pid and lang == "EN":
            desc_map[pid] = normalize_str(d.get("productDescription"))
    
    for r in products:
        pid = normalize_id(r.get("product"))
        if not pid:
            continue
        
        conn.execute(
            "INSERT OR REPLACE INTO products (id, name, category, unit_price, currency, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                pid,
                desc_map.get(pid, normalize_str(r.get("productOldId"))),
                normalize_str(r.get("productGroup")),
                None,  # No unit price in raw product data
                None,
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    print(f"  → products: {count} rows")


def ingest_orders(conn: sqlite3.Connection):
    """Ingest sales_order_headers → orders table."""
    print("\n── Ingesting orders ──")
    records = read_jsonl_folder("sales_order_headers")
    
    # SAP delivery status mapping
    status_map = {
        "": "OPEN",
        "A": "NOT_DELIVERED",
        "B": "PARTIALLY_DELIVERED",
        "C": "FULLY_DELIVERED",
    }
    
    for r in records:
        order_id = normalize_id(r.get("salesOrder"))
        if not order_id:
            continue
        
        raw_status = normalize_str(r.get("overallDeliveryStatus")) or ""
        status = status_map.get(raw_status, raw_status)
        
        conn.execute(
            "INSERT OR REPLACE INTO orders (id, customer_id, order_date, status, total_amount, currency, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                order_id,
                normalize_id(r.get("soldToParty")),
                normalize_date(r.get("creationDate")),
                status,
                normalize_amount(r.get("totalNetAmount")),
                normalize_str(r.get("transactionCurrency")),
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    print(f"  → orders: {count} rows")


def ingest_order_items(conn: sqlite3.Connection):
    """Ingest sales_order_items → order_items table."""
    print("\n── Ingesting order_items ──")
    records = read_jsonl_folder("sales_order_items")
    
    for r in records:
        order_id = normalize_id(r.get("salesOrder"))
        item_num = normalize_str(r.get("salesOrderItem"))
        if not order_id or not item_num:
            continue
        
        item_id = f"{order_id}-{item_num}"
        quantity = normalize_amount(r.get("requestedQuantity"))
        net_amount = normalize_amount(r.get("netAmount"))
        
        # Calculate unit_price from net_amount / quantity
        unit_price = None
        if net_amount is not None and quantity and quantity > 0:
            unit_price = round(net_amount / quantity, 4)
        
        conn.execute(
            "INSERT OR REPLACE INTO order_items (id, order_id, product_id, quantity, unit_price, line_amount, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                item_id,
                order_id,
                normalize_id(r.get("material")),
                quantity,
                unit_price,
                net_amount,
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
    print(f"  → order_items: {count} rows")


def ingest_deliveries(conn: sqlite3.Connection):
    """
    Ingest outbound_delivery_headers + outbound_delivery_items → deliveries table.
    The order_id link comes from delivery_items.referenceSdDocument (the sales order).
    """
    print("\n── Ingesting deliveries ──")
    headers = read_jsonl_folder("outbound_delivery_headers")
    items = read_jsonl_folder("outbound_delivery_items")
    
    # Build delivery→order mapping from items (use first item's referenceSdDocument)
    delivery_to_order = {}
    delivery_to_plant = {}
    for item in items:
        del_id = normalize_id(item.get("deliveryDocument"))
        order_ref = normalize_id(item.get("referenceSdDocument"))
        plant = normalize_str(item.get("plant"))
        if del_id and order_ref:
            delivery_to_order.setdefault(del_id, order_ref)
        if del_id and plant:
            delivery_to_plant.setdefault(del_id, plant)
    
    # SAP goods movement status mapping
    status_map = {
        "": "NOT_STARTED",
        "A": "NOT_YET_STARTED",
        "B": "PARTIALLY_COMPLETED",
        "C": "COMPLETED",
    }
    
    for r in headers:
        del_id = normalize_id(r.get("deliveryDocument"))
        if not del_id:
            continue
        
        raw_status = normalize_str(r.get("overallGoodsMovementStatus")) or ""
        status = status_map.get(raw_status, raw_status)
        
        # Use actualGoodsMovementDate if available, else creationDate
        delivery_date = normalize_date(r.get("actualGoodsMovementDate")) or normalize_date(r.get("creationDate"))
        
        conn.execute(
            "INSERT OR REPLACE INTO deliveries (id, order_id, plant, delivery_date, status, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                del_id,
                delivery_to_order.get(del_id),
                delivery_to_plant.get(del_id, normalize_str(r.get("shippingPoint"))),
                delivery_date,
                status,
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    print(f"  → deliveries: {count} rows")
    
    no_order = conn.execute("SELECT COUNT(*) FROM deliveries WHERE order_id IS NULL").fetchone()[0]
    if no_order:
        print(f"  ⚠ {no_order} deliveries have no order_id link")


def ingest_invoices(conn: sqlite3.Connection):
    """
    Ingest billing_document_headers + billing_document_items + billing_document_cancellations → invoices table.
    
    Join logic:
    - billing_document_items.referenceSdDocument links to delivery documents
    - billing_document_headers.accountingDocument is stored for payment joins
    - billing_document_cancellations flags cancelled invoices
    """
    print("\n── Ingesting invoices ──")
    headers = read_jsonl_folder("billing_document_headers")
    items = read_jsonl_folder("billing_document_items")
    cancellations = read_jsonl_folder("billing_document_cancellations")
    
    # Build billing→delivery mapping from items
    # referenceSdDocument in billing items points to delivery documents
    billing_to_delivery = {}
    billing_to_order = {}
    for item in items:
        bill_id = normalize_id(item.get("billingDocument"))
        ref_doc = normalize_id(item.get("referenceSdDocument"))
        if bill_id and ref_doc:
            billing_to_delivery.setdefault(bill_id, ref_doc)
    
    # Build delivery→order lookup from DB
    delivery_order_map = {}
    for row in conn.execute("SELECT id, order_id FROM deliveries WHERE order_id IS NOT NULL"):
        delivery_order_map[row[0]] = row[1]
    
    # Build cancellation set
    cancelled_docs = set()
    for c in cancellations:
        bill_id = normalize_id(c.get("billingDocument"))
        if bill_id:
            cancelled_docs.add(bill_id)
        # Also the cancelledBillingDocument field
        cancelled_ref = normalize_id(c.get("cancelledBillingDocument"))
        if cancelled_ref:
            cancelled_docs.add(cancelled_ref)
    
    for r in headers:
        bill_id = normalize_id(r.get("billingDocument"))
        if not bill_id:
            continue
        
        # Determine cancellation status
        is_cancelled = r.get("billingDocumentIsCancelled", False) or bill_id in cancelled_docs
        
        # Link to delivery via billing items
        delivery_id = billing_to_delivery.get(bill_id)
        
        # Resolve order_id: either from delivery→order chain, or from soldToParty-based lookup
        order_id = None
        if delivery_id and delivery_id in delivery_order_map:
            order_id = delivery_order_map[delivery_id]
        
        # Status determination
        status = "CANCELLED" if is_cancelled else "UNPAID"  # Will be updated after payments
        
        conn.execute(
            "INSERT OR REPLACE INTO invoices "
            "(id, order_id, delivery_id, customer_id, amount, currency, issue_date, status, accounting_document, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                bill_id,
                order_id,
                delivery_id,
                normalize_id(r.get("soldToParty")),
                normalize_amount(r.get("totalNetAmount")),
                normalize_str(r.get("transactionCurrency")),
                normalize_date(r.get("billingDocumentDate")),
                status,
                normalize_id(r.get("accountingDocument")),
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    cancelled_count = conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'CANCELLED'").fetchone()[0]
    print(f"  → invoices: {count} rows ({cancelled_count} cancelled)")


def ingest_payments(conn: sqlite3.Connection):
    """
    Ingest payments_accounts_receivable → payments table.
    
    Critical join: payments.clearingAccountingDocument = invoices.accounting_document
    (NOT billingDocument directly)
    """
    print("\n── Ingesting payments ──")
    records = read_jsonl_folder("payments_accounts_receivable")
    
    # Build accounting_document → invoice_id lookup
    acct_doc_to_invoice = {}
    for row in conn.execute("SELECT id, accounting_document FROM invoices WHERE accounting_document IS NOT NULL"):
        acct_doc_to_invoice[row[1]] = row[0]
    
    paid_invoice_ids = set()
    
    for r in records:
        acct_doc = normalize_id(r.get("accountingDocument"))
        if not acct_doc:
            continue
        
        # The clearing accounting document links to the billing doc's accounting document
        clearing_doc = normalize_id(r.get("clearingAccountingDocument"))
        invoice_id = acct_doc_to_invoice.get(clearing_doc) if clearing_doc else None
        
        # Also try direct match: the payment's own accountingDocument might match an invoice
        if not invoice_id:
            invoice_id = acct_doc_to_invoice.get(acct_doc)
        
        payment_id = f"{acct_doc}-{normalize_str(r.get('accountingDocumentItem', '1'))}"
        
        conn.execute(
            "INSERT OR REPLACE INTO payments (id, invoice_id, amount, payment_date, method, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                payment_id,
                invoice_id,
                normalize_amount(r.get("amountInTransactionCurrency")),
                normalize_date(r.get("clearingDate") or r.get("postingDate")),
                normalize_str(r.get("financialAccountType")),
                to_raw_json(r),
            )
        )
        
        if invoice_id:
            paid_invoice_ids.add(invoice_id)
    
    conn.commit()
    
    # Update invoice statuses: PAID if we found a payment
    if paid_invoice_ids:
        placeholders = ",".join("?" for _ in paid_invoice_ids)
        conn.execute(
            f"UPDATE invoices SET status = 'PAID' WHERE id IN ({placeholders}) AND status != 'CANCELLED'",
            list(paid_invoice_ids)
        )
        conn.commit()
    
    count = conn.execute("SELECT COUNT(*) FROM payments").fetchone()[0]
    linked = conn.execute("SELECT COUNT(*) FROM payments WHERE invoice_id IS NOT NULL").fetchone()[0]
    print(f"  → payments: {count} rows ({linked} linked to invoices)")
    
    paid_count = conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'PAID'").fetchone()[0]
    print(f"  → invoices updated: {paid_count} marked as PAID")


def ingest_journal_entries(conn: sqlite3.Connection):
    """
    Ingest journal_entry_items_accounts_receivable → journal_entries table.
    Links to invoices via accountingDocument.
    """
    print("\n── Ingesting journal entries ──")
    records = read_jsonl_folder("journal_entry_items_accounts_receivable")
    
    # Build accounting_document → invoice_id lookup
    acct_doc_to_invoice = {}
    for row in conn.execute("SELECT id, accounting_document FROM invoices WHERE accounting_document IS NOT NULL"):
        acct_doc_to_invoice[row[1]] = row[0]
    
    for r in records:
        acct_doc = normalize_id(r.get("accountingDocument"))
        item = normalize_str(r.get("accountingDocumentItem"))
        if not acct_doc:
            continue
        
        entry_id = f"JE-{acct_doc}-{item or '1'}"
        invoice_id = acct_doc_to_invoice.get(acct_doc)
        
        amount = normalize_amount(r.get("amountInTransactionCurrency"))
        debit = amount if amount and amount > 0 else None
        credit = abs(amount) if amount and amount < 0 else None
        
        conn.execute(
            "INSERT OR REPLACE INTO journal_entries "
            "(id, invoice_id, entry_date, debit_amount, credit_amount, account_code, raw_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                entry_id,
                invoice_id,
                normalize_date(r.get("postingDate")),
                debit,
                credit,
                normalize_str(r.get("glAccount")),
                to_raw_json(r),
            )
        )
    
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM journal_entries").fetchone()[0]
    linked = conn.execute("SELECT COUNT(*) FROM journal_entries WHERE invoice_id IS NOT NULL").fetchone()[0]
    print(f"  → journal_entries: {count} rows ({linked} linked to invoices)")


# ─── Broken Flow Detection ──────────────────────────────────────────────────

def print_broken_flows(conn: sqlite3.Connection):
    """Print summary of broken O2C flows."""
    print("\n══════════════════════════════════════")
    print("BROKEN FLOW DETECTION")
    print("══════════════════════════════════════")
    
    orders_no_delivery = conn.execute("""
        SELECT COUNT(*) FROM orders o
        LEFT JOIN deliveries d ON d.order_id = o.id
        WHERE d.id IS NULL
    """).fetchone()[0]
    
    deliveries_no_invoice = conn.execute("""
        SELECT COUNT(*) FROM deliveries d
        LEFT JOIN invoices i ON i.delivery_id = d.id
        WHERE i.id IS NULL
    """).fetchone()[0]
    
    invoices_no_payment = conn.execute("""
        SELECT COUNT(*) FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
        WHERE p.id IS NULL AND i.status != 'CANCELLED'
    """).fetchone()[0]
    
    complete = conn.execute("""
        SELECT COUNT(DISTINCT o.id) FROM orders o
        JOIN deliveries d ON d.order_id = o.id
        JOIN invoices i ON (i.delivery_id = d.id OR i.order_id = o.id)
        JOIN payments p ON p.invoice_id = i.id
    """).fetchone()[0]
    
    print(f"  Orders without delivery:     {orders_no_delivery}")
    print(f"  Deliveries without invoice:  {deliveries_no_invoice}")
    print(f"  Invoices without payment:    {invoices_no_payment}")
    print(f"  Fully complete order flows:  {complete}")


# ─── Main ────────────────────────────────────────────────────────────────────

def create_schema(conn: sqlite3.Connection):
    """Create all tables and indices."""
    conn.executescript(SCHEMA_DDL)
    conn.executescript(INDEX_DDL)
    conn.commit()


def run_ingestion():
    """Run the full ingestion pipeline."""
    print("╔══════════════════════════════════════╗")
    print("║  SAP O2C Data Ingestion Pipeline     ║")
    print("╚══════════════════════════════════════╝")
    print(f"\nData source: {DATA_SOURCE}")
    print(f"Database:    {DB_PATH}")
    
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove old DB to start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()
        print("  (removed existing database)")
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    try:
        print("\n── Creating schema ──")
        create_schema(conn)
        print("  ✓ 9 tables + indices created")
        
        # Ingest in dependency order
        address_by_bp = ingest_addresses(conn)
        ingest_customers(conn, address_by_bp)
        ingest_products(conn)
        ingest_orders(conn)
        ingest_order_items(conn)
        ingest_deliveries(conn)
        ingest_invoices(conn)
        ingest_payments(conn)
        ingest_journal_entries(conn)
        
        # Print summary
        print("\n══════════════════════════════════════")
        print("INGESTION SUMMARY")
        print("══════════════════════════════════════")
        tables = ["customers", "products", "addresses", "orders", "order_items",
                  "deliveries", "invoices", "payments", "journal_entries"]
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:20s} {count:>6d} rows")
        
        print_broken_flows(conn)
        
    finally:
        conn.close()
    
    print(f"\n✓ Ingestion complete. Database: {DB_PATH}")


if __name__ == "__main__":
    run_ingestion()
