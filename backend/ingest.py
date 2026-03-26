#!/usr/bin/env python3
"""
SAP Order-to-Cash Data Ingestion Pipeline  ── FULL 19-FOLDER VERSION
=======================================================================

Architecture
────────────
Phase 1  ─ PARALLEL I/O   : All 19 JSONL folders are read from disk
           concurrently using ThreadPoolExecutor.  Pure file-I/O → no
           GIL contention, real wall-clock speedup.

Phase 2  ─ SERIAL WRITES  : SQLite only allows one writer at a time, so
           all inserts stay sequential but use fast executemany() bulk
           inserts inside explicit transactions (10-100× faster than
           row-by-row commits).

Why not parallel writes?
  SQLite with WAL mode supports concurrent READS but only one WRITER.
  Parallel write threads cause "database is locked" errors.  For 4 MB of
  data the serial bulk-insert path completes in < 1 s anyway.

Tables created (19 source folders → 19 tables + 19 extra indices):
  Core flow  : customers, products, addresses, orders, order_items,
               deliveries, invoices, payments, journal_entries
  Enrichment : schedule_lines, plants, storage_locations,
               sales_areas, customer_sales_areas,
               pricing_conditions, credit_management,
               material_docs, customer_material_info,
               partner_functions
"""

import json
import glob
import os
import sqlite3
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from typing import Optional, List, Dict, Tuple

# ─── Configuration ────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).resolve().parent.parent
DATA_SOURCE = BASE_DIR / "sap-o2c-data"
DB_PATH     = BASE_DIR / "data" / "business.db"

# How many threads to use for parallel JSONL reading.
# Each folder is one task → cap at folder count, but 8 is plenty for disk I/O.
READ_WORKERS = 8

# ─── JSONL Reader (thread-safe, no shared state) ─────────────────────────────

def read_jsonl_folder(folder_name: str) -> Tuple[str, List[dict], List[str]]:
    """
    Read all part-*.jsonl AND *.json files from a folder.
    Returns (folder_name, records, warnings) — fully self-contained so it
    can run safely in a thread without sharing any mutable state.
    """
    folder_path = DATA_SOURCE / folder_name
    records: List[dict] = []
    warnings: List[str] = []

    # Accept both part-*.jsonl (streaming exports) and *.json (single-file dumps)
    files = sorted(glob.glob(str(folder_path / "part-*.jsonl")))
    files += sorted(glob.glob(str(folder_path / "*.json")))
    # De-duplicate in case a file matches both patterns (unlikely but safe)
    files = list(dict.fromkeys(files))

    if not files:
        warnings.append(f"⚠  No JSONL/JSON files found in {folder_name}/")
        return folder_name, records, warnings

    for filepath in files:
        fname = os.path.basename(filepath)
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                raw = fh.read().strip()
        except Exception as e:
            warnings.append(f"⚠  File read error in {fname}: {e}")
            continue

        if not raw:
            continue

        # Handle two formats:
        #   1. JSONL  – one JSON object per line
        #   2. JSON   – a single array [ {...}, {...} ]
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    records.extend(parsed)
                else:
                    records.append(parsed)
            except json.JSONDecodeError as e:
                warnings.append(f"⚠  JSON parse error in {fname}: {e}")
        else:
            for line_num, line in enumerate(raw.splitlines(), 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    warnings.append(f"⚠  JSONL parse error in {fname} line {line_num}: {e}")

    return folder_name, records, warnings


def read_all_folders_parallel(folder_names: List[str]) -> Dict[str, List[dict]]:
    """
    Read all folders in parallel using ThreadPoolExecutor.
    Returns a dict: folder_name → list of records.
    """
    results: Dict[str, List[dict]] = {}
    print(f"\n── Phase 1: Parallel JSONL read ({READ_WORKERS} threads) ──")

    with ThreadPoolExecutor(max_workers=READ_WORKERS) as executor:
        future_map = {
            executor.submit(read_jsonl_folder, folder): folder
            for folder in folder_names
        }
        for future in as_completed(future_map):
            folder_name, records, warnings = future.result()
            results[folder_name] = records
            status = f"✓  {folder_name:<45} {len(records):>6} records"
            print(f"  {status}")
            for w in warnings:
                print(f"     {w}")

    return results


# ─── Normalization Helpers ────────────────────────────────────────────────────

def normalize_str(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None

def normalize_id(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None

def normalize_date(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return s

def normalize_amount(val) -> Optional[float]:
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
    return json.dumps(record, default=str, ensure_ascii=False)


# ─── Schema DDL ───────────────────────────────────────────────────────────────

SCHEMA_DDL = """
-- ── Core O2C Flow ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customers (
    id          TEXT PRIMARY KEY,
    name        TEXT,
    country     TEXT,
    city        TEXT,
    address_id  TEXT,
    created_at  TEXT,
    raw_source  TEXT
);

CREATE TABLE IF NOT EXISTS addresses (
    id          TEXT PRIMARY KEY,
    street      TEXT,
    city        TEXT,
    country     TEXT,
    postal_code TEXT,
    raw_source  TEXT
);

CREATE TABLE IF NOT EXISTS products (
    id          TEXT PRIMARY KEY,
    name        TEXT,           -- from product_descriptions (EN)
    category    TEXT,           -- productGroup
    unit_price  REAL,           -- derived from order_items
    currency    TEXT,
    raw_source  TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    customer_id     TEXT REFERENCES customers(id),
    order_date      TEXT,
    status          TEXT,
    total_amount    REAL,
    currency        TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    id          TEXT PRIMARY KEY,          -- salesOrder-salesOrderItem
    order_id    TEXT REFERENCES orders(id),
    product_id  TEXT REFERENCES products(id),
    quantity    REAL,
    unit_price  REAL,
    line_amount REAL,
    raw_source  TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              TEXT PRIMARY KEY,
    order_id        TEXT REFERENCES orders(id),
    plant           TEXT,
    delivery_date   TEXT,
    status          TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
    id                   TEXT PRIMARY KEY,
    order_id             TEXT REFERENCES orders(id),
    delivery_id          TEXT REFERENCES deliveries(id),
    customer_id          TEXT REFERENCES customers(id),
    amount               REAL,
    currency             TEXT,
    issue_date           TEXT,
    status               TEXT,  -- PAID | PARTIAL | UNPAID | CANCELLED
    accounting_document  TEXT,
    raw_source           TEXT
);

CREATE TABLE IF NOT EXISTS payments (
    id              TEXT PRIMARY KEY,
    invoice_id      TEXT REFERENCES invoices(id),
    amount          REAL,
    payment_date    TEXT,
    method          TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id              TEXT PRIMARY KEY,
    invoice_id      TEXT,
    entry_date      TEXT,
    debit_amount    REAL,
    credit_amount   REAL,
    account_code    TEXT,
    raw_source      TEXT
);

-- ── Enrichment Tables ────────────────────

CREATE TABLE IF NOT EXISTS schedule_lines (
    id                  TEXT PRIMARY KEY,   -- salesOrder-item-schedLine
    order_id            TEXT REFERENCES orders(id),
    order_item_id       TEXT REFERENCES order_items(id),
    requested_date      TEXT,
    confirmed_date      TEXT,
    confirmed_quantity  REAL,
    delivery_block      TEXT,
    raw_source          TEXT
);

CREATE TABLE IF NOT EXISTS plants (
    id              TEXT PRIMARY KEY,   -- plant code e.g. "1010"
    name            TEXT,
    country         TEXT,
    city            TEXT,
    company_code    TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS storage_locations (
    id              TEXT PRIMARY KEY,   -- plant+storageLocation composite
    plant_id        TEXT REFERENCES plants(id),
    location_code   TEXT,
    name            TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS sales_areas (
    id              TEXT PRIMARY KEY,   -- salesOrg+distChannel+division
    sales_org       TEXT,
    dist_channel    TEXT,
    division        TEXT,
    name            TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS customer_sales_areas (
    id              TEXT PRIMARY KEY,   -- customer+salesOrg+distChannel+division
    customer_id     TEXT REFERENCES customers(id),
    sales_org       TEXT,
    dist_channel    TEXT,
    division        TEXT,
    currency        TEXT,
    payment_terms   TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS pricing_conditions (
    id              TEXT PRIMARY KEY,
    order_id        TEXT REFERENCES orders(id),
    condition_type  TEXT,
    amount          REAL,
    currency        TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS credit_management (
    id                  TEXT PRIMARY KEY,
    customer_id         TEXT REFERENCES customers(id),
    credit_limit        REAL,
    currency            TEXT,
    credit_exposure     REAL,
    credit_block        TEXT,
    last_check_date     TEXT,
    raw_source          TEXT
);

CREATE TABLE IF NOT EXISTS material_docs (
    id              TEXT PRIMARY KEY,
    delivery_id     TEXT REFERENCES deliveries(id),
    posting_date    TEXT,
    movement_type   TEXT,
    quantity        REAL,
    plant           TEXT,
    storage_loc     TEXT,
    raw_source      TEXT
);

CREATE TABLE IF NOT EXISTS customer_material_info (
    id                  TEXT PRIMARY KEY,
    customer_id         TEXT REFERENCES customers(id),
    product_id          TEXT REFERENCES products(id),
    customer_mat_num    TEXT,
    sales_org           TEXT,
    dist_channel        TEXT,
    raw_source          TEXT
);

CREATE TABLE IF NOT EXISTS partner_functions (
    id              TEXT PRIMARY KEY,
    order_id        TEXT REFERENCES orders(id),
    partner_func    TEXT,   -- SP=soldTo, SH=shipTo, BP=billTo, PY=payer
    partner_id      TEXT,
    raw_source      TEXT
);
"""

INDEX_DDL = """
-- Core FK indices
CREATE INDEX IF NOT EXISTS idx_orders_customer         ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order       ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product     ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_order        ON deliveries(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_order          ON invoices(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_delivery       ON invoices(delivery_id);
CREATE INDEX IF NOT EXISTS idx_invoices_customer       ON invoices(customer_id);
CREATE INDEX IF NOT EXISTS idx_invoices_acct_doc       ON invoices(accounting_document);
CREATE INDEX IF NOT EXISTS idx_payments_invoice        ON payments(invoice_id);
CREATE INDEX IF NOT EXISTS idx_journal_invoice         ON journal_entries(invoice_id);
-- Enrichment indices
CREATE INDEX IF NOT EXISTS idx_sched_order             ON schedule_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_sched_item              ON schedule_lines(order_item_id);
CREATE INDEX IF NOT EXISTS idx_storloc_plant           ON storage_locations(plant_id);
CREATE INDEX IF NOT EXISTS idx_csa_customer            ON customer_sales_areas(customer_id);
CREATE INDEX IF NOT EXISTS idx_pricing_order           ON pricing_conditions(order_id);
CREATE INDEX IF NOT EXISTS idx_credit_customer         ON credit_management(customer_id);
CREATE INDEX IF NOT EXISTS idx_matdoc_delivery         ON material_docs(delivery_id);
CREATE INDEX IF NOT EXISTS idx_cmi_customer            ON customer_material_info(customer_id);
CREATE INDEX IF NOT EXISTS idx_cmi_product             ON customer_material_info(product_id);
CREATE INDEX IF NOT EXISTS idx_partner_order           ON partner_functions(order_id);
"""


# ─── Bulk Insert Helper ───────────────────────────────────────────────────────

def bulk_insert(conn: sqlite3.Connection, table: str, columns: List[str],
                rows: List[tuple], replace: bool = True) -> int:
    """
    Fast bulk insert using executemany() inside a single transaction.
    Returns number of rows inserted.
    """
    if not rows:
        return 0
    verb    = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
    ph      = ",".join("?" * len(columns))
    col_str = ",".join(columns)
    sql     = f"{verb} INTO {table} ({col_str}) VALUES ({ph})"
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


# ─── Phase 2: Serial Ingestion Functions ─────────────────────────────────────

def ingest_addresses(conn, data: Dict[str, List[dict]]) -> Dict[str, dict]:
    records  = data.get("business_partner_addresses", [])
    addr_by_bp: Dict[str, dict] = {}
    rows = []
    for r in records:
        addr_id = normalize_id(r.get("addressId"))
        bp      = normalize_id(r.get("businessPartner"))
        if not addr_id:
            continue
        row_dict = {
            "id":          addr_id,
            "street":      normalize_str(r.get("streetName")),
            "city":        normalize_str(r.get("cityName")),
            "country":     normalize_str(r.get("country")),
            "postal_code": normalize_str(r.get("postalCode")),
            "raw_source":  to_raw_json(r),
        }
        rows.append(tuple(row_dict.values()))
        if bp:
            addr_by_bp[bp] = row_dict

    n = bulk_insert(conn, "addresses",
                    ["id","street","city","country","postal_code","raw_source"], rows)
    print(f"  addresses                  → {n:>6} rows")
    return addr_by_bp


def ingest_customers(conn, data: Dict[str, List[dict]], addr_by_bp: Dict[str, dict]):
    records = data.get("business_partners", [])
    rows = []
    for r in records:
        bp_id = normalize_id(r.get("businessPartner"))
        if not bp_id:
            continue
        addr = addr_by_bp.get(bp_id, {})
        rows.append((
            bp_id,
            normalize_str(r.get("businessPartnerFullName")),
            addr.get("country"),
            addr.get("city"),
            addr.get("id"),
            normalize_date(r.get("creationDate")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "customers",
                    ["id","name","country","city","address_id","created_at","raw_source"], rows)
    print(f"  customers                  → {n:>6} rows")


def ingest_products(conn, data: Dict[str, List[dict]]):
    products     = data.get("products", [])
    descriptions = data.get("product_descriptions", [])

    desc_map: Dict[str, str] = {}
    for d in descriptions:
        pid  = normalize_id(d.get("product"))
        lang = normalize_str(d.get("language"))
        if pid and lang == "EN":
            desc_map[pid] = normalize_str(d.get("productDescription"))

    rows = []
    for r in products:
        pid = normalize_id(r.get("product"))
        if not pid:
            continue
        rows.append((
            pid,
            desc_map.get(pid, normalize_str(r.get("productOldId"))),
            normalize_str(r.get("productGroup")),
            None, None,
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "products",
                    ["id","name","category","unit_price","currency","raw_source"], rows)
    print(f"  products                   → {n:>6} rows")


def ingest_orders(conn, data: Dict[str, List[dict]]):
    records = data.get("sales_order_headers", [])
    status_map = {"": "OPEN", "A": "NOT_DELIVERED",
                  "B": "PARTIALLY_DELIVERED", "C": "FULLY_DELIVERED"}
    rows = []
    for r in records:
        order_id = normalize_id(r.get("salesOrder"))
        if not order_id:
            continue
        raw_s = normalize_str(r.get("overallDeliveryStatus")) or ""
        rows.append((
            order_id,
            normalize_id(r.get("soldToParty")),
            normalize_date(r.get("creationDate")),
            status_map.get(raw_s, raw_s),
            normalize_amount(r.get("totalNetAmount")),
            normalize_str(r.get("transactionCurrency")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "orders",
                    ["id","customer_id","order_date","status","total_amount","currency","raw_source"], rows)
    print(f"  orders                     → {n:>6} rows")


def ingest_order_items(conn, data: Dict[str, List[dict]]):
    records = data.get("sales_order_items", [])
    rows = []
    for r in records:
        order_id = normalize_id(r.get("salesOrder"))
        item_num = normalize_str(r.get("salesOrderItem"))
        if not order_id or not item_num:
            continue
        qty        = normalize_amount(r.get("requestedQuantity"))
        net_amount = normalize_amount(r.get("netAmount"))
        unit_price = round(net_amount / qty, 4) if net_amount and qty and qty > 0 else None
        rows.append((
            f"{order_id}-{item_num}",
            order_id,
            normalize_id(r.get("material")),
            qty,
            unit_price,
            net_amount,
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "order_items",
                    ["id","order_id","product_id","quantity","unit_price","line_amount","raw_source"], rows)
    print(f"  order_items                → {n:>6} rows")


def ingest_deliveries(conn, data: Dict[str, List[dict]]):
    headers = data.get("outbound_delivery_headers", [])
    items   = data.get("outbound_delivery_items", [])

    del_to_order: Dict[str, str] = {}
    del_to_plant: Dict[str, str] = {}
    for item in items:
        del_id    = normalize_id(item.get("deliveryDocument"))
        order_ref = normalize_id(item.get("referenceSdDocument"))
        plant     = normalize_str(item.get("plant"))
        if del_id and order_ref:
            del_to_order.setdefault(del_id, order_ref)
        if del_id and plant:
            del_to_plant.setdefault(del_id, plant)

    status_map = {"": "NOT_STARTED", "A": "NOT_YET_STARTED",
                  "B": "PARTIALLY_COMPLETED", "C": "COMPLETED"}
    rows = []
    for r in headers:
        del_id = normalize_id(r.get("deliveryDocument"))
        if not del_id:
            continue
        raw_s = normalize_str(r.get("overallGoodsMovementStatus")) or ""
        delivery_date = (normalize_date(r.get("actualGoodsMovementDate"))
                         or normalize_date(r.get("creationDate")))
        rows.append((
            del_id,
            del_to_order.get(del_id),
            del_to_plant.get(del_id, normalize_str(r.get("shippingPoint"))),
            delivery_date,
            status_map.get(raw_s, raw_s),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "deliveries",
                    ["id","order_id","plant","delivery_date","status","raw_source"], rows)
    no_order = conn.execute(
        "SELECT COUNT(*) FROM deliveries WHERE order_id IS NULL").fetchone()[0]
    print(f"  deliveries                 → {n:>6} rows  "
          f"({no_order} without order link)")


def ingest_invoices(conn, data: Dict[str, List[dict]]):
    headers       = data.get("billing_document_headers", [])
    items         = data.get("billing_document_items", [])
    cancellations = data.get("billing_document_cancellations", [])

    billing_to_delivery: Dict[str, str] = {}
    for item in items:
        bill_id = normalize_id(item.get("billingDocument"))
        ref_doc = normalize_id(item.get("referenceSdDocument"))
        if bill_id and ref_doc:
            billing_to_delivery.setdefault(bill_id, ref_doc)

    delivery_order_map: Dict[str, str] = {
        row[0]: row[1] for row in
        conn.execute("SELECT id, order_id FROM deliveries WHERE order_id IS NOT NULL")
    }

    cancelled_docs: set = set()
    for c in cancellations:
        for field in ("billingDocument", "cancelledBillingDocument"):
            bid = normalize_id(c.get(field))
            if bid:
                cancelled_docs.add(bid)

    rows = []
    for r in headers:
        bill_id = normalize_id(r.get("billingDocument"))
        if not bill_id:
            continue
        is_cancelled = r.get("billingDocumentIsCancelled", False) or bill_id in cancelled_docs
        delivery_id  = billing_to_delivery.get(bill_id)
        order_id     = delivery_order_map.get(delivery_id) if delivery_id else None
        status       = "CANCELLED" if is_cancelled else "UNPAID"
        rows.append((
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
        ))
    n = bulk_insert(conn, "invoices",
                    ["id","order_id","delivery_id","customer_id","amount","currency",
                     "issue_date","status","accounting_document","raw_source"], rows)
    cancelled = conn.execute(
        "SELECT COUNT(*) FROM invoices WHERE status='CANCELLED'").fetchone()[0]
    print(f"  invoices                   → {n:>6} rows  ({cancelled} cancelled)")


def ingest_payments(conn, data: Dict[str, List[dict]]):
    records = data.get("payments_accounts_receivable", [])

    acct_doc_to_invoice: Dict[str, str] = {
        row[1]: row[0] for row in
        conn.execute("SELECT id, accounting_document FROM invoices "
                     "WHERE accounting_document IS NOT NULL")
    }

    paid_ids: set = set()
    rows = []
    for r in records:
        acct_doc = normalize_id(r.get("accountingDocument"))
        if not acct_doc:
            continue
        clearing_doc = normalize_id(r.get("clearingAccountingDocument"))
        invoice_id   = (acct_doc_to_invoice.get(clearing_doc)
                        if clearing_doc else None)
        if not invoice_id:
            invoice_id = acct_doc_to_invoice.get(acct_doc)

        payment_id = f"{acct_doc}-{normalize_str(r.get('accountingDocumentItem','1'))}"
        rows.append((
            payment_id,
            invoice_id,
            normalize_amount(r.get("amountInTransactionCurrency")),
            normalize_date(r.get("clearingDate") or r.get("postingDate")),
            normalize_str(r.get("financialAccountType")),
            to_raw_json(r),
        ))
        if invoice_id:
            paid_ids.add(invoice_id)

    n = bulk_insert(conn, "payments",
                    ["id","invoice_id","amount","payment_date","method","raw_source"], rows)
    if paid_ids:
        ph = ",".join("?" * len(paid_ids))
        conn.execute(
            f"UPDATE invoices SET status='PAID' "
            f"WHERE id IN ({ph}) AND status != 'CANCELLED'",
            list(paid_ids))
        conn.commit()
    paid_count = conn.execute(
        "SELECT COUNT(*) FROM invoices WHERE status='PAID'").fetchone()[0]
    linked = conn.execute(
        "SELECT COUNT(*) FROM payments WHERE invoice_id IS NOT NULL").fetchone()[0]
    print(f"  payments                   → {n:>6} rows  "
          f"({linked} linked, {paid_count} invoices now PAID)")


def ingest_journal_entries(conn, data: Dict[str, List[dict]]):
    records = data.get("journal_entry_items_accounts_receivable", [])
    acct_doc_to_invoice: Dict[str, str] = {
        row[1]: row[0] for row in
        conn.execute("SELECT id, accounting_document FROM invoices "
                     "WHERE accounting_document IS NOT NULL")
    }
    rows = []
    for r in records:
        acct_doc = normalize_id(r.get("accountingDocument"))
        item     = normalize_str(r.get("accountingDocumentItem"))
        if not acct_doc:
            continue
        amount = normalize_amount(r.get("amountInTransactionCurrency"))
        debit  = amount if amount and amount > 0 else None
        credit = abs(amount) if amount and amount < 0 else None
        rows.append((
            f"JE-{acct_doc}-{item or '1'}",
            acct_doc_to_invoice.get(acct_doc),
            normalize_date(r.get("postingDate")),
            debit,
            credit,
            normalize_str(r.get("glAccount")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "journal_entries",
                    ["id","invoice_id","entry_date","debit_amount","credit_amount",
                     "account_code","raw_source"], rows)
    linked = conn.execute(
        "SELECT COUNT(*) FROM journal_entries WHERE invoice_id IS NOT NULL").fetchone()[0]
    print(f"  journal_entries            → {n:>6} rows  ({linked} linked to invoices)")


# ─── Enrichment Ingest Functions ──────────

def ingest_schedule_lines(conn, data: Dict[str, List[dict]]):
    records = data.get("sales_order_schedule_lines", [])
    if not records:
        print(f"  schedule_lines             →      0 rows")
        return
    rows = []
    for r in records:
        order_id = normalize_id(r.get("salesOrder"))
        item_num = normalize_str(r.get("salesOrderItem"))
        sched    = normalize_str(r.get("scheduleLine"))
        if not order_id or not item_num:
            continue
        item_id = f"{order_id}-{item_num}"
        sched_id = f"{item_id}-{sched or '1'}"
        rows.append((
            sched_id,
            order_id,
            item_id,
            normalize_date(r.get("requestedDeliveryDate")),
            normalize_date(r.get("confirmedDeliveryDate")),
            normalize_amount(r.get("confirmedQuantity") or r.get("scheduledQuantity")),
            normalize_str(r.get("deliveryBlockReason")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "schedule_lines",
                    ["id","order_id","order_item_id","requested_date","confirmed_date",
                     "confirmed_quantity","delivery_block","raw_source"], rows)
    print(f"  schedule_lines             → {n:>6} rows")


def ingest_plants(conn, data: Dict[str, List[dict]]):
    records = (data.get("plants") or data.get("plant_master") or [])
    if not records:
        print(f"  plants                     →      0 rows")
        return
    rows = []
    for r in records:
        plant_id = normalize_id(r.get("plant") or r.get("Plant"))
        if not plant_id:
            continue
        rows.append((
            plant_id,
            normalize_str(r.get("plantName") or r.get("name1")),
            normalize_str(r.get("country") or r.get("countryKey")),
            normalize_str(r.get("city") or r.get("city1")),
            normalize_str(r.get("companyCode")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "plants",
                    ["id","name","country","city","company_code","raw_source"], rows)
    print(f"  plants                     → {n:>6} rows")


def ingest_storage_locations(conn, data: Dict[str, List[dict]]):
    records = data.get("product_storage_locations", [])
    if not records:
        print(f"  storage_locations          →      0 rows")
        return
    rows = []
    for r in records:
        plant    = normalize_id(r.get("plant") or r.get("Plant"))
        stor_loc = normalize_id(r.get("storageLocation") or r.get("storageLoc"))
        if not plant or not stor_loc:
            continue
        rows.append((
            f"{plant}-{stor_loc}",
            plant,
            stor_loc,
            normalize_str(r.get("storagLocationName") or r.get("description")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "storage_locations",
                    ["id","plant_id","location_code","name","raw_source"], rows)
    print(f"  storage_locations          → {n:>6} rows")


def ingest_sales_areas(conn, data: Dict[str, List[dict]]):
    records = (data.get("sales_areas") or data.get("sales_organization_areas") or [])
    if not records:
        print(f"  sales_areas                →      0 rows")
        return
    rows = []
    seen: set = set()
    for r in records:
        s_org = normalize_str(r.get("salesOrganization") or r.get("salesOrg"))
        d_ch  = normalize_str(r.get("distributionChannel") or r.get("distChannel"))
        div   = normalize_str(r.get("division") or r.get("Division"))
        if not s_org:
            continue
        area_id = f"{s_org}-{d_ch or 'XX'}-{div or 'XX'}"
        if area_id in seen:
            continue
        seen.add(area_id)
        rows.append((
            area_id, s_org, d_ch, div,
            normalize_str(r.get("name") or r.get("salesOrgName")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "sales_areas",
                    ["id","sales_org","dist_channel","division","name","raw_source"], rows)
    print(f"  sales_areas                → {n:>6} rows")


def ingest_customer_sales_areas(conn, data: Dict[str, List[dict]]):
    records = (data.get("customer_sales_area_assignments") or [])
    if not records:
        print(f"  customer_sales_areas       →      0 rows")
        return
    rows = []
    for r in records:
        cust  = normalize_id(r.get("customer") or r.get("businessPartner"))
        s_org = normalize_str(r.get("salesOrganization"))
        d_ch  = normalize_str(r.get("distributionChannel"))
        div   = normalize_str(r.get("division"))
        if not cust or not s_org:
            continue
        rows.append((
            f"{cust}-{s_org}-{d_ch or 'XX'}-{div or 'XX'}",
            cust, s_org, d_ch, div,
            normalize_str(r.get("currency")),
            normalize_str(r.get("paymentTerms")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "customer_sales_areas",
                    ["id","customer_id","sales_org","dist_channel","division",
                     "currency","payment_terms","raw_source"], rows)
    print(f"  customer_sales_areas       → {n:>6} rows")


def ingest_pricing_conditions(conn, data: Dict[str, List[dict]]):
    records = (data.get("sales_order_pricing_elements") or [])
    if not records:
        print(f"  pricing_conditions         →      0 rows")
        return
    rows = []
    for r in records:
        order_id = normalize_id(r.get("salesOrder"))
        item_num = normalize_str(r.get("salesOrderItem") or "000000")
        cond_t   = normalize_str(r.get("conditionType"))
        if not order_id or not cond_t:
            continue
        step   = normalize_str(r.get("pricingProcedureStep") or "0")
        ctr    = normalize_str(r.get("pricingProcedureCounter") or "0")
        pc_id  = f"{order_id}-{item_num}-{cond_t}-{step}-{ctr}"
        rows.append((
            pc_id,
            order_id,
            cond_t,
            normalize_amount(r.get("conditionRateAmount")),
            normalize_str(r.get("transactionCurrency")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "pricing_conditions",
                    ["id","order_id","condition_type","amount","currency","raw_source"], rows)
    print(f"  pricing_conditions         → {n:>6} rows")


def ingest_credit_management(conn, data: Dict[str, List[dict]]):
    records = (data.get("credit_management_master") or [])
    if not records:
        print(f"  credit_management          →      0 rows")
        return
    rows = []
    for r in records:
        cust = normalize_id(r.get("businessPartner"))
        if not cust:
            continue
        segment = normalize_str(r.get("creditSegment") or "DEFAULT")
        rows.append((
            f"{cust}-{segment}",
            cust,
            normalize_amount(r.get("creditLimitAmount")),
            normalize_str(r.get("creditLimitCurrency")),
            normalize_amount(r.get("totalLiability")),
            normalize_str(r.get("creditCheckStatus")),
            normalize_date(r.get("creditLastDate")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "credit_management",
                    ["id","customer_id","credit_limit","currency","credit_exposure",
                     "credit_block","last_check_date","raw_source"], rows)
    print(f"  credit_management          → {n:>6} rows")


def ingest_material_docs(conn, data: Dict[str, List[dict]]):
    records = (data.get("material_documents") or [])
    if not records:
        print(f"  material_docs              →      0 rows")
        return
    delivery_ids: set = {row[0] for row in conn.execute("SELECT id FROM deliveries")}
    rows = []
    for r in records:
        mat_doc  = normalize_id(r.get("materialDocument"))
        item_num = normalize_str(r.get("materialDocumentItem") or "1")
        if not mat_doc:
            continue
        ref_doc     = normalize_id(r.get("referenceDocument"))
        delivery_id = ref_doc if ref_doc in delivery_ids else None
        rows.append((
            f"{mat_doc}-{item_num}",
            delivery_id,
            normalize_date(r.get("postingDate")),
            normalize_str(r.get("goodsMovementType")),
            normalize_amount(r.get("quantityInBaseUnit")),
            normalize_str(r.get("plant")),
            normalize_str(r.get("storageLocation")),
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "material_docs",
                    ["id","delivery_id","posting_date","movement_type","quantity",
                     "plant","storage_loc","raw_source"], rows)
    print(f"  material_docs              → {n:>6} rows")


def ingest_customer_material_info(conn, data: Dict[str, List[dict]]):
    records = (data.get("customer_material_info") or [])
    if not records:
        print(f"  customer_material_info     →      0 rows")
        return
    rows = []
    for r in records:
        cust     = normalize_id(r.get("customer"))
        material = normalize_id(r.get("material"))
        s_org    = normalize_str(r.get("salesOrganization"))
        d_ch     = normalize_str(r.get("distributionChannel"))
        if not cust or not material:
            continue
        rows.append((
            f"{cust}-{material}-{s_org or 'X'}-{d_ch or 'X'}",
            cust, material,
            normalize_str(r.get("customerMaterialNumber")),
            s_org, d_ch,
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "customer_material_info",
                    ["id","customer_id","product_id","customer_mat_num",
                     "sales_org","dist_channel","raw_source"], rows)
    print(f"  customer_material_info     → {n:>6} rows")


def ingest_partner_functions(conn, data: Dict[str, List[dict]]):
    records = (data.get("sales_order_partner_functions") or [])
    if not records:
        print(f"  partner_functions          →      0 rows")
        return
    rows = []
    for r in records:
        order_id     = normalize_id(r.get("salesOrder"))
        partner_func = normalize_str(r.get("partnerFunction"))
        partner_id   = normalize_id(r.get("customer"))
        if not order_id or not partner_func:
            continue
        rows.append((
            f"{order_id}-{partner_func}",
            order_id, partner_func, partner_id,
            to_raw_json(r),
        ))
    n = bulk_insert(conn, "partner_functions",
                    ["id","order_id","partner_func","partner_id","raw_source"], rows)
    print(f"  partner_functions          → {n:>6} rows")


# ─── Broken Flow & Main ────────────────────────────────────────────────────────

def print_broken_flows(conn: sqlite3.Connection):
    print("\n══════════════════════════════════════════════════")
    print("  BROKEN FLOW DETECTION")
    print("══════════════════════════════════════════════════")
    checks = [
        ("Orders without delivery", "SELECT COUNT(*) FROM orders o LEFT JOIN deliveries d ON d.order_id=o.id WHERE d.id IS NULL"),
        ("Deliveries without invoice", "SELECT COUNT(*) FROM deliveries d LEFT JOIN invoices i ON i.delivery_id=d.id WHERE i.id IS NULL"),
        ("Invoices without payment", "SELECT COUNT(*) FROM invoices i LEFT JOIN payments p ON p.invoice_id=i.id WHERE p.id IS NULL AND i.status != 'CANCELLED'"),
        ("Fully complete O2C flows", "SELECT COUNT(DISTINCT o.id) FROM orders o JOIN deliveries d ON d.order_id=o.id JOIN invoices i ON i.delivery_id=d.id JOIN payments p ON p.invoice_id=i.id"),
    ]
    for label, sql in checks:
        val = conn.execute(sql).fetchone()[0]
        print(f"  {label:<45} {val:>6}")


ALL_FOLDERS = [
    "business_partner_addresses", "business_partners", "products", "product_descriptions",
    "sales_order_headers", "sales_order_items", "outbound_delivery_headers", "outbound_delivery_items",
    "billing_document_headers", "billing_document_items", "billing_document_cancellations",
    "payments_accounts_receivable", "journal_entry_items_accounts_receivable",
    "sales_order_schedule_lines", "plants", "product_storage_locations",
    "customer_sales_area_assignments", "sales_order_pricing_elements", "credit_management_master",
    "material_documents", "customer_material_info", "sales_order_partner_functions",
]

def run_ingestion():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    if Path(DB_PATH).exists(): Path(DB_PATH).unlink()
    data = read_all_folders_parallel(ALL_FOLDERS)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.executescript(SCHEMA_DDL)
        conn.executescript(INDEX_DDL)
        addr_by_bp = ingest_addresses(conn, data)
        ingest_customers(conn, data, addr_by_bp)
        ingest_products(conn, data)
        ingest_orders(conn, data)
        ingest_order_items(conn, data)
        ingest_deliveries(conn, data)
        ingest_invoices(conn, data)
        ingest_payments(conn, data)
        ingest_journal_entries(conn, data)
        print()
        ingest_schedule_lines(conn, data)
        ingest_plants(conn, data)
        ingest_storage_locations(conn, data)
        ingest_sales_areas(conn, data)
        ingest_customer_sales_areas(conn, data)
        ingest_pricing_conditions(conn, data)
        ingest_credit_management(conn, data)
        ingest_material_docs(conn, data)
        ingest_customer_material_info(conn, data)
        ingest_partner_functions(conn, data)
        print_broken_flows(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    run_ingestion()
