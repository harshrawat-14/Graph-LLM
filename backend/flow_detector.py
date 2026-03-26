#!/usr/bin/env python3
"""
Broken Flow Detector
====================
Identifies incomplete Order-to-Cash flows in the database.
"""

import sqlite3
from typing import Dict, List, Any


def _execute_sql(conn: sqlite3.Connection, sql: str) -> List[Dict[str, Any]]:
    """Execute SQL and return list of dicts."""
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(sql)
    return [dict(row) for row in cursor.fetchall()]


def get_all_broken_flows(db_path: str) -> dict:
    """Detect and return all broken O2C flows."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        result = {
            "orders_without_delivery": _execute_sql(conn, """
                SELECT o.id, o.customer_id, o.order_date, o.status, o.total_amount, o.currency
                FROM orders o LEFT JOIN deliveries d ON d.order_id = o.id
                WHERE d.id IS NULL
                ORDER BY o.order_date DESC
            """),
            "deliveries_without_invoice": _execute_sql(conn, """
                SELECT d.id, d.order_id, d.delivery_date, d.status, d.plant
                FROM deliveries d LEFT JOIN invoices i ON i.delivery_id = d.id
                WHERE i.id IS NULL
                ORDER BY d.delivery_date DESC
            """),
            "invoices_without_payment": _execute_sql(conn, """
                SELECT i.id, i.order_id, i.amount, i.currency, i.status, i.issue_date
                FROM invoices i LEFT JOIN payments p ON p.invoice_id = i.id
                WHERE p.id IS NULL AND i.status != 'CANCELLED'
                ORDER BY i.issue_date DESC
            """),
            "orders_fully_complete": _execute_sql(conn, """
                SELECT DISTINCT o.id, o.customer_id, o.order_date, o.total_amount, o.currency
                FROM orders o
                JOIN deliveries d ON d.order_id = o.id
                JOIN invoices i ON (i.delivery_id = d.id OR i.order_id = o.id)
                JOIN payments p ON p.invoice_id = i.id
                ORDER BY o.order_date DESC
            """),
        }

        # Add summary counts
        result["summary"] = {
            "orders_without_delivery": len(result["orders_without_delivery"]),
            "deliveries_without_invoice": len(result["deliveries_without_invoice"]),
            "invoices_without_payment": len(result["invoices_without_payment"]),
            "orders_fully_complete": len(result["orders_fully_complete"]),
        }

        return result
    finally:
        conn.close()


if __name__ == "__main__":
    import os, json
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "business.db")
    flows = get_all_broken_flows(db_path)
    print(json.dumps(flows["summary"], indent=2))
