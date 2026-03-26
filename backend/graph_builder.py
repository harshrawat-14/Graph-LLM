#!/usr/bin/env python3
"""
Graph Construction Module
=========================
Builds a NetworkX DiGraph from the SQLite database where every business entity
is a node and every relationship is a directed edge with metadata.
"""

import json
import sqlite3
from collections import deque
from typing import Optional, Dict, List, Any, Set, Tuple

import networkx as nx


# ─── Node / Edge Helpers ─────────────────────────────────────────────────────

NODE_COLORS = {
    "Customer": "#3B82F6",     # blue
    "Order": "#22C55E",        # green
    "OrderItem": "#A855F7",    # purple (light)
    "Product": "#8B5CF6",      # purple
    "Delivery": "#F97316",     # orange
    "Invoice": "#EAB308",      # yellow
    "Payment": "#14B8A6",      # teal
    "JournalEntry": "#6366F1", # indigo
    "BrokenFlow": "#EF4444",   # red
}


def _make_node(node_id: str, node_type: str, label: str,
               metadata: dict = None, broken: bool = False) -> dict:
    return {
        "id": node_id,
        "type": node_type,
        "label": label,
        "metadata": metadata or {},
        "broken": broken,
        "color": NODE_COLORS.get(node_type, "#9CA3AF"),
    }


def _make_edge(source: str, target: str, relation: str,
               confirmed: bool = True, broken: bool = False) -> dict:
    return {
        "source": source,
        "target": target,
        "relation": relation,
        "confirmed": confirmed,
        "broken": broken,
    }


# ─── Graph Builder ───────────────────────────────────────────────────────────

def build_graph(db_path: str) -> nx.DiGraph:
    """
    Build a NetworkX directed graph from all entities in the SQLite database.
    Returns the populated graph.
    """
    G = nx.DiGraph()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        _add_customers(G, conn)
        _add_products(G, conn)
        _add_orders(G, conn)
        _add_order_items(G, conn)
        _add_deliveries(G, conn)
        _add_invoices(G, conn)
        _add_payments(G, conn)
        _add_journal_entries(G, conn)
        _add_broken_flows(G, conn)
    finally:
        conn.close()

    return G


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a plain dict, excluding raw_source for metadata."""
    d = dict(row)
    d.pop("raw_source", None)
    return d


def _add_customers(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM customers"):
        r = dict(row)
        meta = _row_to_dict(row)
        G.add_node(f"C-{r['id']}", **_make_node(
            f"C-{r['id']}", "Customer",
            r.get("name") or r["id"],
            meta
        ))


def _add_products(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM products"):
        r = dict(row)
        meta = _row_to_dict(row)
        G.add_node(f"P-{r['id']}", **_make_node(
            f"P-{r['id']}", "Product",
            r.get("name") or r["id"],
            meta
        ))


def _add_orders(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM orders"):
        r = dict(row)
        meta = _row_to_dict(row)
        order_node = f"O-{r['id']}"
        G.add_node(order_node, **_make_node(
            order_node, "Order",
            f"Order {r['id']}",
            meta
        ))
        # Edge: Customer → Order (PLACED)
        if r.get("customer_id"):
            cust_node = f"C-{r['customer_id']}"
            if G.has_node(cust_node):
                G.add_edge(cust_node, order_node, **_make_edge(
                    cust_node, order_node, "PLACED"
                ))


def _add_order_items(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM order_items"):
        r = dict(row)
        meta = _row_to_dict(row)
        item_node = f"OI-{r['id']}"
        G.add_node(item_node, **_make_node(
            item_node, "OrderItem",
            f"Item {r['id']}",
            meta
        ))
        # Edge: Order → OrderItem (CONTAINS)
        if r.get("order_id"):
            order_node = f"O-{r['order_id']}"
            if G.has_node(order_node):
                G.add_edge(order_node, item_node, **_make_edge(
                    order_node, item_node, "CONTAINS"
                ))
        # Edge: OrderItem → Product (FOR_PRODUCT)
        if r.get("product_id"):
            prod_node = f"P-{r['product_id']}"
            if G.has_node(prod_node):
                G.add_edge(item_node, prod_node, **_make_edge(
                    item_node, prod_node, "FOR_PRODUCT"
                ))


def _add_deliveries(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM deliveries"):
        r = dict(row)
        meta = _row_to_dict(row)
        del_node = f"D-{r['id']}"
        G.add_node(del_node, **_make_node(
            del_node, "Delivery",
            f"Delivery {r['id']}",
            meta
        ))
        # Edge: Order → Delivery (FULFILLED_BY)
        if r.get("order_id"):
            order_node = f"O-{r['order_id']}"
            if G.has_node(order_node):
                G.add_edge(order_node, del_node, **_make_edge(
                    order_node, del_node, "FULFILLED_BY"
                ))


def _add_invoices(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM invoices"):
        r = dict(row)
        meta = _row_to_dict(row)
        inv_node = f"I-{r['id']}"
        G.add_node(inv_node, **_make_node(
            inv_node, "Invoice",
            f"Invoice {r['id']}",
            meta
        ))
        # Edge: Delivery → Invoice (BILLED_AS) - preferred
        linked = False
        if r.get("delivery_id"):
            del_node = f"D-{r['delivery_id']}"
            if G.has_node(del_node):
                G.add_edge(del_node, inv_node, **_make_edge(
                    del_node, inv_node, "BILLED_AS"
                ))
                linked = True
        # Edge: Order → Invoice (BILLED_AS, fallback if no delivery link)
        if not linked and r.get("order_id"):
            order_node = f"O-{r['order_id']}"
            if G.has_node(order_node):
                G.add_edge(order_node, inv_node, **_make_edge(
                    order_node, inv_node, "BILLED_AS", confirmed=True
                ))


def _add_payments(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM payments"):
        r = dict(row)
        meta = _row_to_dict(row)
        pay_node = f"PAY-{r['id']}"
        G.add_node(pay_node, **_make_node(
            pay_node, "Payment",
            f"Payment {r['id']}",
            meta
        ))
        # Edge: Invoice → Payment (PAID_BY)
        if r.get("invoice_id"):
            inv_node = f"I-{r['invoice_id']}"
            if G.has_node(inv_node):
                G.add_edge(inv_node, pay_node, **_make_edge(
                    inv_node, pay_node, "PAID_BY"
                ))


def _add_journal_entries(G: nx.DiGraph, conn: sqlite3.Connection):
    for row in conn.execute("SELECT * FROM journal_entries"):
        r = dict(row)
        meta = _row_to_dict(row)
        je_node = f"JE-{r['id']}"
        G.add_node(je_node, **_make_node(
            je_node, "JournalEntry",
            f"JE {r['id']}",
            meta
        ))
        # Edge: Invoice → JournalEntry (JOURNAL_ENTRY)
        if r.get("invoice_id"):
            inv_node = f"I-{r['invoice_id']}"
            if G.has_node(inv_node):
                G.add_edge(inv_node, je_node, **_make_edge(
                    inv_node, je_node, "JOURNAL_ENTRY"
                ))


def _add_broken_flows(G: nx.DiGraph, conn: sqlite3.Connection):
    """Add BrokenFlow marker nodes for incomplete O2C chains."""
    # Orders without delivery
    for row in conn.execute("""
        SELECT o.id FROM orders o
        LEFT JOIN deliveries d ON d.order_id = o.id
        WHERE d.id IS NULL
    """):
        oid = row[0]
        broken_id = f"MISSING_DEL_{oid}"
        order_node = f"O-{oid}"
        G.add_node(broken_id, **_make_node(
            broken_id, "BrokenFlow", "Missing Delivery", broken=True
        ))
        if G.has_node(order_node):
            G.add_edge(order_node, broken_id, **_make_edge(
                order_node, broken_id, "FULFILLED_BY", broken=True
            ))

    # Deliveries without invoice
    for row in conn.execute("""
        SELECT d.id FROM deliveries d
        LEFT JOIN invoices i ON i.delivery_id = d.id
        WHERE i.id IS NULL
    """):
        did = row[0]
        broken_id = f"MISSING_INV_{did}"
        del_node = f"D-{did}"
        G.add_node(broken_id, **_make_node(
            broken_id, "BrokenFlow", "Missing Invoice", broken=True
        ))
        if G.has_node(del_node):
            G.add_edge(del_node, broken_id, **_make_edge(
                del_node, broken_id, "BILLED_AS", broken=True
            ))

    # Non-cancelled invoices without payment
    for row in conn.execute("""
        SELECT i.id FROM invoices i
        LEFT JOIN payments p ON p.invoice_id = i.id
        WHERE p.id IS NULL AND i.status != 'CANCELLED'
    """):
        iid = row[0]
        broken_id = f"MISSING_PAY_{iid}"
        inv_node = f"I-{iid}"
        G.add_node(broken_id, **_make_node(
            broken_id, "BrokenFlow", "Missing Payment", broken=True
        ))
        if G.has_node(inv_node):
            G.add_edge(inv_node, broken_id, **_make_edge(
                inv_node, broken_id, "PAID_BY", broken=True
            ))


# ─── Query Functions ─────────────────────────────────────────────────────────

def get_graph_json(G: nx.DiGraph) -> dict:
    """Convert the graph to JSON-serializable {nodes, links} format."""
    nodes = []
    for nid, data in G.nodes(data=True):
        node = dict(data)
        node["id"] = nid
        nodes.append(node)

    links = []
    for src, tgt, data in G.edges(data=True):
        edge = dict(data)
        edge["source"] = src
        edge["target"] = tgt
        links.append(edge)

    return {"nodes": nodes, "links": links}


def get_node_neighbors(G: nx.DiGraph, node_id: str, depth: int = 1) -> dict:
    """Get all nodes within `depth` hops of `node_id`."""
    if node_id not in G:
        return {"nodes": [], "links": []}

    visited = {node_id}
    frontier = {node_id}

    for _ in range(depth):
        next_frontier = set()
        for n in frontier:
            # Both directions (predecessors + successors) for undirected neighborhood
            for neighbor in list(G.successors(n)) + list(G.predecessors(n)):
                if neighbor not in visited:
                    visited.add(neighbor)
                    next_frontier.add(neighbor)
        frontier = next_frontier

    subgraph = G.subgraph(visited)
    return get_graph_json(subgraph)


def trace_flow(G: nx.DiGraph, start_id: str) -> list:
    """DFS downstream from start_id, returning ordered list of node IDs."""
    if start_id not in G:
        return []

    visited = []
    stack = [start_id]
    seen = set()

    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        visited.append(node)
        # Push successors in reverse for consistent left-to-right DFS
        for succ in reversed(list(G.successors(node))):
            if succ not in seen:
                stack.append(succ)

    return visited


def find_path(G: nx.DiGraph, source_id: str, target_id: str) -> list:
    """BFS to find shortest path from source to target. Returns node IDs."""
    if source_id not in G or target_id not in G:
        return []

    try:
        # Try directed path first
        return list(nx.shortest_path(G, source_id, target_id))
    except nx.NetworkXNoPath:
        pass

    # Try undirected path
    try:
        undirected = G.to_undirected()
        return list(nx.shortest_path(undirected, source_id, target_id))
    except nx.NetworkXNoPath:
        return []


def get_broken_flows(G: nx.DiGraph) -> dict:
    """Get all broken flow nodes grouped by type."""
    broken = {"missing_deliveries": [], "missing_invoices": [], "missing_payments": []}

    for nid, data in G.nodes(data=True):
        if data.get("broken"):
            if nid.startswith("MISSING_DEL_"):
                broken["missing_deliveries"].append(nid)
            elif nid.startswith("MISSING_INV_"):
                broken["missing_invoices"].append(nid)
            elif nid.startswith("MISSING_PAY_"):
                broken["missing_payments"].append(nid)

    return broken


def get_entity_subgraph(G: nx.DiGraph, node_ids: list) -> dict:
    """Extract subgraph containing the specified nodes and edges between them."""
    valid = [n for n in node_ids if n in G]
    if not valid:
        return {"nodes": [], "links": []}

    # Include all edges between the specified nodes
    subgraph = G.subgraph(valid)
    return get_graph_json(subgraph)


# ─── Main (for testing) ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "business.db")
    G = build_graph(db_path)
    data = get_graph_json(G)
    print(f"Graph built: {len(data['nodes'])} nodes, {len(data['links'])} links")

    # Breakdown by type
    from collections import Counter
    type_counts = Counter(n.get("type") for n in data["nodes"])
    for t, c in sorted(type_counts.items()):
        print(f"  {t}: {c}")

    broken = get_broken_flows(G)
    total_broken = sum(len(v) for v in broken.values())
    print(f"\nBroken flow nodes: {total_broken}")
    for k, v in broken.items():
        print(f"  {k}: {len(v)}")
