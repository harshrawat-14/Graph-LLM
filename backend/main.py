#!/usr/bin/env python3
"""
FastAPI Backend
===============
Main application entry point exposing all API endpoints.
Graph is built at startup and cached in app.state.
"""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from graph_builder import (
    build_graph, get_graph_json, get_node_neighbors,
    trace_flow, find_path, get_broken_flows, get_entity_subgraph
)
from flow_detector import get_all_broken_flows
from query_engine import QueryEngine, search_entities

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = str(BASE_DIR / "data" / "business.db")


# ─── Lifespan (Startup/Shutdown) ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build graph and init query engine at startup."""
    print("🚀 Building graph from database...")
    app.state.graph = build_graph(DB_PATH)
    graph_data = get_graph_json(app.state.graph)
    print(f"   ✓ Graph: {len(graph_data['nodes'])} nodes, {len(graph_data['links'])} links")

    from config import LLM_API_KEY, LLM_PROVIDER
    app.state.query_engine = QueryEngine(DB_PATH)
    if LLM_API_KEY:
        print(f"   ✓ {LLM_PROVIDER.upper()} API configured")
    else:
        print(f"   ⚠ No API key found — LLM queries will use fallback mode")

    yield

    print("👋 Shutting down")


# ─── App ─────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SAP O2C Graph Query Engine",
    description="Graph-based data modeling and NL query system for SAP Order-to-Cash data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request / Response Models ───────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    session_id: str = ""


class QueryResponse(BaseModel):
    answer: str
    sql_used: Optional[str] = None
    data: list = []
    highlighted_node_ids: list = []
    intent: str = ""
    confidence: str = ""
    error: Optional[str] = None


# ─── Endpoints ───────────────────────────────────────────────────────────────

@app.get("/api/graph")
async def get_full_graph():
    """Full graph as JSON {nodes, links}."""
    return get_graph_json(app.state.graph)


@app.get("/api/graph/neighbors")
async def get_neighbors(node_id: str = Query(...), depth: int = Query(1, ge=1, le=5)):
    """Get node neighbors within depth hops."""
    result = get_node_neighbors(app.state.graph, node_id, depth)
    if not result["nodes"]:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return result


@app.get("/api/graph/trace")
async def get_trace(start_id: str = Query(...)):
    """DFS downstream trace from a node."""
    node_ids = trace_flow(app.state.graph, start_id)
    if not node_ids:
        raise HTTPException(status_code=404, detail=f"Node '{start_id}' not found")
    subgraph = get_entity_subgraph(app.state.graph, node_ids)
    return {"trace": node_ids, **subgraph}


@app.get("/api/graph/path")
async def get_path(source: str = Query(...), target: str = Query(...)):
    """BFS shortest path between two nodes."""
    path = find_path(app.state.graph, source, target)
    if not path:
        raise HTTPException(status_code=404, detail=f"No path found between '{source}' and '{target}'")
    subgraph = get_entity_subgraph(app.state.graph, path)
    return {"path": path, **subgraph}


@app.get("/api/broken-flows")
async def get_broken_flows_endpoint():
    """All broken flow summaries."""
    return get_all_broken_flows(DB_PATH)


@app.post("/api/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """Natural language query → SQL → data-backed answer."""
    result = app.state.query_engine.process_query(
        request.question, request.session_id
    )
    return QueryResponse(**result)


@app.get("/api/entities/search")
async def entity_search(q: str = Query(..., min_length=1)):
    """Fuzzy entity search by name."""
    results = search_entities(q, DB_PATH)
    return {"results": results}


@app.get("/api/stats")
async def get_stats():
    """Dataset summary statistics."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    try:
        stats = {}
        for table in ["customers", "products", "orders", "order_items",
                       "deliveries", "invoices", "payments", "journal_entries"]:
            stats[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        # Additional analytics
        stats["cancelled_invoices"] = conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE status = 'CANCELLED'"
        ).fetchone()[0]
        stats["paid_invoices"] = conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE status = 'PAID'"
        ).fetchone()[0]
        stats["unpaid_invoices"] = conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE status = 'UNPAID'"
        ).fetchone()[0]

        # Broken flows
        stats["orders_without_delivery"] = conn.execute("""
            SELECT COUNT(*) FROM orders o
            LEFT JOIN deliveries d ON d.order_id = o.id WHERE d.id IS NULL
        """).fetchone()[0]
        stats["deliveries_without_invoice"] = conn.execute("""
            SELECT COUNT(*) FROM deliveries d
            LEFT JOIN invoices i ON i.delivery_id = d.id WHERE i.id IS NULL
        """).fetchone()[0]
        stats["invoices_without_payment"] = conn.execute("""
            SELECT COUNT(*) FROM invoices i
            LEFT JOIN payments p ON p.invoice_id = i.id
            WHERE p.id IS NULL AND i.status != 'CANCELLED'
        """).fetchone()[0]

        graph_data = get_graph_json(app.state.graph)
        stats["graph_nodes"] = len(graph_data["nodes"])
        stats["graph_links"] = len(graph_data["links"])

        broken = get_broken_flows(app.state.graph)
        stats["broken_flow_nodes"] = sum(len(v) for v in broken.values())

        return stats
    finally:
        conn.close()


# ─── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
