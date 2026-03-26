# SAP Order-to-Cash Graph Query Engine

A production-quality graph-based data modeling and natural language query system for SAP Order-to-Cash (O2C) business data.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    React Frontend                         │
│  ┌─────────────────────┐  ┌────────────────────────────┐ │
│  │  Graph Canvas        │  │  Chat Panel                │ │
│  │  (react-force-graph) │  │  NL → SQL → Data → Answer  │ │
│  │  860 nodes, 950 edges│  │  Collapsible SQL + Tables   │ │
│  │  Glow highlighting   │  │  Confidence badges          │ │
│  └─────────────────────┘  └────────────────────────────┘ │
└──────────────────────┬───────────────────────────────────┘
                       │ HTTP (REST API)
┌──────────────────────┴───────────────────────────────────┐
│                  FastAPI Backend                          │
│  ┌────────────┐ ┌──────────────┐ ┌─────────────────────┐│
│  │ Graph API   │ │ Query Engine │ │ Flow Detector       ││
│  │ NetworkX    │ │ 5-Layer      │ │ Broken O2C chains   ││
│  │ DiGraph     │ │ NL→SQL       │ │ SQL analytics       ││
│  └─────┬──────┘ └──────┬───────┘ └─────────┬───────────┘│
│        │               │                    │            │
│        └───────────────┴────────────────────┘            │
│                        │                                 │
│              ┌─────────┴──────────┐                      │
│              │  SQLite Database   │                      │
│              │  9 normalized      │                      │
│              │  tables + indices  │                      │
│              └────────────────────┘                      │
└──────────────────────────────────────────────────────────┘
```

## Why SQLite + NetworkX over Neo4j

| Criterion | SQLite + NetworkX | Neo4j |
|-----------|------------------|-------|
| **Setup** | Zero config, single file DB | Requires server, JVM |
| **Deployment** | Single `pip install` | Docker image, separate service |
| **SQL analytics** | Native, fast aggregations | Requires Cypher (different syntax) |
| **Graph traversal** | In-memory NetworkX (sub-ms) | Network-hop latency |
| **Data size** | ~1K entities → fits in RAM | Overkill for this scale |
| **LLM integration** | Gemini generates SQLite SQL directly | Would need Cypher generation |
| **Portability** | DB is a single file | Requires persistent volume |

SQLite handles the relational queries (aggregations, joins, filters) while NetworkX provides in-memory graph traversal (DFS traces, BFS paths, neighborhood expansion) — best of both worlds.

## LLM Prompting Strategy

### Schema-Constrained Generation
The query engine provides only relevant DDL to Gemini, with column-level comments explaining business semantics. This prevents hallucination of non-existent columns.

### 3-Layer Guardrail Design

1. **Pre-LLM Keyword Classifier** — Rejects off-domain questions before any LLM call (zero token spend on "What's the weather?")
2. **Schema Constraint** — System prompt restricts SQL generation to existing tables/columns only
3. **Post-Execution Validation** — SQL errors caught and returned as structured errors, never as hallucinated answers

### Structured Output + Few-Shot
- Forces JSON output format with intent classification
- 7 few-shot examples covering all intent types
- Chain-of-thought reasoning (silent, not output)

## Broken Flow Detection

Incomplete O2C chains are represented as **first-class nodes** in the graph:

- `MISSING_DEL_{order_id}` — Order with no delivery
- `MISSING_INV_{delivery_id}` — Delivery with no invoice
- `MISSING_PAY_{invoice_id}` — Invoice with no payment

These nodes are colored red with dashed borders, making broken flows immediately visible in the visualization.

## Running

### Prerequisites
- Python 3.9+
- Node.js 18+
- [Gemini API key](https://makersuite.google.com/app/apikey) (free tier works)

### Quick Start

```bash
# 1. Clone and enter the project
cd GRAPH-LLM

# 2. Setup a Python virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# 3. Install backend dependencies
pip install -r backend/requirements.txt

# 4. Set Gemini API key (optional but required for natural language queries)
export GEMINI_API_KEY="AIzaSyCf2KmLd_WDp1r2za6UFnGOSqfNG6tYLP4"

# 5. Run data ingestion to build the local SQLite database
python3 backend/ingest.py

# 6. Start the FastAPI backend server
python3 backend/main.py &

# 7. Open a new terminal tab (or split pane) for the frontend
cd frontend
npm install
npm run dev
```

Open http://localhost:5173

### Docker

```bash
export GEMINI_API_KEY="your-key-here"
docker-compose up --build
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/graph` | GET | Full graph JSON {nodes, links} |
| `/api/graph/neighbors` | GET | Node neighbors (?node_id=X&depth=1) |
| `/api/graph/trace` | GET | DFS downstream trace (?start_id=X) |
| `/api/graph/path` | GET | BFS shortest path (?source=X&target=Y) |
| `/api/broken-flows` | GET | All broken flow summaries |
| `/api/query` | POST | NL query → SQL → answer |
| `/api/entities/search` | GET | Fuzzy entity search (?q=name) |
| `/api/stats` | GET | Dataset summary statistics |

## Example Queries

| Query | Expected Output |
|-------|----------------|
| "Which products appear in the most invoices?" | Ranked list with counts |
| "Show all incomplete order flows" | Orders missing delivery/invoice/payment |
| "Trace the flow of order 740506" | Full O2C chain with status at each step |
| "How many orders does each customer have?" | Customer-level order counts |
| "What is the capital of France?" | Polite rejection (off-domain) |

## Data Pipeline

SAP JSONL data → `ingest.py` → SQLite (9 tables) → `graph_builder.py` → NetworkX (860 nodes) → FastAPI → React UI

### Entity Counts
- 8 customers, 100 orders, 167 order items, 69 products
- 86 deliveries, 163 invoices, 120 payments, 123 journal entries
- 24 broken flow markers (14 missing deliveries, 3 missing invoices, 7 missing payments)
