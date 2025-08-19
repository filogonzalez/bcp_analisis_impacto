# PRD — Codebase Knowledge Graph RAG LLM (Azure Databricks) — **MVP v2**
**Status:** Draft • **Owners:** Data Engineering (Lineage) • **Reviewers:** Governance, Platform  
**Last Change:** Lock parser stack (Python `ast` + SQLGlot), add GraphFrames traversal + path caches, keep SQLLineage dev-only.

---

## 1) Objective (keep it simple)
Deliver a **working MVP** that:
- Parses Databricks notebooks (SQL + PySpark) using **SQLGlot** and Python **`ast`** (no code execution).
- Builds a **Delta-backed knowledge graph** (**nodes** & **edges** in Unity Catalog).
- Uses **GraphFrames** for lineage traversal (multi-hop, shortest paths) and persists **k‑hop path caches** to Delta for low‑latency serve.
- Answers lineage questions via **RAG** (Databricks **Vector Search** + **Model Serving** endpoint).

**Non‑Goals (v2):** Full UI, real-time UC audit-log streaming, multi-agent orchestration, advanced visualizations, enterprise integrations.

---

## 2) MVP Scope
**In-scope**
1. **Path ingestion (DLT)**: Read notebook paths from `/Volumes/...` (CSV/XLSX) with Auto Loader → Bronze → **Silver SCD‑1** using `dlt.apply_changes` (key=`entity_id`, sequence=`processing_ts`).
2. **Content retrieval**: Read source from Workspace (SDK) or pre-exported files. 
3. **Parsing (core)**:
   - **SQL** → **SQLGlot** (Spark dialect): extract **tables, columns, CTEs, JOIN/MERGE**, column expressions.
   - **PySpark** → Python **`ast`**: detect `spark.sql`, `spark.table`, `.write.saveAsTable`, `.save`, `.createOrReplaceTempView`, joins/unions/filters when evident.
4. **Graph build**: Write **nodes** (table|column|transformation|function) & **edges** (contains|defines|consumes|transforms_into) to Unity Catalog Delta.
5. **Traversal**: Build `GraphFrame(V=nodes, E=edges)`; run **BFS / shortestPaths / motif** for **≤ 5 hops**; persist **`path_cache`** table for fast Q&A.
6. **Retrieval & RAG**: Create **Vector Search** index over node/edge text; implement **`answer_lineage(query)`** that does: embed → hybrid retrieve → graft graph facts (including `path_cache`) → call Serving endpoint → return answer + citations.
7. **Quality & Security**: DLT expectations (not-null keys, type checks), UC RBAC, secret scopes.

**Out-of-scope (for MVP)**: Real-time audit streaming, UI app, diagramming, multi-agent flows, external integrations.

---

## 3) Technical Decisions (MVP v2)
- **SQL parser:** **SQLGlot** (Spark/Databricks dialect). Rationale: high accuracy on Spark syntax; zero network deps; low maintenance.
- **Python/PySpark parser:** **`ast`**. Rationale: safe static analysis; robust function/call detection; stdlib.
- **Traversal engine:** **GraphFrames** for BFS/shortestPaths/motif; compute results in batch and **persist** compact **`path_cache`** to Delta.
- **Storage:** Unity Catalog **Delta** tables partitioned by entity/relationship types; audit fields; soft-delete ready.
- **Retrieval:** Databricks **Vector Search** (hybrid: semantic + keyword). 
- **Serving:** Databricks **Model Serving** endpoint (private) for RAG generation.
- **Environment:** Azure Databricks workspace, Lakeflow Declerative Pipelines.

**Why this is faster & simpler**
- Single, proven **SQLGlot** + **`ast`** stack ⇒ no duplicate parsers; fewer heuristics.  
- **GraphFrames** computes multi-hop lineage once; **path_cache** makes Q&A snappy.  
- Everything runs **Databricks‑native** with UC, Vector Search, and Model Serving Endpoint.

---

## 4) High‑Level Architecture (text)
**DLT Ingestion** → Notebook list (Silver)  
**Parser Job** → SQLGlot/`ast` → `nodes_df`, `edges_df`  
**DLT Graph Writer** → MERGE into **nodes_table**, **edges_table**  
**GraphFrames Job** → lineage BFS/shortestPaths/motif → **path_cache**  
**Vector Search** → index nodes/edges text  
**RAG Q&A** → retrieve (hybrid) + graph facts → LLM answer with sources

---

## 5) Data Model (Delta, Unity Catalog)
### 5.1 `nodes_table`
- `node_id` STRING (PK) — stable deterministic ID  
- `entity_type` STRING — `table|column|transformation|function`  
- `entity_name` STRING — canonical name (`catalog.schema.table[.column]`)  
- `properties` MAP<STRING,STRING> — e.g., data_type, nullable, file_path, cell_id  
- `embedding` ARRAY<FLOAT> (nullable) — populated post‑parse  
- `is_active` BOOLEAN, `version` INT, `created_at` TS, `updated_at` TS

### 5.2 `edges_table`
- `edge_id` STRING (PK) — deterministic from (src,tgt,type,expr-hash)  
- `source_node_id` STRING, `target_node_id` STRING  
- `relationship_type` STRING — `contains|defines|consumes|transforms_into`  
- `properties` MAP<STRING,STRING> — expression, join keys, where, confidence  
- `confidence_score` FLOAT (nullable)  
- `is_active` BOOLEAN, `version` INT, `created_at` TS

### 5.3 `path_cache` (derived; optional but recommended)
- `src_node_id` STRING, `tgt_node_id` STRING, `min_hops` INT  
- `path` ARRAY<STRING> (node_ids or compact JSON)  
- `computed_at` TS  
**Partitioning**: by `min_hops` and/or `entity_type` of `tgt` (denormalized cols).

---

## 6) Pipelines & Jobs
### A. **DLT — Notebook Path Ingestion**
- **Source**: `/Volumes/<catalog>/<schema>/nb_paths` (CSV/XLSX).
- **Pattern**: Auto Loader → Bronze → **Silver SCD‑1** with `dlt.apply_changes` (key: `entity_id`, seq: `processing_ts`).
- **Expectations**: non-null `path`, valid workspace prefix.

### B. **Parser Job (Batch/Triggered)**
- **Input**: `notebook_paths_silver` (changed since last run).
- **Steps**:
  1) Read notebook content (SDK or from exported SOURCE).  
  2) Split into cells; classify SQL vs Python.  
  3) **SQL**: `sqlglot.parse_one(sql, read="spark")` → extract tables/columns/CTEs; emit edges (`consumes`, `defines`, `transforms_into`).  
  4) **Python**: `ast.parse(cell)` → walk `ast.Call` to capture `spark.sql/table`, `write.saveAsTable/save`, `createOrReplaceTempView`, joins/unions/filters; map to tables/columns where resolvable.  
  5) Build `nodes_df`, `edges_df` (+ minimal `confidence_score`).

### C. **DLT — Graph Writer**
- Merge `nodes_df`/`edges_df` into UC Delta tables.  
- Expectations: valid IDs, referential integrity (edge src/tgt exist), allowed relationship types.

### D. **GraphFrames — Traversal + Cache**
- Build `GraphFrame(V, E)` from **nodes_table/edges_table**.  
- Compute **BFS/shortestPaths/motif** up to **5 hops**; write **`path_cache`**.  
- Strategy: recompute only impacted subgraph (changed `source_node_id` or `target_node_id`).

### E. **Vector Search — Indexing**
- Create index over `nodes_table` (`entity_type|entity_name|properties`) and `edges_table` (`properties.expression`).  
- Triggered sync after graph writes; primary key `node_id` (or `edge_id`).

### F. **RAG Q&A — Minimal Interface**
- Function `answer_lineage(query: str) -> {answer, sources}`:  
  1) embed query → VS hybrid search (top‑k).  
  2) fetch graph facts + **path_cache** candidates.  
  3) craft grounded prompt (bulleted facts + source refs).  
  4) call Serving endpoint; return **final answer + citations**.

---

## 7) Interfaces (MVP)
- **SQL examples** (workspace SQL):  
  - Upstream of a table: `SELECT ... FROM edges WHERE target_node_id = '<table_id>'`  
  - Downstream of a column: chain `transforms_into` edges (or query `path_cache`).
- **Programmatic**: a small notebook/whl function: `answer_lineage()` (see above).

---

## 8) Non‑Functional Requirements
- **Platform**: Azure Databricks (UC, DLT/Lakeflow Declerative Pipelines, Vector Search, Model Serving).  
- **Runtime**: Python **3.12.8**.  
- **Security**: UC RBAC on tables; secrets in Azure Key Vault backed with Databricks Secret Scopes.  
- **Ops**: Logging (parser errors, unparsable cells), idempotent writes, basic metrics (rows added, edges by type).  
- **Performance targets (MVP)**:  
  - Parse ≥ 95% of common SELECT/CTE/JOIN/MERGE patterns.  
  - 5‑hop lineage answer in **< 1s** when served from `path_cache` (typical scale).  
  - Vector search retrieval **< 300ms** per query (index warmed).
  - Responds with a lineage impact analysis structured table.

---

## 9) Acceptance Criteria
1. DLT builds/maintains **`notebook_paths_silver`**, **`nodes_table`**, **`edges_table`** (and optional **`path_cache`**).  
2. Parser extracts **table & column lineage** from Spark SQL (SELECT/CTE/JOIN/MERGE) and common PySpark IO patterns.  
3. `answer_lineage()` correctly answers:  
   - **Downstream** of `<catalog.schema.table.column>` (list tables/columns).  
   - **Upstream** of `<catalog.schema.table>`.  
   - **Impact** of changing `<column>` (uses `path_cache`).  
   Each answer returns **source snippets/ids** as citations. 
   - **structured table output** of the impact analysis results with origin/sources, transformation, and destinitions/sinks/targets. 
4. Vector index returns relevant entities for free‑text (e.g., “where does `customer_id` flow?”).

---

## 10) Milestones (no dates)
1) **Foundation**: UC tables + DLT skeletons (paths, nodes, edges).  
2) **Parsing**: SQLGlot + `ast` extractors → `nodes_df`/`edges_df`.  
3) **Traversal**: GraphFrames lineage + `path_cache`.  
4) **Search**: Vector Search index (nodes first).  
5) **RAG**: `answer_lineage()` minimal serving.  
6) **Hardening**: expectations, metrics, runbook, sample queries.

---

## 11) Risks & Mitigations
- **Parsing variance** (edge cases): log + fallback rules; mark low‑confidence; add manual override table.  
- **Graph scale**: partition by `entity_type`/`relationship_type`; incremental cache updates; prune to N hops.  
- **RAG hallucination**: always ground with graph facts; include citations; deny if insufficient evidence.  
- **Env constraints**: pre-stage wheels (SQLGlot, GraphFrames); validate against DBR image; pin hashes.  

---

## 12) Deliverables
- **MVP notebooks/jobs**:  
  - `DLT_nb_paths` (Auto Loader → Silver SCD‑1)  
  - `DLT_graph_writer` (nodes/edges)  
  - `job_parser_sqlglot_ast`  
  - `job_graphframes_cache`  
  - `notebook_answer_lineage` (RAG)
- **UC Artifacts**: `nodes_table`, `edges_table`, `path_cache`, VS index.  
- **Docs**: Runbook + sample SQL & Python usage.  

---

## 13) Config (YAML/notebook dict)
```yaml
sources:
  paths_volume: "/Volumes/<catalog>/<schema>/nb_paths"
processing:
  batch_size: 20
  max_retries: 3
graph:
  node_types: [table, column, transformation, function]
  edge_types: [contains, defines, consumes, transforms_into]
  max_hops_cache: 5
retrieval:
  vector_index: "<catalog>.<schema>.kg_nodes_vs"
  top_k: 8
serving:
  endpoint: "codebase-lineage-rag-endpoint"
```

---

## 14) Test Plan (MVP)
- **Unit**: SQLGlot rule coverage (CTE, JOIN, MERGE), `ast` detections (sql/table/read/write/tempview).  
- **Integration**: Parse→Graph→Cache→RAG happy-path; corrupted path rows; unparsable cells.  
- **Perf smoke**: Cache build on 100 notebooks; answer 10 typical queries; measure latencies.  
- **Security**: UC grants; secret resolution; PII redaction in embeddings.

---

## 15) Change Log (v2)
- **Added:** GraphFrames lineage + `path_cache` for fast multi‑hop answers.  
- **Decided:** Parser stack = **SQLGlot + `ast`** (runtime).   
- **Aligned:** UC + Vector Search + Serving only; no external services.

