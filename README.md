# ArionKoder Challenges

A collection of five focused Python mini-projects showcasing patterns in streaming data processing, resource management, meta‑programming, lazy evaluation, and distributed task scheduling.

Each challenge ships with:
- A small FastAPI (or pure Python) example app / library code.
- A `demo_test.py` (and/or tests folder) you can run with `pytest`.
- Minimal dependencies (most standard library + a few popular packages).

## Quick Start
```bash
# (Optional) create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux

# Install shared dependencies (root consolidated list)
pip install -r requirements.txt  # if you create one (see note below)

# Or install per-feature on demand
pip install fastapi uvicorn aiohttp pydantic psutil ijson pytest
```

Run a single challenge demo test:
```bash
cd challenge-1
py demo_test.py
pytest -q tests/
```
Run all demo tests (bash one-liner):
```bash
for d in challenge-*; do echo "== $d =="; (cd "$d" && pytest -q demo_test.py || echo "(failed)"); done
```
Start any FastAPI app for manual exploration (example for challenge 1):
```bash
cd challenge-1
uvicorn app:app --reload
```

---
## Challenge 1 – Memory‑Efficient Data Pipeline
**Goal:** Stream, transform (projection), and aggregate arbitrary JSON/NDJSON webhook data in constant memory; persist results to SQLite and a file‑based simulated message queue.

**Key Concepts:**
- Incremental parsing (NDJSON & optional `ijson` streaming for large JSON arrays).
- On‑the‑fly projection & aggregation (group + sum or count) using iterators.
- Dual sink: SQLite (`webhook_results.db`) and file message queue (`message_queue/`).

**Test:**
```bash
cd challenge-1
py demo_test.py
pytest -q tests/
```
**Run App:** `uvicorn app:app --reload`

---
## Challenge 2 – Custom Context Manager for Resource Management
**Goal:** Robust async context manager that acquires/releases heterogeneous resources (DB, external HTTP API, in‑memory cache) with performance metrics, logging, and error isolation.

**Key Concepts:**
- Parallel async acquisition & teardown with detailed timing.
- Pluggable resource classes (`DatabaseConnection`, `APIConnection`, `CacheConnection`).
- Structured connection logs + basic analytics endpoints.

**Test:**
```bash
cd challenge-2
py demo_test.py
pytest -q tests/
```
**Run App:** `uvicorn app:app --reload`

---
## Challenge 3 – Advanced Meta‑Programming (Contracts & Plugins)
**Goal:** Enforce API contracts (processors / validators / transformers) via a metaclass that validates method signatures & attributes and auto‑registers plugin classes.

**Key Concepts:**
- Metaclass (`ContractEnforcerMeta`) instrumentation & contract checking.
- Automatic plugin registry with runtime performance metrics.
- Typed FastAPI endpoints for process / validate / transform operations.

**Test:**
```bash
cd challenge-3
py demo_test.py
pytest -q tests/
```
**Run App:** `uvicorn app:app --reload`

---
## Challenge 4 – Custom Iterator with Lazy Evaluation
**Goal:** Provide a chainable lazy collection supporting composable operations (`map`, `filter`, `skip`, `take`, `batch`, pagination) executing only upon iteration while optionally caching results.

**Key Concepts:**
- Deferred execution pipeline encoded as op tuples.
- Pagination helpers (`page`, `paginate`) and grouping (`batch` / `chunk`).
- Memory usage scales with emitted output, not raw input.

**Test:**
```bash
cd challenge-4
py demo_test.py
pytest -q tests/
```
**Demo Script:** Run `python demo.py` (or inspect `main.py` if present) for examples.

---
## Challenge 5 – Distributed Task Scheduler
**Goal:** Priority‑aware task scheduler with hybrid worker execution (threads + processes) supporting task submission, status tracking, cancellation, metrics, and cleanup.

**Key Concepts:**
- Hybrid execution pool: CPU‑bound tasks via processes; others via threads.
- Task lifecycle tracking (pending → running → completed/failed/cancelled/retrying).
- Scheduler + system metrics (throughput, utilization, queue stats, CPU/memory).

**Test:**
```bash
cd challenge-5
py demo_test.py
pytest -q tests/
```
**Run App:** `uvicorn app:app --reload`

---
## Common Demo Test Pattern
Each `demo_test.py` typically:
1. Imports and initializes core components (or FastAPI test client).
2. Executes a representative workflow (e.g., submit webhook, run resource test, process plugin data, build lazy pipeline, submit task).
3. Asserts on key invariants (status flags, aggregation counts, validation results, task states, etc.).

You can open any `demo_test.py` for quick reference of the intended usage surface.

---
## Suggested Root `requirements.txt`
A consolidated (approximate) dependency list you can add at repo root if desired:
```
fastapi
uvicorn
aiohttp
pydantic
psutil
ijson  # optional, enables streaming large JSON arrays in challenge 1
pytest
```
Install with:
```bash
pip install -r requirements.txt
```

> Note: SQLite, `asyncio`, `logging`, `json`, `multiprocessing`, etc. are from the Python standard library.

---
## Troubleshooting
| Issue | Tip |
|-------|-----|
| Missing optional streaming (`ijson`) | Install it or fall back to standard JSON parsing. |
| Port already in use | Run `uvicorn app:app --port 8001` or kill existing process. |
| psutil not installed (challenge 5 metrics) | `pip install psutil` to enable richer system metrics. |
| Tests hang on task scheduler shutdown | Ensure no long‑running tasks; increase timeout or cancel tasks. |

---
## License / Usage
Educational sample code for demonstrating Python patterns. Adapt freely.

---
## At a Glance
| Challenge | Focus | Primary File(s) |
|-----------|-------|-----------------|
| 1 | Streaming + aggregation | `challenge-1/app.py`, `utils.py` |
| 2 | Resource context mgmt | `challenge-2/app.py`, `utils.py` |
| 3 | Meta‑programming / plugins | `challenge-3/app.py`, `utils.py` |
| 4 | Lazy evaluation iterator | `challenge-4/lazy.py` |
| 5 | Distributed scheduling | `challenge-5/app.py`, `utils.py` |

Happy hacking! 🚀
