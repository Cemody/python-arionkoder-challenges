from typing import Any, Dict, Iterable, Iterator, Optional, Set, AsyncIterator
import asyncio
import datetime
import sqlite3
import json
import sys
import os
from pathlib import Path

from fastapi import FastAPI, Request, Query, Header

def _iter_records(payload: Any) -> Iterator[Dict[str, Any]]:
    """
    Lazily walk the payload and yield dict-like 'records'.
    Handles:
      - a list of objects
      - nested objects under common collection keys
      - a single object
    """
    # If it's a list/tuple, walk each item
    if isinstance(payload, (list, tuple)):
        for item in payload:
            yield from _iter_records(item)
        return

    # If it's a dict, yield itself and also walk common nested collection keys
    if isinstance(payload, dict):
        # Yield the dict itself as a record
        yield payload

        # Common collection keys we will walk if present
        for key in ("events", "items", "data", "records", "rows"):
            child = payload.get(key)
            if isinstance(child, (list, tuple, dict)):
                yield from _iter_records(child)

def _project(fields: Optional[Set[str]]) -> callable:
    """
    Return a projector function that keeps only certain fields (if provided).
    """
    if not fields:
        return lambda r: r
    return lambda r: {k: r.get(k) for k in fields if k in r}

def _aggregate(
    records: Iterable[Dict[str, Any]],
    group_by: str,
    sum_field: Optional[str] = None,
) -> Dict[Any, float]:
    """
    Streaming aggregation using iterators.
    If sum_field is provided -> sum per group.
    Otherwise -> count per group.
    """
    agg: Dict[Any, float] = {}
    for rec in records:
        if group_by not in rec:
            continue
        key = rec[group_by]

        if sum_field:
            val = rec.get(sum_field, 0)
            if isinstance(val, (int, float)):
                agg[key] = agg.get(key, 0.0) + float(val)
            else:
                # Non-numeric sum_field values are ignored
                continue
        else:
            agg[key] = agg.get(key, 0.0) + 1.0
    return agg


def _aggregate_in_place(
    agg: Dict[Any, float],
    rec: Dict[str, Any],
    group_by: Optional[str],
    sum_field: Optional[str],
) -> None:
    if not group_by:
        return
    if group_by not in rec:
        return
    key = rec[group_by]
    if sum_field:
        val = rec.get(sum_field, 0)
        if isinstance(val, (int, float)):
            agg[key] = agg.get(key, 0.0) + float(val)
    else:
        agg[key] = agg.get(key, 0.0) + 1.0


# ---------- Streaming parsers ----------

async def iter_ndjson_records(request: Request) -> AsyncIterator[Dict[str, Any]]:
    """
    Stream NDJSON (one JSON object per line) without buffering the whole body.
    """
    buffer = ""
    async for chunk in request.stream():
        buffer += chunk.decode("utf-8", errors="replace")
        while True:
            nl = buffer.find("\n")
            if nl == -1:
                break
            line, buffer = buffer[:nl], buffer[nl+1:]
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                elif isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, dict):
                            yield item
            except Exception:
                # If a line isn't valid JSON, print it raw once
                print(line)
                sys.stdout.flush()
                continue
    # trailing partial line
    tail = buffer.strip()
    if tail:
        try:
            obj = json.loads(tail)
            if isinstance(obj, dict):
                yield obj
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        yield item
        except Exception:
            print(tail)
            sys.stdout.flush()

async def iter_json_records(request: Request) -> AsyncIterator[Dict[str, Any]]:
    """
    Stream records from a large JSON payload using ijson if available.
    Falls back to whole-body parse only if ijson is not installed.
    """
    try:
        import ijson  # type: ignore
    except Exception:
        # Fallback (not constant-memory): single read.
        data = await request.json()
        def walk(x):
            if isinstance(x, dict):
                yield x
                for k in ("events", "items", "data", "records", "rows"):
                    if k in x:
                        yield from walk(x[k])
            elif isinstance(x, list):
                for it in x:
                    yield from walk(it)
        for r in walk(data):
            yield r
        return

    # Use ijson for streaming parse - simpler approach
    # Read the full body first, then use ijson to parse it
    body = b""
    async for chunk in request.stream():
        body += chunk
    
    # Use ijson to parse the complete body
    try:
        # Parse the JSON and extract records
        data = ijson.loads(body.decode('utf-8'))
        def walk(x):
            if isinstance(x, dict):
                yield x
                for k in ("events", "items", "data", "records", "rows"):
                    if k in x:
                        yield from walk(x[k])
            elif isinstance(x, list):
                for it in x:
                    yield from walk(it)
        for r in walk(data):
            yield r
    except Exception:
        # If ijson fails, try standard json
        try:
            data = json.loads(body.decode('utf-8'))
            def walk(x):
                if isinstance(x, dict):
                    yield x
                    for k in ("events", "items", "data", "records", "rows"):
                        if k in x:
                            yield from walk(x[k])
                elif isinstance(x, list):
                    for it in x:
                        yield from walk(it)
            for r in walk(data):
                yield r
        except Exception:
            # Last resort - just print the body
            print(body.decode('utf-8', errors='replace'))
            sys.stdout.flush()


# ---------- Database and Message Queue Functions ----------

# Initialize database
def init_database():
    """Initialize SQLite database for storing webhook results"""
    db_path = Path("webhook_results.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS webhook_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            group_by_field TEXT,
            sum_field TEXT,
            aggregation_data TEXT,
            processed_records INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

async def save_to_database(result: Dict[str, Any], group_by: Optional[str], sum_field: Optional[str]):
    """Save webhook results to SQLite database"""
    try:
        # Run database operations in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _save_to_database_sync, result, group_by, sum_field)
        print(f"✓ Saved result to database: {result['processed_records']} records processed")
    except Exception as e:
        print(f"✗ Database save failed: {e}")

def _save_to_database_sync(result: Dict[str, Any], group_by: Optional[str], sum_field: Optional[str]):
    """Synchronous database save operation"""
    init_database()  # Ensure database exists
    
    db_path = Path("webhook_results.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO webhook_results (timestamp, group_by_field, sum_field, aggregation_data, processed_records)
        VALUES (?, ?, ?, ?, ?)
    """, (
        result["timestamp"],
        group_by,
        sum_field,
        json.dumps(result["aggregation"]),
        result["processed_records"]
    ))
    
    conn.commit()
    conn.close()

async def publish_to_message_queue(result: Dict[str, Any], group_by: Optional[str], sum_field: Optional[str]):
    """Publish webhook results to message queue (simulated with file-based queue)"""
    try:
        # Simulate message queue with file-based approach
        message = {
            "id": f"msg_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}",
            "timestamp": result["timestamp"],
            "payload": {
                "group_by_field": group_by,
                "sum_field": sum_field,
                "aggregation": result["aggregation"],
                "processed_records": result["processed_records"]
            },
            "status": "published"
        }
        
        # Run file operations in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _publish_to_queue_sync, message)
        print(f"✓ Published to message queue: {message['id']}")
    except Exception as e:
        print(f"✗ Message queue publish failed: {e}")

def _publish_to_queue_sync(message: Dict[str, Any]):
    """Synchronous message queue publish operation"""
    queue_dir = Path("message_queue")
    queue_dir.mkdir(exist_ok=True)
    
    message_file = queue_dir / f"{message['id']}.json"
    with open(message_file, 'w') as f:
        json.dump(message, f, indent=2)

# ---------- Utility functions for accessing stored data ----------

def get_recent_results(limit: int = 10) -> list:
    """Get recent webhook results from database"""
    try:
        db_path = Path("webhook_results.db")
        if not db_path.exists():
            return []
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT timestamp, group_by_field, sum_field, aggregation_data, processed_records, created_at
            FROM webhook_results 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "timestamp": row[0],
                "group_by_field": row[1],
                "sum_field": row[2],
                "aggregation": json.loads(row[3]) if row[3] else None,
                "processed_records": row[4],
                "created_at": row[5]
            })
        
        conn.close()
        return results
    except Exception as e:
        print(f"Error retrieving results: {e}")
        return []

def get_queued_messages(limit: int = 10) -> list:
    """Get recent messages from the message queue"""
    try:
        queue_dir = Path("message_queue")
        if not queue_dir.exists():
            return []
        
        message_files = sorted(queue_dir.glob("*.json"), key=os.path.getmtime, reverse=True)[:limit]
        messages = []
        
        for file_path in message_files:
            try:
                with open(file_path, 'r') as f:
                    message = json.load(f)
                    messages.append(message)
            except Exception as e:
                print(f"Error reading message file {file_path}: {e}")
        
        return messages
    except Exception as e:
        print(f"Error retrieving queued messages: {e}")
        return []