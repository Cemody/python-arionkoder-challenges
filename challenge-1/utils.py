from typing import Any, Dict, Iterable, Iterator, Optional, Set, AsyncIterator

from fastapi import FastAPI, Request, Query, Header
import sys
import json

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