
import json
import sys
import datetime
import asyncio
from typing import Any, Optional, Set, Dict, Iterable

from utils import (
    _iter_records, _project, _aggregate,
    iter_ndjson_records, iter_json_records, _aggregate_in_place,
    save_to_database, publish_to_message_queue,
    get_recent_results, get_queued_messages
)

from fastapi import FastAPI, Request, Query, Header

app = FastAPI()

@app.post("/webhook")
async def webhook(
    request: Request,
    group_by: Optional[str] = Query(None, description="Field name to group by"),
    sum_field: Optional[str] = Query(None, description="Numeric field to sum per group"),
    include: Optional[str]   = Query(None, description="Comma-separated field list for projection"),
) -> Dict[str, Any]:
    """
    Streaming receiver that:
      - prints each record once (as compact JSON)
      - transforms via projection (include=...)
      - aggregates on the fly (group_by[&sum_field])
    Works in constant memory for NDJSON and (with ijson) large JSON.
    """
    included_fields: Optional[Set[str]] = set(map(str.strip, include.split(","))) if include else None
    projector = _project(included_fields)
    aggregation: Dict[Any, float] = {}

    # Get content type from request headers directly
    content_type = request.headers.get("content-type", "")
    is_ndjson = content_type and "application/x-ndjson" in content_type.lower()

    try:
        if is_ndjson:
            async for rec in iter_ndjson_records(request):
                # print once
                print(json.dumps(rec, ensure_ascii=False))
                sys.stdout.flush()

                # transform + aggregate in constant memory
                prec = projector(rec)
                _aggregate_in_place(aggregation, prec, group_by, sum_field)
        else:
            # Generic JSON (constant memory if ijson is installed)
            async for rec in iter_json_records(request):
                print(json.dumps(rec, ensure_ascii=False))
                sys.stdout.flush()

                prec = projector(rec)
                _aggregate_in_place(aggregation, prec, group_by, sum_field)

        result = {
            "ok": True,
            "aggregation": aggregation or None,  # None if no group_by
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "processed_records": len(aggregation) if aggregation else 0
        }

        # Save results to database and publish to message queue
        await asyncio.gather(
            save_to_database(result, group_by, sum_field),
            publish_to_message_queue(result, group_by, sum_field),
            return_exceptions=True
        )

        return result

    except Exception:
        # Non-JSON? Stream raw text lines to stdout without buffering.
        line_buf = ""
        async for chunk in request.stream():
            line_buf += chunk.decode("utf-8", errors="replace")
            while True:
                nl = line_buf.find("\n")
                if nl == -1:
                    break
                line, line_buf = line_buf[:nl], line_buf[nl+1:]
                print(line)
                sys.stdout.flush()
        if line_buf:
            print(line_buf)
            sys.stdout.flush()
        return {"ok": True, "note": "received non-JSON body; printed as text"}


# ---------- Additional endpoints for viewing stored data ----------

@app.get("/results")
async def get_results(limit: int = Query(10, description="Number of recent results to return")):
    """Get recent webhook processing results from database"""
    results = get_recent_results(limit)
    return {
        "ok": True,
        "results": results,
        "count": len(results)
    }

@app.get("/messages")
async def get_messages(limit: int = Query(10, description="Number of recent messages to return")):
    """Get recent messages from the message queue"""
    messages = get_queued_messages(limit)
    return {
        "ok": True,
        "messages": messages,
        "count": len(messages)
    }

@app.get("/status")
async def get_status():
    """Get system status including recent activity"""
    recent_results = get_recent_results(5)
    recent_messages = get_queued_messages(5)
    
    return {
        "ok": True,
        "status": "running",
        "recent_activity": {
            "database_records": len(recent_results),
            "queued_messages": len(recent_messages),
            "last_processed": recent_results[0]["timestamp"] if recent_results else None
        }
    }