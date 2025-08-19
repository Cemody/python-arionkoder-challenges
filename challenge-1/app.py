
import json
import sys
from typing import Any, Optional, Set, Dict, Iterable

from utils import (
    _iter_records, _project, _aggregate,
    iter_ndjson_records, iter_json_records, _aggregate_in_place
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

        return {
            "ok": True,
            "aggregation": aggregation or None,  # None if no group_by
        }

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