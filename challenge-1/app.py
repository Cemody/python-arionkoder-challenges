
import json
import sys
import datetime
import asyncio
from typing import Any, Optional, Dict, List

from fastapi import FastAPI, Request, Query, Depends
from fastapi.responses import JSONResponse

from utils import (
    _iter_records, _project, _aggregate,
    iter_ndjson_records, iter_json_records, _aggregate_in_place,
    save_to_database, publish_to_message_queue,
    get_recent_results, get_queued_messages
)

from models import (
    WebhookParams, WebhookResponse, ResultsResponse, 
    MessagesResponse, StatusResponse, ErrorResponse,
    ActivitySummary, HealthCheckResponse, DatabaseResult, MessageQueueResult
)

app = FastAPI()

@app.post("/webhook", response_model=WebhookResponse)
async def webhook(
    request: Request,
    params: WebhookParams = Depends()
) -> WebhookResponse:
    """Stream, optionally project & aggregate records (NDJSON or JSON)."""
    start_time = datetime.datetime.now()
    included_fields = params.get_included_fields()
    projector = _project(included_fields)
    aggregation: Dict[Any, float] = {}

    # Detect NDJSON vs generic JSON
    content_type = request.headers.get("content-type", "")
    is_ndjson = content_type and "application/x-ndjson" in content_type.lower()

    try:
        if is_ndjson:
            async for rec in iter_ndjson_records(request):
                # Print raw record once
                print(json.dumps(rec, ensure_ascii=False))
                sys.stdout.flush()

                # Project then aggregate incrementally
                prec = projector(rec)
                _aggregate_in_place(aggregation, prec, params.group_by, params.sum_field)
        else:
            # Fallback: generic JSON (streamed if ijson available)
            async for rec in iter_json_records(request):
                print(json.dumps(rec, ensure_ascii=False))
                sys.stdout.flush()

                prec = projector(rec)
                _aggregate_in_place(aggregation, prec, params.group_by, params.sum_field)

    # Timing
        end_time = datetime.datetime.now()
        processing_time_ms = (end_time - start_time).total_seconds() * 1000

        result = WebhookResponse(
            ok=True,
            aggregation=aggregation or None,  # None if no group_by
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            processed_records=len(aggregation) if aggregation else 0,
            processing_time_ms=processing_time_ms
        )

    # Persist + publish (fire & forget)
        await asyncio.gather(
            save_to_database(result.model_dump(), params.group_by, params.sum_field),
            publish_to_message_queue(result.model_dump(), params.group_by, params.sum_field),
            return_exceptions=True
        )

        return result

    except Exception as e:
        # If body not JSON: stream raw text lines
        try:
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
            
            return WebhookResponse(
                ok=True,
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                processed_records=0,
                note="received non-JSON body; printed as text"
            )
        except Exception:
            return JSONResponse(
                status_code=400,
                content=ErrorResponse(
                    error=f"Failed to process request: {str(e)}",
                    error_code="PROCESSING_ERROR",
                    timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
                ).model_dump()
            )


################################ Endpoints: inspection & status ################################

@app.get("/results", response_model=ResultsResponse)
async def get_results(
    limit: int = Query(10, description="Number of recent results to return", ge=1, le=100)
) -> ResultsResponse:
    """Return recent stored webhook aggregation results."""
    try:
        results = get_recent_results(limit)
        # Normalize to models
        database_results = []
        for result in results:
            if isinstance(result, dict):
                database_results.append(DatabaseResult(**result))
            
        return ResultsResponse(
            ok=True,
            results=database_results,
            count=len(database_results),
            pagination={
                "limit": limit,
                "total": len(database_results)
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=f"Failed to retrieve results: {str(e)}",
                error_code="DATABASE_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
            ).model_dump()
        )

@app.get("/messages", response_model=MessagesResponse)
async def get_messages(
    limit: int = Query(10, description="Number of recent messages to return", ge=1, le=100)
) -> MessagesResponse:
    """Return recent queued message payloads."""
    try:
        messages = get_queued_messages(limit)
        # Normalize to models
        queue_messages = []
        for message in messages:
            if isinstance(message, dict):
                queue_messages.append(MessageQueueResult(**message))
                
        return MessagesResponse(
            ok=True,
            messages=queue_messages,
            count=len(queue_messages),
            queue_stats={
                "total_messages": len(queue_messages),
                "limit_applied": limit
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=f"Failed to retrieve messages: {str(e)}",
                error_code="QUEUE_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
            ).model_dump()
        )

@app.get("/status", response_model=StatusResponse)
async def get_status() -> StatusResponse:
    """Return lightweight service status + recent activity summary."""
    try:
        recent_results = get_recent_results(5)
        recent_messages = get_queued_messages(5)
        
        # Build activity snapshot (placeholder metrics where noted)
        activity = ActivitySummary(
            total_requests=len(recent_results),
            successful_requests=len([r for r in recent_results if r.get('ok', True)]),
            failed_requests=len([r for r in recent_results if not r.get('ok', True)]),
            avg_processing_time_ms=None,  # Could calculate if we had timing data
            last_request_time=datetime.datetime.fromisoformat(recent_results[0]["timestamp"]) if recent_results else None,
            active_connections=1  # Placeholder
        )
        
        return StatusResponse(
            ok=True,
            status="healthy",
            uptime_seconds=0.0,  # Could track actual uptime
            version="1.0.0",
            recent_activity=activity,
            system_metrics={
                "database_records": len(recent_results),
                "queued_messages": len(recent_messages)
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=f"Failed to get status: {str(e)}",
                error_code="STATUS_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
            ).model_dump()
        )

@app.get("/health", response_model=HealthCheckResponse)
async def health_check() -> HealthCheckResponse:
    """Basic dependency health probe (fast)."""
    start_time = datetime.datetime.now()
    
    try:
        # Placeholder component checks (extend with real probes later)
        checks = {
            "database": True,  # Could test actual database connection
            "message_queue": True,  # Could test actual queue connection
            "memory": True,  # Could check memory usage
            "disk": True  # Could check disk space
        }
        
        end_time = datetime.datetime.now()
        response_time = (end_time - start_time).total_seconds() * 1000
        
        status = "healthy" if all(checks.values()) else "unhealthy"
        
        return HealthCheckResponse(
            status=status,
            timestamp=start_time,
            checks=checks,
            response_time_ms=response_time
        )
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content=ErrorResponse(
                error=f"Health check failed: {str(e)}",
                error_code="HEALTH_CHECK_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
            ).model_dump()
        )