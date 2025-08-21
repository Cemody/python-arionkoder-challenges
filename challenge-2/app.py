import json
import sys
import datetime
import asyncio
import time
from typing import Any, Optional, Dict, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query, HTTPException, Depends
from fastapi.responses import JSONResponse

from utils import (
    ResourceManager, DatabaseConnection, APIConnection, 
    CacheConnection, save_connection_log, get_connection_logs,
    get_performance_analytics
)

from models import (
    ResourceTestParams, ResourceTestResponse, ResourceTestResult,
    ConnectionStatus, ResourceType, ConnectionMetrics,
    ConnectionLogsResponse, PerformanceResponse, StatusResponse,
    ErrorResponse
)

app = FastAPI()

@app.post("/resources/test", response_model=ResourceTestResponse)
async def test_resources(
    params: ResourceTestParams = Depends()
) -> ResourceTestResponse:
    """
    Test multiple resource connections using the custom context manager.
    Demonstrates robust resource management with automatic cleanup.
    """
    start_time = datetime.datetime.now()
    requested_resources = params.get_resource_types_list()
    
    results = {}
    connection_logs = []
    
    try:
        # Use the custom context manager to handle multiple resources
        async with ResourceManager(requested_resources) as resources:
            for resource_name, connection in resources.connections.items():
                test_start = datetime.datetime.now()
                
                try:
                    # Test each resource connection
                    test_result = await connection.test_connection()
                    test_end = datetime.datetime.now()
                    test_duration = (test_end - test_start).total_seconds() * 1000
                    
                    # Create connection metrics
                    metrics = ConnectionMetrics(
                        connection_time_ms=test_duration,
                        response_time_ms=test_duration,
                        retry_count=0
                    )
                    
                    results[resource_name] = ResourceTestResult(
                        resource_type=ResourceType(resource_name),
                        status=ConnectionStatus.CONNECTED,
                        success=True,
                        result=test_result,
                        connection_time=test_start,
                        test_duration_ms=test_duration,
                        metrics=metrics
                    )
                    
                    connection_logs.append({
                        "resource": resource_name,
                        "action": "test",
                        "status": "success",
                        "timestamp": test_start.isoformat()
                    })
                    
                except Exception as e:
                    test_end = datetime.datetime.now()
                    test_duration = (test_end - test_start).total_seconds() * 1000
                    
                    results[resource_name] = ResourceTestResult(
                        resource_type=ResourceType(resource_name),
                        status=ConnectionStatus.ERROR,
                        success=False,
                        error_message=str(e),
                        connection_time=test_start,
                        test_duration_ms=test_duration
                    )
                    
                    connection_logs.append({
                        "resource": resource_name,
                        "action": "test",
                        "status": "error",
                        "error": str(e),
                        "timestamp": test_start.isoformat()
                    })
        
        # Save connection logs
        await save_connection_log(connection_logs)
        
        # Calculate totals
        end_time = datetime.datetime.now()
        total_duration = (end_time - start_time).total_seconds() * 1000
        successful_count = sum(1 for r in results.values() if r.success)
        
        # Create summary
        summary = {
            "total_tested": len(results),
            "successful": successful_count,
            "failed": len(results) - successful_count,
            "success_rate": (successful_count / len(results) * 100) if results else 0
        }
        
        return ResourceTestResponse(
            ok=True,
            results=results,
            summary=summary,
            total_duration_ms=total_duration,
            timestamp=end_time
        )
        
    except Exception as e:
        # Log the error
        error_log = {
            "resource": "resource_manager",
            "action": "test_multiple",
            "status": "error",
            "error": str(e),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        await save_connection_log([error_log])
        
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=f"Resource testing failed: {str(e)}",
                error_code="RESOURCE_TEST_ERROR",
                error_type="RESOURCE_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            ).dict()
        )

@app.post("/resources/execute")
async def execute_resource_operations(
    request: Request,
    operations: Optional[str] = Query(None, description="JSON string of operations to execute")
) -> Dict[str, Any]:
    """
    Execute operations on multiple resources using the context manager.
    Example operations: [{"resource": "database", "operation": "query", "data": {...}}]
    """
    if not operations:
        raise HTTPException(status_code=400, detail="Operations parameter is required")
    
    try:
        operation_list = json.loads(operations)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in operations parameter")
    
    if not isinstance(operation_list, list):
        raise HTTPException(status_code=400, detail="Operations must be a list")
    
    # Extract required resource types from operations
    required_resources = list(set(op.get("resource") for op in operation_list if op.get("resource")))
    
    results = {}
    connection_logs = []
    
    try:
        async with ResourceManager(required_resources) as resources:
            for i, operation in enumerate(operation_list):
                resource_name = operation.get("resource")
                operation_type = operation.get("operation")
                operation_data = operation.get("data", {})
                
                if resource_name not in resources.connections:
                    results[f"operation_{i}"] = {
                        "status": "error",
                        "error": f"Resource '{resource_name}' not available"
                    }
                    continue
                
                try:
                    connection = resources.connections[resource_name]
                    result = await connection.execute_operation(operation_type, operation_data)
                    
                    results[f"operation_{i}"] = {
                        "status": "success",
                        "resource": resource_name,
                        "operation": operation_type,
                        "result": result,
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    
                    connection_logs.append({
                        "resource": resource_name,
                        "action": f"execute_{operation_type}",
                        "status": "success",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    })
                    
                except Exception as e:
                    results[f"operation_{i}"] = {
                        "status": "error",
                        "resource": resource_name,
                        "operation": operation_type,
                        "error": str(e),
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    
                    connection_logs.append({
                        "resource": resource_name,
                        "action": f"execute_{operation_type}",
                        "status": "error",
                        "error": str(e),
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    })
        
        # Save connection logs
        await save_connection_log(connection_logs)
        
        return {
            "ok": True,
            "executed_operations": len(operation_list),
            "results": results,
            "successful_operations": len([r for r in results.values() if r["status"] == "success"]),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
    except Exception as e:
        error_log = {
            "resource": "resource_manager",
            "action": "execute_operations",
            "status": "error",
            "error": str(e),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        await save_connection_log([error_log])
        
        raise HTTPException(status_code=500, detail=f"Resource execution error: {str(e)}")

# ---------- Additional endpoints for monitoring and management ----------

@app.get("/resources/status", response_model=StatusResponse)
async def get_resource_status() -> StatusResponse:
    """Get status of all available resource types"""
    start_time = datetime.datetime.now()
    available_resources = ["database", "api", "cache"]
    resource_health = {}
    active_connections = {}
    
    for resource_type in available_resources:
        try:
            async with ResourceManager([resource_type]) as resources:
                connection = resources.connections[resource_type]
                test_result = await connection.test_connection()
                resource_health[resource_type] = True
                active_connections[resource_type] = 1  # Placeholder
        except Exception as e:
            resource_health[resource_type] = False
            active_connections[resource_type] = 0
    
    end_time = datetime.datetime.now()
    uptime = (end_time - start_time).total_seconds()
    
    all_healthy = all(resource_health.values())
    status_desc = "healthy" if all_healthy else "degraded"
    
    return StatusResponse(
        ok=True,
        status=status_desc,
        uptime_seconds=uptime,
        active_connections=active_connections,
        resource_health=resource_health,
        last_activity=end_time
    )

@app.get("/resources/logs", response_model=ConnectionLogsResponse) 
async def get_logs(
    limit: int = Query(20, description="Number of recent logs to return", ge=1, le=1000)
) -> ConnectionLogsResponse:
    """Get recent connection logs"""
    try:
        logs = await get_connection_logs(limit)
        # Convert logs to ConnectionLog models if needed
        connection_logs = []
        for log in logs:
            if isinstance(log, dict):
                connection_logs.append(log)
        
        return ConnectionLogsResponse(
            ok=True,
            logs=connection_logs,
            count=len(connection_logs),
            filters_applied={"limit": limit},
            pagination={"total": len(connection_logs), "limit": limit}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=f"Failed to retrieve logs: {str(e)}",
                error_code="LOG_RETRIEVAL_ERROR",
                error_type="DATABASE_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            ).dict()
        )

@app.get("/resources/analytics", response_model=PerformanceResponse)
async def get_analytics(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    hours: int = Query(24, description="Time period in hours", ge=1, le=168)
) -> PerformanceResponse:
    """Get comprehensive performance analytics"""
    try:
        analytics = await get_performance_analytics(resource_type, hours)
        
        return PerformanceResponse(
            ok=True,
            analytics=analytics,
            generated_at=datetime.datetime.now(datetime.timezone.utc),
            time_range={
                "start": datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours),
                "end": datetime.datetime.now(datetime.timezone.utc)
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error=f"Failed to retrieve analytics: {str(e)}",
                error_code="ANALYTICS_ERROR",
                error_type="PERFORMANCE_ERROR",
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            ).dict()
        )

@app.get("/resources/health")
async def health_check():
    """Enhanced health check endpoint with performance summary"""
    try:
        # Test each resource type quickly
        health_results = {}
        overall_start = time.time()
        
        for resource_type in ["database", "api", "cache"]:
            try:
                start_time = time.time()
                async with ResourceManager([resource_type]) as resources:
                    connection = resources.connections[resource_type]
                    await connection.test_connection()
                
                response_time = time.time() - start_time
                health_results[resource_type] = {
                    "status": "healthy",
                    "response_time": response_time
                }
            except Exception as e:
                response_time = time.time() - start_time
                health_results[resource_type] = {
                    "status": "unhealthy",
                    "error": str(e),
                    "response_time": response_time
                }
        
        overall_time = time.time() - overall_start
        healthy_count = sum(1 for r in health_results.values() if r["status"] == "healthy")
        
        return {
            "ok": True,
            "status": "healthy" if healthy_count == len(health_results) else "degraded",
            "service": "resource_manager",
            "resources": health_results,
            "healthy_resources": healthy_count,
            "total_resources": len(health_results),
            "overall_response_time": overall_time,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {
            "ok": False,
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
