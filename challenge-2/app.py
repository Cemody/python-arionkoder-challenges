import json
import sys
import datetime
import asyncio
import time
from typing import Any, Optional, Dict, List
from contextlib import asynccontextmanager

from utils import (
    ResourceManager, DatabaseConnection, APIConnection, 
    CacheConnection, save_connection_log, get_connection_logs,
    get_performance_analytics
)

from fastapi import FastAPI, Request, Query, HTTPException

app = FastAPI()

@app.post("/resources/test")
async def test_resources(
    request: Request,
    resource_types: Optional[str] = Query(None, description="Comma-separated resource types to test (database,api,cache)")
) -> Dict[str, Any]:
    """
    Test multiple resource connections using the custom context manager.
    Demonstrates robust resource management with automatic cleanup.
    """
    requested_resources = []
    if resource_types:
        requested_resources = [r.strip() for r in resource_types.split(",")]
    else:
        # Default to all resource types
        requested_resources = ["database", "api", "cache"]
    
    results = {}
    connection_logs = []
    
    try:
        # Use the custom context manager to handle multiple resources
        async with ResourceManager(requested_resources) as resources:
            for resource_name, connection in resources.items():
                try:
                    # Test each resource connection
                    test_result = await connection.test_connection()
                    results[resource_name] = {
                        "status": "success",
                        "result": test_result,
                        "connection_time": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    
                    connection_logs.append({
                        "resource": resource_name,
                        "action": "test",
                        "status": "success",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    })
                    
                except Exception as e:
                    results[resource_name] = {
                        "status": "error",
                        "error": str(e),
                        "connection_time": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                    
                    connection_logs.append({
                        "resource": resource_name,
                        "action": "test",
                        "status": "error",
                        "error": str(e),
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    })
        
        # Save connection logs
        await save_connection_log(connection_logs)
        
        return {
            "ok": True,
            "tested_resources": list(results.keys()),
            "results": results,
            "total_resources": len(results),
            "successful_connections": len([r for r in results.values() if r["status"] == "success"]),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
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
        
        raise HTTPException(status_code=500, detail=f"Resource manager error: {str(e)}")

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
                
                if resource_name not in resources:
                    results[f"operation_{i}"] = {
                        "status": "error",
                        "error": f"Resource '{resource_name}' not available"
                    }
                    continue
                
                try:
                    connection = resources[resource_name]
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

@app.get("/resources/status")
async def get_resource_status():
    """Get status of all available resource types"""
    available_resources = ["database", "api", "cache"]
    status_results = {}
    
    for resource_type in available_resources:
        try:
            async with ResourceManager([resource_type]) as resources:
                connection = resources[resource_type]
                test_result = await connection.test_connection()
                status_results[resource_type] = {
                    "status": "available",
                    "last_test": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "details": test_result
                }
        except Exception as e:
            status_results[resource_type] = {
                "status": "unavailable",
                "error": str(e),
                "last_test": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
    
    return {
        "ok": True,
        "resources": status_results,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

@app.get("/resources/logs")
async def get_logs(limit: int = Query(20, description="Number of recent logs to return")):
    """Get recent connection logs"""
    logs = await get_connection_logs(limit)
    return {
        "ok": True,
        "logs": logs,
        "count": len(logs),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

@app.get("/resources/analytics")
async def get_analytics(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    hours: int = Query(24, description="Time period in hours")
):
    """Get comprehensive performance analytics"""
    analytics = await get_performance_analytics(resource_type, hours)
    return {
        "ok": True,
        "analytics": analytics,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

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
                    connection = resources[resource_type]
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
