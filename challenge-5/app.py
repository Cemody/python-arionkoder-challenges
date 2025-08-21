"""
Distributed Task Scheduler Application

A Python-based task scheduling system that distributes tasks across multiple worker processes.
Features:
- Task submission and management
- Multi-process worker pool
- Task priority and scheduling
- Status monitoring and reporting
- Resource management and load balancing
"""

import datetime
import uuid
from typing import Any, Dict
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks

from utils import (
    TaskScheduler,
    save_task_to_database, 
    get_task_status,
    cleanup_completed_tasks, 
    get_system_metrics
)

from models import (
    TaskSubmissionRequest, 
    TaskSubmissionResponse, 
    TaskStatusResponse,
    WorkerStatusResponse,
    SystemMetricsResponse,
    TaskStatus
)

scheduler = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global scheduler
    
    # Startup
    print("🚀 Starting Distributed Task Scheduler...")
    scheduler = TaskScheduler(max_workers=4, queue_size=100)
    await scheduler.start()
    print("✅ Task Scheduler initialized with worker pool")
    
    yield
    
    # Shutdown
    print("🛑 Shutting down Task Scheduler...")
    await scheduler.shutdown()
    print("✅ Cleanup completed")

app = FastAPI(
    title="Distributed Task Scheduler",
    description="A high-performance task scheduling system with distributed workers",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/tasks/submit", response_model=TaskSubmissionResponse)
async def submit_task(
    request: TaskSubmissionRequest,
    background_tasks: BackgroundTasks
) -> TaskSubmissionResponse:
    """
    Submit a new task for processing.
    Tasks are distributed across available workers based on priority and load.
    """
    start_time = datetime.datetime.now()
    
    # Initialize scheduler if not already done (for testing)
    global scheduler
    if scheduler is None:
        scheduler = TaskScheduler(max_workers=4, queue_size=100)
        await scheduler.start()
    
    try:
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # Create task with metadata
        task = {
            'id': task_id,
            'name': request.task_name,
            'payload': request.payload,
            'priority': request.priority,
            'max_retries': request.max_retries,
            'timeout': request.timeout,
            'created_at': start_time.isoformat(),
            'status': TaskStatus.PENDING
        }
        
        # Submit to scheduler
        success = await scheduler.submit_task(task)
        
        if not success:
            raise HTTPException(
                status_code=503, 
                detail="Task queue is full. Please try again later."
            )
        
        # Save to database asynchronously
        background_tasks.add_task(save_task_to_database, task)
        
        processing_time = (datetime.datetime.now() - start_time).total_seconds() * 1000
        
        return TaskSubmissionResponse(
            task_id=task_id,
            status=TaskStatus.PENDING,
            message="Task submitted successfully",
            queue_position=await scheduler.get_queue_position(task_id),
            estimated_start_time=await scheduler.estimate_start_time(task_id),
            processing_time_ms=processing_time
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit task: {str(e)}")

@app.get("/tasks/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status_endpoint(task_id: str) -> TaskStatusResponse:
    """Get the current status of a specific task"""
    try:
        # Initialize scheduler if not already done (for testing)
        global scheduler
        if scheduler is None:
            scheduler = TaskScheduler(max_workers=4, queue_size=100)
            await scheduler.start()
        
        # Check scheduler first for live status
        live_status = await scheduler.get_task_status(task_id)
        
        if live_status:
            return TaskStatusResponse(
                task_id=task_id,
                status=live_status['status'],
                progress=live_status.get('progress', 0),
                result=live_status.get('result'),
                error_message=live_status.get('error'),
                worker_id=live_status.get('worker_id'),
                started_at=live_status.get('started_at'),
                completed_at=live_status.get('completed_at'),
                processing_time_ms=live_status.get('processing_time_ms'),
                retry_count=live_status.get('retry_count', 0)
            )
        
        # Fall back to database lookup
        db_status = await get_task_status(task_id)
        if not db_status:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return TaskStatusResponse(**db_status)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")

@app.get("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str) -> Dict[str, Any]:
    """Cancel a pending or running task"""
    try:
        # Initialize scheduler if not already done (for testing)
        global scheduler
        if scheduler is None:
            scheduler = TaskScheduler(max_workers=4, queue_size=100)
            await scheduler.start()
        
        success = await scheduler.cancel_task(task_id)
        
        if success:
            return {
                "task_id": task_id,
                "status": "cancelled",
                "message": "Task cancelled successfully"
            }
        else:
            raise HTTPException(
                status_code=400, 
                detail="Task cannot be cancelled (not found or already completed)"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cancel task: {str(e)}")

@app.get("/workers/status", response_model=WorkerStatusResponse)
async def get_workers_status() -> WorkerStatusResponse:
    """Get status information for all workers"""
    try:
        # Initialize scheduler if not already done (for testing)
        global scheduler
        if scheduler is None:
            scheduler = TaskScheduler(max_workers=4, queue_size=100)
            await scheduler.start()
        
        worker_stats = await scheduler.get_worker_stats()
        
        return WorkerStatusResponse(
            total_workers=worker_stats['total_workers'],
            active_workers=worker_stats['active_workers'],
            idle_workers=worker_stats['idle_workers'],
            workers=worker_stats['workers'],
            queue_size=worker_stats['queue_size'],
            completed_tasks=worker_stats['completed_tasks'],
            failed_tasks=worker_stats['failed_tasks']
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get worker status: {str(e)}")

@app.get("/system/metrics", response_model=SystemMetricsResponse)
async def get_system_metrics_endpoint() -> SystemMetricsResponse:
    """Get comprehensive system metrics and performance data"""
    try:
        # Initialize scheduler if not already done (for testing)
        global scheduler
        if scheduler is None:
            scheduler = TaskScheduler(max_workers=4, queue_size=100)
            await scheduler.start()
        
        metrics = await get_system_metrics()
        scheduler_stats = await scheduler.get_scheduler_stats()
        
        return SystemMetricsResponse(
            uptime_seconds=metrics['uptime_seconds'],
            cpu_usage_percent=metrics['cpu_usage_percent'],
            memory_usage_mb=metrics['memory_usage_mb'],
            total_tasks_processed=scheduler_stats['total_processed'],
            tasks_per_second=scheduler_stats['throughput'],
            average_task_time_ms=scheduler_stats['avg_processing_time'],
            worker_utilization=scheduler_stats['worker_utilization'],
            queue_utilization=scheduler_stats['queue_utilization']
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get system metrics: {str(e)}")

@app.post("/system/cleanup")
async def cleanup_system(
    background_tasks: BackgroundTasks,
    older_than_hours: int = 24
) -> Dict[str, Any]:
    """Clean up completed tasks older than specified hours"""
    try:
        background_tasks.add_task(cleanup_completed_tasks, older_than_hours)
        
        return {
            "message": f"Cleanup initiated for tasks older than {older_than_hours} hours",
            "status": "started"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start cleanup: {str(e)}")

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    try:
        # Initialize scheduler if not already done (for testing)
        global scheduler
        if scheduler is None:
            scheduler = TaskScheduler(max_workers=4, queue_size=100)
            await scheduler.start()
        
        is_healthy = await scheduler.health_check()
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "timestamp": datetime.datetime.now().isoformat(),
            "scheduler_running": is_healthy,
            "worker_pool_active": await scheduler.worker_pool_active()
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.datetime.now().isoformat(),
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
