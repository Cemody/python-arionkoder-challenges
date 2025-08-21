"""Models for distributed task scheduler (tasks, workers, metrics)."""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum
import json


class TaskPriority(str, Enum):
    """Priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class TaskStatus(str, Enum):
    """Execution lifecycle states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskSubmissionRequest(BaseModel):
    """Submit task request payload."""
    task_name: str = Field(
        ..., 
        description="Name/type of the task to execute",
        min_length=1,
        max_length=100
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="Task payload data"
    )
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL,
        description="Task priority level"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retry attempts"
    )
    timeout: int = Field(
        default=300,
        ge=1,
        le=3600,
        description="Task timeout in seconds"
    )
    
    @field_validator('payload')
    @classmethod
    def validate_payload(cls, v):
        """Ensure payload JSON serializable."""
        try:
            json.dumps(v)
            return v
        except (TypeError, ValueError) as e:
            raise ValueError(f"Payload must be JSON serializable: {e}")


class TaskSubmissionResponse(BaseModel):
    """Submission acknowledgment."""
    task_id: str = Field(..., description="Unique task identifier")
    status: TaskStatus = Field(..., description="Current task status")
    message: str = Field(..., description="Status message")
    queue_position: int = Field(..., description="Position in task queue")
    estimated_start_time: Optional[str] = Field(None, description="Estimated start time")
    processing_time_ms: float = Field(..., description="Request processing time in milliseconds")


class TaskResult(BaseModel):
    """Result wrapper for completed task."""
    success: bool = Field(..., description="Whether task completed successfully")
    result_data: Optional[Dict[str, Any]] = Field(None, description="Task result data")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    metrics: Optional[Dict[str, Any]] = Field(None, description="Execution metrics")


class TaskStatusResponse(BaseModel):
    """Status query response."""
    task_id: str = Field(..., description="Task identifier")
    status: TaskStatus = Field(..., description="Current task status")
    progress: int = Field(default=0, ge=0, le=100, description="Task progress percentage")
    result: Optional[TaskResult] = Field(None, description="Task result if completed")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    worker_id: Optional[str] = Field(None, description="ID of worker processing task")
    started_at: Optional[str] = Field(None, description="Task start timestamp")
    completed_at: Optional[str] = Field(None, description="Task completion timestamp")
    processing_time_ms: Optional[float] = Field(None, description="Total processing time")
    retry_count: int = Field(default=0, description="Number of retry attempts made")


class WorkerInfo(BaseModel):
    """Worker runtime stats."""
    worker_id: str = Field(..., description="Unique worker identifier")
    status: str = Field(..., description="Worker status (idle, busy, error)")
    current_task: Optional[str] = Field(None, description="Currently processing task ID")
    tasks_completed: int = Field(default=0, description="Total tasks completed")
    tasks_failed: int = Field(default=0, description="Total tasks failed")
    uptime_seconds: float = Field(..., description="Worker uptime in seconds")
    last_heartbeat: str = Field(..., description="Last heartbeat timestamp")
    cpu_usage_percent: Optional[float] = Field(None, description="Worker CPU usage")
    memory_usage_mb: Optional[float] = Field(None, description="Worker memory usage")


class WorkerStatusResponse(BaseModel):
    """Aggregate worker pool stats."""
    total_workers: int = Field(..., description="Total number of workers")
    active_workers: int = Field(..., description="Number of active workers")
    idle_workers: int = Field(..., description="Number of idle workers")
    workers: List[WorkerInfo] = Field(..., description="Detailed worker information")
    queue_size: int = Field(..., description="Current task queue size")
    completed_tasks: int = Field(..., description="Total completed tasks")
    failed_tasks: int = Field(..., description="Total failed tasks")


class SystemMetricsResponse(BaseModel):
    """System + scheduler metrics snapshot."""
    uptime_seconds: float = Field(..., description="System uptime in seconds")
    cpu_usage_percent: float = Field(..., description="System CPU usage percentage")
    memory_usage_mb: float = Field(..., description="System memory usage in MB")
    total_tasks_processed: int = Field(..., description="Total tasks processed")
    tasks_per_second: float = Field(..., description="Current task processing rate")
    average_task_time_ms: float = Field(..., description="Average task processing time")
    worker_utilization: float = Field(..., description="Worker pool utilization percentage")
    queue_utilization: float = Field(..., description="Task queue utilization percentage")


class SchedulerStats(BaseModel):
    """Scheduler performance counters."""
    total_submitted: int = Field(default=0, description="Total tasks submitted")
    total_processed: int = Field(default=0, description="Total tasks processed")
    total_failed: int = Field(default=0, description="Total tasks failed")
    total_cancelled: int = Field(default=0, description="Total tasks cancelled")
    average_queue_time_ms: float = Field(default=0, description="Average time in queue")
    average_processing_time_ms: float = Field(default=0, description="Average processing time")
    throughput_per_minute: float = Field(default=0, description="Tasks per minute")
    peak_queue_size: int = Field(default=0, description="Peak queue size reached")


class ErrorResponse(BaseModel):
    """Error envelope."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: str = Field(..., description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request identifier")


class StatusResponse(BaseModel):
    """Generic status payload."""
    status: str = Field(..., description="Status message")
    timestamp: str = Field(..., description="Response timestamp")
    data: Optional[Dict[str, Any]] = Field(None, description="Additional data")
