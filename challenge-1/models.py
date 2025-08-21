"""
Data models for the streaming data processing webhook system.
"""

from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime
import re


class WebhookParams(BaseModel):
    """Parameters for webhook processing"""
    group_by: Optional[str] = Field(
        None, 
        description="Field name to group by for aggregation"
    )
    sum_field: Optional[str] = Field(
        None, 
        description="Numeric field to sum per group"
    )
    include: Optional[str] = Field(
        None, 
        description="Comma-separated field list for projection"
    )
    
    @field_validator('include')
    @classmethod
    def validate_include_fields(cls, v):
        """Validate include fields are properly formatted"""
        if v:
            fields = [f.strip() for f in v.split(",")]
            if any(not f for f in fields):
                raise ValueError("Include fields cannot be empty")
            # Check for valid field names (alphanumeric + underscore)
            for field in fields:
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', field):
                    raise ValueError(f"Invalid field name: {field}")
        return v
    
    @model_validator(mode='after')
    def validate_aggregation_params(self):
        """Validate that sum_field is only used with group_by"""
        if self.sum_field and not self.group_by:
            raise ValueError("sum_field requires group_by to be specified")
        
        return self
    
    def get_included_fields(self) -> Optional[Set[str]]:
        """Get the set of included fields for projection"""
        if self.include:
            return set(f.strip() for f in self.include.split(","))
        return None


class WebhookResponse(BaseModel):
    """Response from webhook processing"""
    ok: bool = Field(True, description="Processing success status")
    aggregation: Optional[Dict[Any, Any]] = Field(
        None, 
        description="Aggregation results grouped by specified field"
    )
    timestamp: str = Field(
        ..., 
        description="Processing timestamp in ISO format"
    )
    processed_records: int = Field(
        ..., 
        description="Number of records processed",
        ge=0
    )
    note: Optional[str] = Field(
        None, 
        description="Additional processing notes"
    )
    processing_time_ms: Optional[float] = Field(
        None, 
        description="Processing time in milliseconds",
        ge=0
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ok": True,
                "aggregation": {
                    "engineering": 150000,
                    "marketing": 120000
                },
                "timestamp": "2024-01-01T12:00:00Z",
                "processed_records": 5,
                "processing_time_ms": 123.45
            }
        }
    )


class DatabaseResult(BaseModel):
    """Database operation result"""
    timestamp: str = Field(..., description="Processing timestamp")
    group_by_field: Optional[str] = Field(None, description="Field used for grouping")
    sum_field: Optional[str] = Field(None, description="Field used for summing")
    aggregation: Optional[Dict[Any, Any]] = Field(None, description="Aggregation results")
    processed_records: int = Field(..., description="Number of records processed", ge=0)
    created_at: str = Field(..., description="Database record creation timestamp")


class MessageQueueResult(BaseModel):
    """Message queue operation result"""
    id: str = Field(..., description="Message ID")
    timestamp: str = Field(..., description="Message timestamp")
    payload: Dict[str, Any] = Field(..., description="Message payload")
    status: str = Field(..., description="Message status")


class ResultsResponse(BaseModel):
    """Response for results endpoint"""
    ok: bool = Field(True, description="Request success status")
    results: List[DatabaseResult] = Field(
        ..., 
        description="List of recent database results"
    )
    count: int = Field(
        ..., 
        description="Total number of results",
        ge=0
    )
    pagination: Optional[Dict[str, Any]] = Field(
        None, 
        description="Pagination information"
    )


class MessagesResponse(BaseModel):
    """Response for messages endpoint"""
    ok: bool = Field(True, description="Request success status")
    messages: List[MessageQueueResult] = Field(
        ..., 
        description="List of queued messages"
    )
    count: int = Field(
        ..., 
        description="Total number of messages",
        ge=0
    )
    queue_stats: Optional[Dict[str, Any]] = Field(
        None, 
        description="Queue statistics"
    )


class ActivitySummary(BaseModel):
    """Recent activity summary"""
    total_requests: int = Field(..., description="Total requests processed", ge=0)
    successful_requests: int = Field(..., description="Successful requests", ge=0)
    failed_requests: int = Field(..., description="Failed requests", ge=0)
    avg_processing_time_ms: Optional[float] = Field(
        None, 
        description="Average processing time in milliseconds",
        ge=0
    )
    last_request_time: Optional[datetime] = Field(
        None, 
        description="Timestamp of last request"
    )
    active_connections: int = Field(..., description="Current active connections", ge=0)


class StatusResponse(BaseModel):
    """Response for status endpoint"""
    ok: bool = Field(True, description="System status")
    status: str = Field(
        ..., 
        description="System status description"
    )
    uptime_seconds: Optional[float] = Field(
        None, 
        description="System uptime in seconds",
        ge=0
    )
    version: Optional[str] = Field(
        None, 
        description="Application version"
    )
    recent_activity: ActivitySummary = Field(
        ..., 
        description="Recent system activity summary"
    )
    system_metrics: Optional[Dict[str, Any]] = Field(
        None, 
        description="Additional system metrics"
    )


class ErrorResponse(BaseModel):
    """Error response model"""
    ok: bool = Field(False, description="Request success status")
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Error details")
    timestamp: str = Field(
        ..., 
        description="Error timestamp in ISO format"
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ok": False,
                "error": "Invalid field name in include parameter",
                "error_code": "VALIDATION_ERROR",
                "details": {"field": "include", "value": "invalid-field-name"},
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Health status")
    timestamp: datetime = Field(..., description="Health check timestamp")
    checks: Dict[str, bool] = Field(
        ..., 
        description="Individual health check results"
    )
    response_time_ms: float = Field(
        ..., 
        description="Health check response time in milliseconds",
        ge=0
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "healthy",
                "timestamp": "2024-01-01T12:00:00Z",
                "checks": {
                    "database": True,
                    "message_queue": True,
                    "memory": True,
                    "disk": True
                },
                "response_time_ms": 15.3
            }
        }
    )
