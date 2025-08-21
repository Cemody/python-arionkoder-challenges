"""
Challenge 2 - Pydantic Models

Data models for the advanced context managers and resource management system.
"""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
from enum import Enum


class ResourceType(str, Enum):
    """Supported resource types"""
    DATABASE = "database"
    API = "api" 
    CACHE = "cache"
    FILE = "file"
    NETWORK = "network"
    CUSTOM = "custom"


class ConnectionStatus(str, Enum):
    """Connection status enumeration"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ResourceTestParams(BaseModel):
    """Parameters for resource testing"""
    resource_types: Optional[str] = Field(
        None,
        description="Comma-separated resource types to test",
        example="database,api,cache"
    )
    timeout_seconds: Optional[float] = Field(
        30.0,
        description="Timeout for resource operations in seconds",
        ge=1.0,
        le=300.0
    )
    max_retries: Optional[int] = Field(
        3,
        description="Maximum number of retry attempts",
        ge=0,
        le=10
    )
    validate_connections: Optional[bool] = Field(
        True,
        description="Whether to validate connections before testing"
    )
    
    @field_validator('resource_types')
    @classmethod
    def validate_resource_types(cls, v):
        """Validate resource types are supported"""
        if v:
            types = [t.strip().lower() for t in v.split(",")]
            valid_types = [rt.value for rt in ResourceType]
            invalid_types = [t for t in types if t not in valid_types]
            if invalid_types:
                raise ValueError(f"Invalid resource types: {invalid_types}. Valid types: {valid_types}")
        return v
    
    def get_resource_types_list(self) -> List[str]:
        """Get list of resource types to test"""
        if self.resource_types:
            return [t.strip().lower() for t in self.resource_types.split(",")]
        return [ResourceType.DATABASE, ResourceType.API, ResourceType.CACHE]


class ConnectionMetrics(BaseModel):
    """Connection performance metrics"""
    connection_time_ms: float = Field(
        ...,
        description="Time to establish connection in milliseconds",
        ge=0
    )
    response_time_ms: Optional[float] = Field(
        None,
        description="Response time in milliseconds",
        ge=0
    )
    data_transferred_bytes: Optional[int] = Field(
        None,
        description="Amount of data transferred in bytes",
        ge=0
    )
    retry_count: int = Field(
        0,
        description="Number of retries performed",
        ge=0
    )
    peak_memory_mb: Optional[float] = Field(
        None,
        description="Peak memory usage in megabytes",
        ge=0
    )


class ResourceTestResult(BaseModel):
    """Result of testing a single resource"""
    resource_type: ResourceType = Field(..., description="Type of resource tested")
    status: ConnectionStatus = Field(..., description="Connection status")
    success: bool = Field(..., description="Whether the test was successful")
    result: Optional[Dict[str, Any]] = Field(
        None,
        description="Test result data"
    )
    error_message: Optional[str] = Field(
        None,
        description="Error message if test failed"
    )
    connection_time: datetime = Field(
        ...,
        description="When the connection was established"
    )
    test_duration_ms: float = Field(
        ...,
        description="Total test duration in milliseconds",
        ge=0
    )
    metrics: Optional[ConnectionMetrics] = Field(
        None,
        description="Connection performance metrics"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional test metadata"
    )


class ResourceTestResponse(BaseModel):
    """Response from resource testing endpoint"""
    ok: bool = Field(True, description="Overall test success status")
    results: Dict[str, ResourceTestResult] = Field(
        ...,
        description="Test results for each resource type"
    )
    summary: Dict[str, Any] = Field(
        ...,
        description="Summary of test results"
    )
    total_duration_ms: float = Field(
        ...,
        description="Total test duration in milliseconds",
        ge=0
    )
    timestamp: datetime = Field(
        ...,
        description="Test execution timestamp"
    )
    
    @model_validator(mode='after')
    def validate_results(self):
        """Validate test results consistency"""
        results = self.results
        ok = self.ok
        
        # Check if 'ok' status matches individual results
        if results:
            any_failed = any(not result.success for result in results.values())
            if ok and any_failed:
                self.ok = False
        
        return self


class ConnectionLog(BaseModel):
    """Connection log entry"""
    log_id: str = Field(..., description="Unique log entry ID")
    resource_type: ResourceType = Field(..., description="Resource type")
    operation: str = Field(..., description="Operation performed")
    status: ConnectionStatus = Field(..., description="Operation status")
    timestamp: datetime = Field(..., description="Log timestamp")
    duration_ms: Optional[float] = Field(
        None,
        description="Operation duration in milliseconds",
        ge=0
    )
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional operation details"
    )
    user_id: Optional[str] = Field(None, description="User ID if applicable")
    session_id: Optional[str] = Field(None, description="Session ID if applicable")


class ConnectionLogsResponse(BaseModel):
    """Response for connection logs endpoint"""
    ok: bool = Field(True, description="Request success status")
    logs: List[ConnectionLog] = Field(..., description="Connection log entries")
    count: int = Field(..., description="Total number of log entries", ge=0)
    filters_applied: Optional[Dict[str, Any]] = Field(
        None,
        description="Filters that were applied to the logs"
    )
    pagination: Optional[Dict[str, Any]] = Field(
        None,
        description="Pagination information"
    )


class PerformanceMetrics(BaseModel):
    """Performance analytics metrics"""
    metric_name: str = Field(..., description="Name of the metric")
    value: Union[float, int] = Field(..., description="Metric value")
    unit: str = Field(..., description="Metric unit")
    timestamp: datetime = Field(..., description="Metric timestamp")
    tags: Optional[Dict[str, str]] = Field(
        None,
        description="Metric tags for categorization"
    )


class PerformanceAnalytics(BaseModel):
    """Performance analytics summary"""
    total_connections: int = Field(..., description="Total connections made", ge=0)
    successful_connections: int = Field(..., description="Successful connections", ge=0)
    failed_connections: int = Field(..., description="Failed connections", ge=0)
    avg_connection_time_ms: float = Field(
        ...,
        description="Average connection time in milliseconds",
        ge=0
    )
    max_connection_time_ms: float = Field(
        ...,
        description="Maximum connection time in milliseconds",
        ge=0
    )
    min_connection_time_ms: float = Field(
        ...,
        description="Minimum connection time in milliseconds",
        ge=0
    )
    success_rate: float = Field(
        ...,
        description="Connection success rate as percentage",
        ge=0,
        le=100
    )
    metrics_by_type: Dict[str, PerformanceMetrics] = Field(
        ...,
        description="Performance metrics grouped by resource type"
    )
    time_series: Optional[List[PerformanceMetrics]] = Field(
        None,
        description="Time series performance data"
    )


class PerformanceResponse(BaseModel):
    """Response for performance analytics endpoint"""
    ok: bool = Field(True, description="Request success status")
    analytics: PerformanceAnalytics = Field(..., description="Performance analytics data")
    generated_at: datetime = Field(..., description="Analytics generation timestamp")
    time_range: Optional[Dict[str, datetime]] = Field(
        None,
        description="Time range for analytics data"
    )


class ResourceConfiguration(BaseModel):
    """Resource configuration"""
    resource_type: ResourceType = Field(..., description="Resource type")
    connection_string: Optional[str] = Field(
        None,
        description="Connection string (may be redacted for security)"
    )
    timeout_seconds: float = Field(
        30.0,
        description="Connection timeout in seconds",
        ge=1.0,
        le=300.0
    )
    max_connections: Optional[int] = Field(
        None,
        description="Maximum number of connections",
        ge=1
    )
    retry_policy: Optional[Dict[str, Any]] = Field(
        None,
        description="Retry policy configuration"
    )
    health_check_interval_seconds: Optional[float] = Field(
        None,
        description="Health check interval in seconds",
        ge=1.0
    )
    custom_properties: Optional[Dict[str, Any]] = Field(
        None,
        description="Custom resource-specific properties"
    )


class StatusResponse(BaseModel):
    """System status response"""
    ok: bool = Field(True, description="System status")
    status: str = Field(..., description="Status description")
    uptime_seconds: float = Field(..., description="System uptime in seconds", ge=0)
    active_connections: Dict[str, int] = Field(
        ...,
        description="Number of active connections by resource type"
    )
    resource_health: Dict[str, bool] = Field(
        ...,
        description="Health status for each resource type"
    )
    memory_usage_mb: Optional[float] = Field(
        None,
        description="Current memory usage in megabytes",
        ge=0
    )
    cpu_usage_percent: Optional[float] = Field(
        None,
        description="Current CPU usage percentage",
        ge=0,
        le=100
    )
    last_activity: Optional[datetime] = Field(
        None,
        description="Timestamp of last activity"
    )


class ErrorResponse(BaseModel):
    """Error response model"""
    ok: bool = Field(False, description="Request success status")
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    error_type: Optional[str] = Field(None, description="Error type/category")
    details: Optional[Dict[str, Any]] = Field(None, description="Error details")
    resource_type: Optional[ResourceType] = Field(
        None,
        description="Resource type if error is resource-specific"
    )
    timestamp: datetime = Field(..., description="Error timestamp")
    stack_trace: Optional[str] = Field(
        None,
        description="Stack trace (only in debug mode)"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "ok": False,
                "error": "Failed to connect to database",
                "error_code": "CONNECTION_TIMEOUT",
                "error_type": "RESOURCE_ERROR",
                "details": {"timeout_seconds": 30, "host": "localhost"},
                "resource_type": "database",
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
