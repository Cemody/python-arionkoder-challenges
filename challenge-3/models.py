"""
Challenge 3 - Pydantic Models

Data models for the meta-programming plugin system with metaclass enforcement.
"""

from typing import Any, Dict, List, Optional, Union, Type
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from datetime import datetime
from enum import Enum


class PluginType(str, Enum):
    """Plugin type enumeration"""
    PROCESSOR = "processor"
    VALIDATOR = "validator"
    TRANSFORMER = "transformer"
    CUSTOM = "custom"


class ContractType(str, Enum):
    """Contract type enumeration"""
    DATA_PROCESSOR = "data_processor"
    VALIDATOR = "validator"
    TRANSFORMER = "transformer"


class PluginStatus(str, Enum):
    """Plugin status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    LOADING = "loading"
    DISABLED = "disabled"


class ProcessRequest(BaseModel):
    """Request for data processing"""
    processor_type: str = Field(
        ...,
        description="Type of processor to use",
        example="JSONProcessor"
    )
    data: Dict[str, Any] = Field(
        ...,
        description="Data to process"
    )
    options: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Processing options"
    )
    validate_input: Optional[bool] = Field(
        True,
        description="Whether to validate input before processing"
    )
    
    @field_validator('processor_type')
    @classmethod
    def validate_processor_type(cls, v):
        """Validate processor type is not empty"""
        if not v or not v.strip():
            raise ValueError("Processor type cannot be empty")
        return v.strip()


class ValidationRequest(BaseModel):
    """Request for data validation"""
    validator_type: str = Field(
        ...,
        description="Type of validator to use",
        example="SchemaValidator"
    )
    data: Any = Field(
        ...,
        description="Data to validate"
    )
    rules: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Validation rules"
    )
    strict: Optional[bool] = Field(
        True,
        description="Whether to use strict validation"
    )
    
    @field_validator('validator_type')
    @classmethod
    def validate_validator_type(cls, v):
        """Validate validator type is not empty"""
        if not v or not v.strip():
            raise ValueError("Validator type cannot be empty")
        return v.strip()


class TransformRequest(BaseModel):
    """Request for data transformation"""
    transformer_type: str = Field(
        ...,
        description="Type of transformer to use",
        example="UppercaseTransformer"
    )
    data: Any = Field(
        ...,
        description="Data to transform"
    )
    options: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Transformation options"
    )
    reverse: Optional[bool] = Field(
        False,
        description="Whether to apply reverse transformation"
    )
    
    @field_validator('transformer_type')
    @classmethod
    def validate_transformer_type(cls, v):
        """Validate transformer type is not empty"""
        if not v or not v.strip():
            raise ValueError("Transformer type cannot be empty")
        return v.strip()


class PluginInstanceRequest(BaseModel):
    """Request for creating plugin instance"""
    plugin_category: PluginType = Field(
        ...,
        description="Plugin category"
    )
    plugin_name: str = Field(
        ...,
        description="Plugin name to instantiate"
    )
    args: Optional[List[Any]] = Field(
        default_factory=list,
        description="Positional arguments for plugin constructor"
    )
    kwargs: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Keyword arguments for plugin constructor"
    )


class ProcessResponse(BaseModel):
    """Response from data processing"""
    success: bool = Field(..., description="Processing success status")
    result: Dict[str, Any] = Field(..., description="Processing result")
    processor_type: str = Field(..., description="Processor type used")
    processing_time_ms: float = Field(
        ...,
        description="Processing time in milliseconds",
        ge=0
    )
    input_validation: Optional[bool] = Field(
        None,
        description="Whether input validation passed"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional processing metadata"
    )
    timestamp: datetime = Field(..., description="Processing timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "result": {"processed_data": "example"},
                "processor_type": "JSONProcessor",
                "processing_time_ms": 45.67,
                "input_validation": True,
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class ValidationResponse(BaseModel):
    """Response from data validation"""
    is_valid: bool = Field(..., description="Validation result")
    errors: List[str] = Field(
        default_factory=list,
        description="Validation error messages"
    )
    validator_type: str = Field(..., description="Validator type used")
    validation_time_ms: float = Field(
        ...,
        description="Validation time in milliseconds",
        ge=0
    )
    rules_applied: Optional[Dict[str, Any]] = Field(
        None,
        description="Validation rules that were applied"
    )
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional validation details"
    )
    timestamp: datetime = Field(..., description="Validation timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "is_valid": True,
                "errors": [],
                "validator_type": "SchemaValidator",
                "validation_time_ms": 12.34,
                "rules_applied": {"schema": "user_schema"},
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class TransformResponse(BaseModel):
    """Response from data transformation"""
    success: bool = Field(..., description="Transformation success status")
    result: Any = Field(..., description="Transformation result")
    transformer_type: str = Field(..., description="Transformer type used")
    transformation_time_ms: float = Field(
        ...,
        description="Transformation time in milliseconds",
        ge=0
    )
    options_applied: Optional[Dict[str, Any]] = Field(
        None,
        description="Transformation options that were applied"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional transformation metadata"
    )
    timestamp: datetime = Field(..., description="Transformation timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "result": "TRANSFORMED DATA",
                "transformer_type": "UppercaseTransformer",
                "transformation_time_ms": 8.91,
                "options_applied": {"preserve_numbers": True},
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class PluginInfo(BaseModel):
    """Information about a registered plugin"""
    name: str = Field(..., description="Plugin name")
    plugin_type: PluginType = Field(..., description="Plugin type/category")
    contract: ContractType = Field(..., description="Contract enforced by metaclass")
    version: Optional[str] = Field(None, description="Plugin version")
    description: Optional[str] = Field(None, description="Plugin description")
    status: PluginStatus = Field(..., description="Plugin status")
    class_name: str = Field(..., description="Plugin class name")
    module_path: Optional[str] = Field(None, description="Module path")
    capabilities: Optional[List[str]] = Field(
        None,
        description="Plugin capabilities"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional plugin metadata"
    )
    registration_time: Optional[datetime] = Field(
        None,
        description="When the plugin was registered"
    )


class PluginRegistryResponse(BaseModel):
    """Response from plugin registry endpoint"""
    ok: bool = Field(True, description="Request success status")
    plugins: Dict[str, Dict[str, PluginInfo]] = Field(
        ...,
        description="Plugins organized by category"
    )
    total_plugins: int = Field(
        ...,
        description="Total number of registered plugins",
        ge=0
    )
    categories: List[str] = Field(
        ...,
        description="Available plugin categories"
    )
    timestamp: datetime = Field(..., description="Registry snapshot timestamp")


class PerformanceMetric(BaseModel):
    """Performance metric for a method"""
    method_name: str = Field(..., description="Method name")
    call_count: int = Field(..., description="Number of calls", ge=0)
    total_time: float = Field(..., description="Total execution time", ge=0)
    avg_time: float = Field(..., description="Average execution time", ge=0)
    min_time: float = Field(..., description="Minimum execution time", ge=0)
    max_time: float = Field(..., description="Maximum execution time", ge=0)
    last_called: Optional[datetime] = Field(
        None,
        description="Last call timestamp"
    )


class PerformanceResponse(BaseModel):
    """Response from performance metrics endpoint"""
    ok: bool = Field(True, description="Request success status")
    metrics: Dict[str, PerformanceMetric] = Field(
        ...,
        description="Performance metrics by method"
    )
    total_methods: int = Field(
        ...,
        description="Total number of tracked methods",
        ge=0
    )
    total_calls: int = Field(
        ...,
        description="Total number of method calls",
        ge=0
    )
    monitoring_duration_seconds: Optional[float] = Field(
        None,
        description="Duration of monitoring in seconds",
        ge=0
    )
    timestamp: datetime = Field(..., description="Metrics snapshot timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ok": True,
                "metrics": {
                    "JSONProcessor.process": {
                        "total_calls": 15,
                        "average_time_ms": 23.4,
                        "min_time_ms": 12.1,
                        "max_time_ms": 45.6
                    }
                },
                "total_methods": 5,
                "total_calls": 42,
                "monitoring_duration_seconds": 3600.0,
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class ContractValidationResult(BaseModel):
    """Result of contract validation"""
    is_compliant: bool = Field(..., description="Whether plugin is contract compliant")
    contract_type: ContractType = Field(..., description="Contract being validated")
    plugin_class: str = Field(..., description="Plugin class name")
    missing_methods: List[str] = Field(
        default_factory=list,
        description="Required methods that are missing"
    )
    invalid_signatures: List[str] = Field(
        default_factory=list,
        description="Methods with invalid signatures"
    )
    violations: List[str] = Field(
        default_factory=list,
        description="Contract violations found"
    )
    validation_time_ms: float = Field(
        ...,
        description="Validation time in milliseconds",
        ge=0
    )
    timestamp: datetime = Field(..., description="Validation timestamp")


class PluginInstanceResponse(BaseModel):
    """Response from plugin instantiation"""
    success: bool = Field(..., description="Instantiation success status")
    plugin_name: str = Field(..., description="Plugin name")
    plugin_type: PluginType = Field(..., description="Plugin type")
    instance_id: Optional[str] = Field(
        None,
        description="Unique instance identifier"
    )
    instantiation_time_ms: float = Field(
        ...,
        description="Instantiation time in milliseconds",
        ge=0
    )
    contract_validation: Optional[ContractValidationResult] = Field(
        None,
        description="Contract validation result"
    )
    error_message: Optional[str] = Field(
        None,
        description="Error message if instantiation failed"
    )
    timestamp: datetime = Field(..., description="Instantiation timestamp")


class SystemHealthResponse(BaseModel):
    """System health response"""
    ok: bool = Field(True, description="System health status")
    status: str = Field(..., description="Overall system status")
    plugin_registry_health: bool = Field(
        ...,
        description="Plugin registry health status"
    )
    metaclass_system_health: bool = Field(
        ...,
        description="Metaclass system health status"
    )
    performance_monitoring_health: bool = Field(
        ...,
        description="Performance monitoring health status"
    )
    total_registered_plugins: int = Field(
        ...,
        description="Total registered plugins",
        ge=0
    )
    active_plugin_instances: int = Field(
        ...,
        description="Active plugin instances",
        ge=0
    )
    uptime_seconds: float = Field(
        ...,
        description="System uptime in seconds",
        ge=0
    )
    memory_usage_mb: Optional[float] = Field(
        None,
        description="Current memory usage in MB",
        ge=0
    )
    timestamp: datetime = Field(..., description="Health check timestamp")


class ErrorResponse(BaseModel):
    """Error response model"""
    ok: bool = Field(False, description="Request success status")
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    error_type: Optional[str] = Field(None, description="Error type")
    plugin_name: Optional[str] = Field(
        None,
        description="Plugin name if error is plugin-specific"
    )
    contract_violations: Optional[List[str]] = Field(
        None,
        description="Contract violations if applicable"
    )
    stack_trace: Optional[str] = Field(
        None,
        description="Stack trace (debug mode only)"
    )
    timestamp: datetime = Field(..., description="Error timestamp")
    
    class Config:
        schema_extra = {
            "example": {
                "ok": False,
                "error": "Plugin contract violation: missing required method 'process'",
                "error_code": "CONTRACT_VIOLATION",
                "error_type": "METACLASS_ERROR",
                "plugin_name": "CustomProcessor",
                "contract_violations": ["Missing method: process"],
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }


class BatchRequest(BaseModel):
    """Batch processing request"""
    operations: List[Dict[str, Any]] = Field(
        ...,
        description="List of operations to perform",
        min_items=1,
        max_items=100
    )
    parallel: Optional[bool] = Field(
        False,
        description="Whether to execute operations in parallel"
    )
    fail_fast: Optional[bool] = Field(
        True,
        description="Whether to stop on first failure"
    )
    timeout_seconds: Optional[float] = Field(
        30.0,
        description="Timeout for batch operation",
        ge=1.0,
        le=300.0
    )
    
    @field_validator('operations')
    @classmethod
    def validate_operations(cls, v):
        """Validate batch operations"""
        if not v:
            raise ValueError("At least one operation is required")
        
        for i, op in enumerate(v):
            if not isinstance(op, dict):
                raise ValueError(f"Operation {i} must be a dictionary")
            if 'type' not in op:
                raise ValueError(f"Operation {i} must have a 'type' field")
        
        return v


class BatchResponse(BaseModel):
    """Batch processing response"""
    success: bool = Field(..., description="Overall batch success status")
    results: List[Dict[str, Any]] = Field(
        ...,
        description="Results for each operation"
    )
    total_operations: int = Field(
        ...,
        description="Total number of operations",
        ge=0
    )
    successful_operations: int = Field(
        ...,
        description="Number of successful operations",
        ge=0
    )
    failed_operations: int = Field(
        ...,
        description="Number of failed operations",
        ge=0
    )
    total_time_ms: float = Field(
        ...,
        description="Total batch processing time in milliseconds",
        ge=0
    )
    parallel_execution: bool = Field(
        ...,
        description="Whether operations were executed in parallel"
    )
    timestamp: datetime = Field(..., description="Batch execution timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "results": [
                    {"operation": 1, "status": "success", "result": "processed"},
                    {"operation": 2, "status": "success", "result": "validated"}
                ],
                "total_operations": 2,
                "successful_operations": 2,
                "failed_operations": 0,
                "total_time_ms": 156.78,
                "parallel_execution": True,
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class StatusResponse(BaseModel):
    """Standard status response"""
    ok: bool = Field(True, description="Request success status")
    message: str = Field(..., description="Status message")
    timestamp: datetime = Field(..., description="Response timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ok": True,
                "message": "System operating normally",
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class ErrorResponse(BaseModel):
    """Standard error response"""
    ok: bool = Field(False, description="Request success status")
    error: str = Field(..., description="Error message")
    error_type: Optional[str] = Field(None, description="Error type/category")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(..., description="Error timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ok": False,
                "error": "Plugin validation failed",
                "error_type": "ContractViolationError",
                "details": {"missing_methods": ["process", "validate_input"]},
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )


class SystemHealthResponse(BaseModel):
    """System health check response"""
    healthy: bool = Field(..., description="Overall system health status")
    plugin_registry_status: str = Field(..., description="Plugin registry health")
    active_plugins: int = Field(..., description="Number of active plugins", ge=0)
    total_plugins: int = Field(..., description="Total registered plugins", ge=0)
    contract_violations: int = Field(..., description="Current contract violations", ge=0)
    performance_metrics: Dict[str, Any] = Field(..., description="System performance metrics")
    uptime_seconds: float = Field(..., description="System uptime in seconds", ge=0)
    timestamp: datetime = Field(..., description="Health check timestamp")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "healthy": True,
                "plugin_registry_status": "operational",
                "active_plugins": 8,
                "total_plugins": 10,
                "contract_violations": 0,
                "performance_metrics": {
                    "average_response_time_ms": 45.2,
                    "total_requests": 1234
                },
                "uptime_seconds": 86400.0,
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }
    )
