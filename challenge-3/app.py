"""FastAPI app exposing metaclass-enforced plugin registry (processors/validators/transformers)."""

import asyncio
import json
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, Query, Body, Depends
from fastapi.responses import JSONResponse

from utils import (
    get_registered_plugins, 
    create_plugin_instance,
    validate_contract_compliance,
    get_performance_summary,
    clear_all_metrics,
    process_data_with_plugin,
    validate_data_with_plugin,
    transform_data_with_plugin,
    get_system_health,
    DATA_PROCESSOR_CONTRACT,
    VALIDATOR_CONTRACT,
    TRANSFORMER_CONTRACT,
    ContractViolationError
)

from models import (
    ProcessRequest, ProcessResponse, ValidationRequest, ValidationResponse,
    TransformRequest, TransformResponse, PluginRegistryResponse, PluginInfo,
    PerformanceResponse, SystemHealthResponse, ErrorResponse, StatusResponse,
    BatchRequest, BatchResponse, ContractValidationResult
)

# Import example plugins to trigger registration
try:
    from plugins.processors import JSONProcessor, CSVProcessor, XMLProcessor
    from plugins.validators import SchemaValidator, RangeValidator, FormatValidator
    from plugins.transformers import UppercaseTransformer, DateTransformer, NumberTransformer
except ImportError:
    # Plugins not available yet - will be created later
    pass

app = FastAPI(
    title="Meta-Programming Plugin System",
    description="Advanced meta-programming system with metaclass-enforced API contracts",
    version="1.0.0"
)


@app.get("/", response_model=StatusResponse)
async def root():
    """Basic service banner."""
    return StatusResponse(
        ok=True,
        message="Meta-Programming Plugin System operational - Features: metaclass contracts, automatic registration, runtime validation",
        timestamp=datetime.now()
    )


@app.get("/health", response_model=SystemHealthResponse)
async def health_check():
    """Return plugin + metrics health summary."""
    health_data = get_system_health()

    return SystemHealthResponse(
        healthy=health_data["healthy"],
        plugin_registry_status=health_data["plugin_registry_status"],
        active_plugins=health_data["active_plugins"],
        total_plugins=health_data["total_plugins"],
        contract_violations=health_data["contract_violations"],
        performance_metrics=health_data["performance_metrics"],
        uptime_seconds=time.time(),  # Simple uptime since import
        timestamp=datetime.now()
    )


@app.get("/plugins")
async def list_plugins(
    category: Optional[str] = Query(None, description="Filter by plugin category")
):
    """List registered plugins (optionally filtered)."""
    try:
        plugins = get_registered_plugins(category)
        
        # Simplify the response to match the pattern from other challenges
        result = {}
        total_plugins = 0
        
        for cat, plugin_dict in plugins.items():
            result[cat] = []
            for name, info in plugin_dict.items():
                plugin_class = info['class']
                plugin_info = {
                    "name": name,
                    "type": cat[:-1],  # Remove 's' from category
                    "contract": info['contract'].name,
                    "version": getattr(plugin_class, 'version', '1.0.0'),
                    "description": (plugin_class.__doc__ or "No description").split('\n')[0],
                    "class_name": plugin_class.__name__,
                    "capabilities": len(info['contract'].required_methods),
                    "registered_at": info['registered_at']
                }
                result[cat].append(plugin_info)
                total_plugins += 1
        
        return {
            "ok": True,
            "plugins": result,
            "total_plugins": total_plugins,
            "categories": list(result.keys()),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list plugins: {str(e)}"
        )


@app.get("/contracts")
async def get_contracts():
    """Return contract definitions (required methods + attrs)."""
    def contract_to_dict(contract):
        return {
            "name": contract.name,
            "required_methods": [
                {
                    "name": method.name,
                    "required_params": method.required_params,
                    "param_types": {k: str(v) for k, v in method.param_types.items()},
                    "return_type": str(method.return_type),
                    "description": method.description
                }
                for method in contract.required_methods
            ],
            "class_attributes": contract.class_attributes,
            "inheritance_requirements": [str(req) for req in contract.inheritance_requirements]
        }
    
    return {
        "available_contracts": {
            "DataProcessor": contract_to_dict(DATA_PROCESSOR_CONTRACT),
            "Validator": contract_to_dict(VALIDATOR_CONTRACT),
            "Transformer": contract_to_dict(TRANSFORMER_CONTRACT)
        },
        "total_contracts": 3,
        "timestamp": datetime.now().isoformat()
    }


# Exception handlers for proper error responses
@app.exception_handler(ContractViolationError)
async def contract_violation_handler(request: Request, exc: ContractViolationError):
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error=f"Contract violation: {str(exc)}",
            error_type="ContractViolationError",
            timestamp=datetime.now()
        ).model_dump()
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


@app.post("/process")
async def process_data(
    processor_type: str = Body(...),
    data: Dict[str, Any] = Body(...),
    validate_input: bool = Body(True)
):
    """Process payload via named processor plugin (optional input validation)."""
    start_time = datetime.now()
    
    try:
        # Use utility function to handle the processing with proper error handling
        result = await process_data_with_plugin(
            processor_type=processor_type,
            data=data,
            options={},
            validate_input=validate_input
        )
        
        processing_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return {
            "success": True,
            "result": result["processed_data"],
            "processor_type": processor_type,
            "processing_time_ms": processing_time_ms,
            "input_validation": result.get("input_validation"),
            "metadata": result.get("metadata", {}),
            "timestamp": datetime.now().isoformat()
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Processor not found: {processor_type}"
        )
    except ContractViolationError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Contract violation in {processor_type}: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Processing failed: {str(e)}"
        )


@app.post("/validate", response_model=ValidationResponse)
async def validate_data(
    request: ValidationRequest
) -> ValidationResponse:
    """Validate payload using validator plugin (rules + strict mode)."""
    start_time = datetime.now()
    
    try:
        # Use utility function to handle validation with proper error handling
        result = await validate_data_with_plugin(
            validator_type=request.validator_type,
            data=request.data,
            rules=request.rules,
            strict=request.strict
        )
        
        validation_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return ValidationResponse(
            is_valid=result["is_valid"],
            errors=result["errors"],
            validator_type=request.validator_type,
            validation_time_ms=validation_time_ms,
            rules_applied=request.rules,
            details=result.get("details", {}),
            timestamp=datetime.now()
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Validator not found: {request.validator_type}"
        )
    except ContractViolationError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Contract violation in {request.validator_type}: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Validation failed: {str(e)}"
        )


@app.post("/transform", response_model=TransformResponse)
async def transform_data(
    request: TransformRequest
) -> TransformResponse:
    """Transform payload with transformer plugin (supports reverse)."""
    start_time = datetime.now()
    
    try:
        # Use utility function to handle transformation with proper error handling
        result = await transform_data_with_plugin(
            transformer_type=request.transformer_type,
            data=request.data,
            options=request.options,
            reverse=request.reverse
        )
        
        transformation_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return TransformResponse(
            success=True,
            result=result["transformed_data"],
            transformer_type=request.transformer_type,
            transformation_time_ms=transformation_time_ms,
            options_applied=request.options,
            metadata=result.get("metadata", {}),
            timestamp=datetime.now()
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Transformer not found: {request.transformer_type}"
        )
    except ContractViolationError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Contract violation in {request.transformer_type}: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Transformation failed: {str(e)}"
        )


@app.get("/metrics", response_model=PerformanceResponse)
async def get_metrics() -> PerformanceResponse:
    """Return aggregated method performance metrics."""
    metrics_summary = get_performance_summary()

    # Convert to the expected format
    formatted_metrics = {}
    total_calls = 0
    
    for method_key, stats in metrics_summary.items():
    # Convert stats to PerformanceMetric format
        from models import PerformanceMetric
        
        formatted_metrics[method_key] = PerformanceMetric(
            method_name=method_key,
            call_count=stats['call_count'],
            total_time=stats['total_time'],
            avg_time=stats['avg_time'],
            min_time=stats['min_time'],
            max_time=stats['max_time'],
            last_called=datetime.now()  # We don't track this currently
        )
        total_calls += stats['call_count']
    
    return PerformanceResponse(
        ok=True,
        metrics=formatted_metrics,
        total_methods=len(formatted_metrics),
        total_calls=total_calls,
        monitoring_duration_seconds=time.time(),  # Simple approximation
        timestamp=datetime.now()
    )


@app.delete("/metrics", response_model=StatusResponse)
async def clear_metrics() -> StatusResponse:
    """Reset in-memory performance counters."""
    clear_all_metrics()
    return StatusResponse(
        ok=True,
        message="All performance metrics cleared",
        timestamp=datetime.now()
    )


# NOTE: Duplicate legacy /validate endpoint removed; single typed version retained.


# Removed duplicate legacy /transform endpoint (typed version retained above).


# NOTE: Duplicate raw /metrics endpoint removed (structured version retained).


# Duplicate untyped /metrics DELETE removed.


# Duplicate contracts endpoint removed (canonical version earlier retained).


@app.post("/validate-compliance")
async def validate_plugin_compliance(
    plugin_class_name: str = Body(..., description="Name of the plugin class to validate"),
    contract_name: str = Body(..., description="Name of the contract to validate against")
):
    """Validate that a plugin class complies with a specific contract"""
    
    # Map contract names to actual contracts
    contracts = {
        "DataProcessor": DATA_PROCESSOR_CONTRACT,
        "Validator": VALIDATOR_CONTRACT,
        "Transformer": TRANSFORMER_CONTRACT
    }
    
    if contract_name not in contracts:
        raise HTTPException(status_code=400, detail=f"Unknown contract: {contract_name}")
    
    # Find the plugin class in the registry
    plugin_class = None
    for category, plugins in get_registered_plugins().items():
        for name, info in plugins.items():
            if name == plugin_class_name:
                plugin_class = info['class']
                break
        if plugin_class:
            break
    
    if not plugin_class:
        raise HTTPException(status_code=404, detail=f"Plugin class not found: {plugin_class_name}")
    
    # Validate compliance
    contract = contracts[contract_name]
    violations = validate_contract_compliance(plugin_class, contract)
    
    return {
        "plugin_class": plugin_class_name,
        "contract": contract_name,
        "compliant": len(violations) == 0,
        "violations": violations,
        "validation_timestamp": time.time()
    }


# Duplicate /health endpoint removed (structured one retained earlier).


# Exception handlers
@app.exception_handler(ContractViolationError)
async def contract_violation_handler(request: Request, exc: ContractViolationError):
    return {
        "error": "Contract Violation",
        "detail": str(exc),
        "type": "contract_violation"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
