"""
Challenge 3 - Advanced Meta-Programming FastAPI Application

RESTful API demonstrating metaclass-enforced plugin system with automatic
registration and contract validation.
"""

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
    """Root endpoint with system information"""
    return StatusResponse(
        ok=True,
        message="Meta-Programming Plugin System operational - Features: metaclass contracts, automatic registration, runtime validation",
        timestamp=datetime.now()
    )


@app.get("/health", response_model=SystemHealthResponse)
async def health_check():
    """System health check endpoint"""
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
    """List all registered plugins, optionally filtered by category"""
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
    """Get all contract definitions"""
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
    """
    Process data using a registered processor plugin.
    Demonstrates metaclass contract enforcement and automatic validation.
    """
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
    request: ValidationRequest = Depends()
) -> ValidationResponse:
    """
    Validate data using a registered validator plugin.
    Demonstrates metaclass contract enforcement for validators.
    """
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
    request: TransformRequest = Depends()
) -> TransformResponse:
    """
    Transform data using a registered transformer plugin.
    Demonstrates metaclass contract enforcement for transformers.
    """
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
    """Get performance metrics for all plugin operations"""
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
    """Clear all performance metrics"""
    clear_all_metrics()
    return StatusResponse(
        ok=True,
        message="All performance metrics cleared",
        timestamp=datetime.now()
    )


@app.post("/validate")
async def validate_data(request: ValidationRequest):
    """Validate data using a registered validator plugin"""
    try:
        # Create validator instance
        validator = create_plugin_instance('validators', request.validator_type)
        
        # Perform validation
        start_time = time.time()
        is_valid = validator.validate(request.data)
        validation_time = time.time() - start_time
        
        # Get errors if validation failed
        errors = validator.get_errors() if not is_valid else []
        
        return {
            "valid": is_valid,
            "validator": request.validator_type,
            "validation_time": validation_time,
            "errors": errors,
            "metadata": {
                "validator_type": getattr(validator, 'validator_type', 'unknown'),
                "rules_applied": request.rules
            }
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ContractViolationError as e:
        raise HTTPException(status_code=500, detail=f"Contract violation: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@app.post("/transform")
async def transform_data(request: TransformRequest):
    """Transform data using a registered transformer plugin"""
    try:
        # Create transformer instance
        transformer = create_plugin_instance('transformers', request.transformer_type)
        
        # Perform transformation
        start_time = time.time()
        if request.reverse:
            result = transformer.reverse_transform(request.data)
            operation = "reverse_transform"
        else:
            result = transformer.transform(request.data)
            operation = "transform"
        transformation_time = time.time() - start_time
        
        return {
            "success": True,
            "transformer": request.transformer_type,
            "operation": operation,
            "transformation_time": transformation_time,
            "result": result,
            "metadata": {
                "transformer_type": getattr(transformer, 'transformer_type', 'unknown'),
                "reversible": getattr(transformer, 'reversible', False)
            }
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ContractViolationError as e:
        raise HTTPException(status_code=500, detail=f"Contract violation: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transformation error: {str(e)}")


@app.get("/metrics")
async def get_metrics():
    """Get performance metrics for all plugin operations"""
    metrics = get_performance_summary()
    
    # Organize by plugin and method
    organized_metrics = {}
    for method_key, stats in metrics.items():
        if '.' in method_key:
            class_name, method_name = method_key.split('.', 1)
            if class_name not in organized_metrics:
                organized_metrics[class_name] = {}
            organized_metrics[class_name][method_name] = stats
    
    return {
        "total_methods_tracked": len(metrics),
        "total_calls": sum(stats['call_count'] for stats in metrics.values()),
        "total_time": sum(stats['total_time'] for stats in metrics.values()),
        "metrics_by_plugin": organized_metrics,
        "raw_metrics": metrics
    }


@app.delete("/metrics")
async def clear_metrics():
    """Clear all performance metrics"""
    clear_all_metrics()
    return {"message": "All performance metrics cleared"}


@app.get("/contracts")
async def get_contracts():
    """Get all contract definitions"""
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
            "optional_methods": [
                {
                    "name": method.name,
                    "required_params": method.required_params,
                    "param_types": {k: str(v) for k, v in method.param_types.items()},
                    "return_type": str(method.return_type),
                    "description": method.description
                }
                for method in contract.optional_methods
            ],
            "class_attributes": contract.class_attributes
        }
    
    return {
        "contracts": {
            "DataProcessor": contract_to_dict(DATA_PROCESSOR_CONTRACT),
            "Validator": contract_to_dict(VALIDATOR_CONTRACT),
            "Transformer": contract_to_dict(TRANSFORMER_CONTRACT)
        }
    }


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


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    plugins = get_registered_plugins()
    total_plugins = sum(len(category_plugins) for category_plugins in plugins.values())
    
    return {
        "status": "healthy",
        "system": "Meta-Programming Plugin System",
        "total_plugins": total_plugins,
        "plugin_categories": list(plugins.keys()),
        "contracts_available": ["DataProcessor", "Validator", "Transformer"],
        "timestamp": time.time()
    }


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
