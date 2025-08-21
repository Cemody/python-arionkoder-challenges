"""Metaclass + contract system for plugin registration & enforcement."""

import inspect
import time
import functools
import logging
from typing import Any, Dict, List, Type, Callable, Optional, get_type_hints
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global plugin registry
PLUGIN_REGISTRY: Dict[str, Dict[str, Any]] = {
    'processors': {},
    'validators': {},
    'transformers': {},
    'exporters': {}
}

# Performance metrics storage
PERFORMANCE_METRICS: Dict[str, List[float]] = {}


class ContractViolationError(Exception):
    """Raised when a class violates its contract."""
    pass


class MethodSignatureError(Exception):
    """Raised when method signature mismatches contract."""
    pass


@dataclass
class MethodContract:
    """Method requirement descriptor."""
    name: str
    required_params: List[str]
    param_types: Dict[str, type]
    return_type: type
    description: str
    validation_rules: List[Callable] = None

    def __post_init__(self):
        if self.validation_rules is None:
            self.validation_rules = []


@dataclass
class ClassContract:
    """Composite class contract (methods, attrs, inheritance)."""
    name: str
    required_methods: List[MethodContract]
    optional_methods: List[MethodContract] = None
    class_attributes: List[str] = None
    inheritance_requirements: List[Type] = None

    def __post_init__(self):
        if self.optional_methods is None:
            self.optional_methods = []
        if self.class_attributes is None:
            self.class_attributes = []
        if self.inheritance_requirements is None:
            self.inheritance_requirements = []


# Define standard contracts
DATA_PROCESSOR_CONTRACT = ClassContract(
    name="DataProcessor",
    required_methods=[
        MethodContract(
            name="process",
            required_params=["self", "data"],
            param_types={"data": Dict[str, Any]},
            return_type=Dict[str, Any],
            description="Process input data and return transformed result"
        ),
        MethodContract(
            name="validate_input",
            required_params=["self", "data"],
            param_types={"data": Dict[str, Any]},
            return_type=bool,
            description="Validate input data format and content"
        ),
        MethodContract(
            name="get_schema",
            required_params=["self"],
            param_types={},
            return_type=Dict[str, Any],
            description="Return schema definition for expected input/output"
        )
    ],
    class_attributes=["processor_type", "version"]
)

VALIDATOR_CONTRACT = ClassContract(
    name="Validator",
    required_methods=[
        MethodContract(
            name="validate",
            required_params=["self", "data"],
            param_types={"data": Any},
            return_type=bool,
            description="Validate data according to specific rules"
        ),
        MethodContract(
            name="get_errors",
            required_params=["self"],
            param_types={},
            return_type=List[str],
            description="Return list of validation errors from last validation"
        )
    ],
    class_attributes=["validator_type"]
)

TRANSFORMER_CONTRACT = ClassContract(
    name="Transformer",
    required_methods=[
        MethodContract(
            name="transform",
            required_params=["self", "data"],
            param_types={"data": Any},
            return_type=Any,
            description="Transform input data to output format"
        ),
        MethodContract(
            name="reverse_transform",
            required_params=["self", "data"],
            param_types={"data": Any},
            return_type=Any,
            description="Reverse the transformation if possible"
        )
    ],
    class_attributes=["transformer_type", "reversible"]
)


class ContractEnforcerMeta(type):
    """Metaclass enforcing declared ClassContract and instrumenting methods."""
    
    def __new__(cls, name, bases, namespace, contract: ClassContract = None, **kwargs):
        """Create a new class with contract enforcement"""
        
        # Skip contract enforcement for abstract base classes
        if name.endswith('Base') or namespace.get('__abstract__', False):
            return super().__new__(cls, name, bases, namespace)
        
        # If no contract is provided, inherit from base class
        if not contract:
            for base in bases:
                if hasattr(base, '_contract'):
                    contract = base._contract
                    break
        
        if contract:
            cls._validate_contract_compliance(name, bases, namespace, contract)
            cls._enhance_methods(namespace, contract)
            cls._add_monitoring(namespace)
        
        # Create the class
        new_class = super().__new__(cls, name, bases, namespace)
        
        # Store the contract on the class for inheritance
        if contract:
            new_class._contract = contract
            cls._register_plugin(new_class, contract)
        
        return new_class
    
    @staticmethod
    def _validate_contract_compliance(name: str, bases: tuple, namespace: dict, contract: ClassContract):
        """Validate that the class complies with its contract"""
        
        # Check required class attributes
        for attr in contract.class_attributes:
            if attr not in namespace and not any(hasattr(base, attr) for base in bases):
                raise ContractViolationError(
                    f"Class {name} missing required attribute: {attr}"
                )
        
        # Check required methods
        for method_contract in contract.required_methods:
            method_name = method_contract.name
            
            # Check if method exists
            if method_name not in namespace and not any(hasattr(base, method_name) for base in bases):
                raise ContractViolationError(
                    f"Class {name} missing required method: {method_name}"
                )
            
            # Validate method signature if method is defined in this class
            if method_name in namespace:
                method = namespace[method_name]
                if callable(method):
                    ContractEnforcerMeta._validate_method_signature(method, method_contract, name)
        
        # Check inheritance requirements
        if contract.inheritance_requirements:
            for required_base in contract.inheritance_requirements:
                if not any(issubclass(base, required_base) for base in bases):
                    raise ContractViolationError(
                        f"Class {name} must inherit from {required_base.__name__}"
                    )
    
    @staticmethod
    def _validate_method_signature(method: Callable, contract: MethodContract, class_name: str):
        """Validate that a method signature matches its contract"""
        try:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            
            # Check required parameters
            for required_param in contract.required_params:
                if required_param not in params:
                    raise MethodSignatureError(
                        f"Method {class_name}.{contract.name} missing required parameter: {required_param}"
                    )
            
            # Validate parameter types (basic validation)
            for param_name, expected_type in contract.param_types.items():
                if param_name in sig.parameters:
                    param = sig.parameters[param_name]
                    if param.annotation != inspect.Parameter.empty and param.annotation != expected_type:
                        logger.warning(
                            f"Parameter {param_name} in {class_name}.{contract.name} "
                            f"has type annotation {param.annotation}, expected {expected_type}"
                        )
            
        except Exception as e:
            logger.warning(f"Could not validate signature for {class_name}.{contract.name}: {e}")
    
    @staticmethod
    def _enhance_methods(namespace: dict, contract: ClassContract):
        """Add automatic enhancements to methods (logging, monitoring, validation)"""
        
        for method_contract in contract.required_methods:
            method_name = method_contract.name
            if method_name in namespace and callable(namespace[method_name]):
                original_method = namespace[method_name]
                enhanced_method = ContractEnforcerMeta._create_enhanced_method(
                    original_method, method_contract
                )
                namespace[method_name] = enhanced_method
    
    @staticmethod
    def _create_enhanced_method(original_method: Callable, contract: MethodContract) -> Callable:
        """Create an enhanced version of a method with monitoring and validation"""
        
        @functools.wraps(original_method)
        def enhanced_method(self, *args, **kwargs):
            class_name = self.__class__.__name__
            method_key = f"{class_name}.{contract.name}"
            
            # Pre-execution logging
            logger.debug(f"Calling {method_key} with args={args}, kwargs={kwargs}")
            
            # Start timing
            start_time = time.time()
            
            try:
                # Input validation (if validation rules exist)
                for rule in contract.validation_rules:
                    if not rule(self, *args, **kwargs):
                        raise ContractViolationError(f"Input validation failed for {method_key}")
                
                # Call original method
                result = original_method(self, *args, **kwargs)
                
                # Validate return type (basic check)
                if hasattr(contract, 'return_type') and contract.return_type != Any:
                    try:
                        # Handle generic types that can't be used with isinstance
                        if hasattr(contract.return_type, '__origin__'):
                            # For generic types like Dict[str, Any], just check the origin type
                            origin_type = contract.return_type.__origin__
                            if not isinstance(result, origin_type):
                                logger.warning(
                                    f"Method {method_key} returned {type(result)}, "
                                    f"expected {origin_type} (origin of {contract.return_type})"
                                )
                        else:
                            # For regular types
                            if not isinstance(result, contract.return_type):
                                logger.warning(
                                    f"Method {method_key} returned {type(result)}, "
                                    f"expected {contract.return_type}"
                                )
                    except TypeError:
                        # Skip type checking for complex generic types
                        logger.debug(f"Skipping return type validation for {method_key} due to complex generic type")
                        pass
                
                # Record successful execution
                execution_time = time.time() - start_time
                ContractEnforcerMeta._record_performance(method_key, execution_time)
                
                logger.debug(f"Completed {method_key} in {execution_time:.4f}s")
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Error in {method_key} after {execution_time:.4f}s: {e}")
                raise
        
        return enhanced_method
    
    @staticmethod
    def _add_monitoring(namespace: dict):
        """Add monitoring capabilities to the class"""
        
        def get_performance_stats(self):
            """Return performance stats (per method)."""
            class_name = self.__class__.__name__
            stats = {}
            for method_key, times in PERFORMANCE_METRICS.items():
                if method_key.startswith(class_name + '.'):
                    method_name = method_key.split('.', 1)[1]
                    if times:
                        stats[method_name] = {
                            'call_count': len(times),
                            'avg_time': sum(times) / len(times),
                            'min_time': min(times),
                            'max_time': max(times),
                            'total_time': sum(times)
                        }
            return stats
        
        def reset_performance_stats(self):
            """Clear stored timings for this class."""
            class_name = self.__class__.__name__
            keys_to_clear = [k for k in PERFORMANCE_METRICS.keys() if k.startswith(class_name + '.')]
            for key in keys_to_clear:
                PERFORMANCE_METRICS[key] = []
        
        namespace['get_performance_stats'] = get_performance_stats
        namespace['reset_performance_stats'] = reset_performance_stats
    
    @staticmethod
    def _record_performance(method_key: str, execution_time: float):
        """Record performance metrics for a method call"""
        if method_key not in PERFORMANCE_METRICS:
            PERFORMANCE_METRICS[method_key] = []
        PERFORMANCE_METRICS[method_key].append(execution_time)
    
    @staticmethod
    @staticmethod
    def _register_plugin(plugin_class: Type, contract: ClassContract):
        """Register a plugin in the global registry"""
        plugin_name = plugin_class.__name__
        
        # Determine plugin category based on contract
        category = 'processors'  # default
        if 'Validator' in contract.name:
            category = 'validators'
        elif 'Transformer' in contract.name:
            category = 'transformers'
        elif 'Exporter' in contract.name:
            category = 'exporters'
        
        PLUGIN_REGISTRY[category][plugin_name] = {
            'class': plugin_class,
            'contract': contract,
            'registered_at': time.time()
        }
        
        logger.info(f"Registered {category[:-1]} plugin: {plugin_name}")


# Convenience base classes that automatically apply contracts
class DataProcessorBase(metaclass=ContractEnforcerMeta, contract=DATA_PROCESSOR_CONTRACT):
    """Base class for data processors with automatic contract enforcement"""
    __abstract__ = True
    _contract = DATA_PROCESSOR_CONTRACT


class ValidatorBase(metaclass=ContractEnforcerMeta, contract=VALIDATOR_CONTRACT):
    """Base class for validators with automatic contract enforcement"""
    __abstract__ = True
    _contract = VALIDATOR_CONTRACT


class TransformerBase(metaclass=ContractEnforcerMeta, contract=TRANSFORMER_CONTRACT):
    """Base class for transformers with automatic contract enforcement"""
    __abstract__ = True
    _contract = TRANSFORMER_CONTRACT
    __abstract__ = True


# Utility functions for working with the plugin system
def get_registered_plugins(category: str = None) -> Dict[str, Any]:
    """Return plugin registry (optionally a single category)."""
    if category:
        return PLUGIN_REGISTRY.get(category, {})
    return PLUGIN_REGISTRY


def create_plugin_instance(category: str, plugin_name: str, *args, **kwargs):
    """Instantiate named plugin class."""
    if category not in PLUGIN_REGISTRY:
        raise ValueError(f"Unknown plugin category: {category}")
    
    if plugin_name not in PLUGIN_REGISTRY[category]:
        raise ValueError(f"Unknown plugin: {plugin_name} in category {category}")
    
    plugin_class = PLUGIN_REGISTRY[category][plugin_name]['class']
    return plugin_class(*args, **kwargs)


def validate_contract_compliance(plugin_class: Type, contract: ClassContract) -> List[str]:
    """Return list of contract violation messages (empty if compliant)."""
    violations = []
    
    # Check required attributes
    for attr in contract.class_attributes:
        if not hasattr(plugin_class, attr):
            violations.append(f"Missing required attribute: {attr}")
    
    # Check required methods
    for method_contract in contract.required_methods:
        if not hasattr(plugin_class, method_contract.name):
            violations.append(f"Missing required method: {method_contract.name}")
        else:
            method = getattr(plugin_class, method_contract.name)
            if not callable(method):
                violations.append(f"Attribute {method_contract.name} is not callable")
    
    return violations


def get_performance_summary() -> Dict[str, Any]:
    """Build aggregated timing stats for all tracked methods."""
    summary = {}
    for method_key, times in PERFORMANCE_METRICS.items():
        if times:
            summary[method_key] = {
                'call_count': len(times),
                'avg_time': sum(times) / len(times),
                'min_time': min(times),
                'max_time': max(times),
                'total_time': sum(times)
            }
    return summary


def clear_all_metrics():
    """Reset global performance metrics store."""
    global PERFORMANCE_METRICS
    PERFORMANCE_METRICS.clear()


async def process_data_with_plugin(processor_type: str, data: Dict[str, Any], 
                                 options: Dict[str, Any] = None, 
                                 validate_input: bool = True) -> Dict[str, Any]:
    """Process data via processor plugin; optional input validation."""
    # Create processor instance
    processor = create_plugin_instance('processors', processor_type)
    
    # Validate input if requested
    input_validation = None
    if validate_input:
        input_validation = processor.validate_input(data)
        if not input_validation:
            raise ValueError("Input validation failed")
    
    # Apply options if provided
    if options and hasattr(processor, 'configure'):
        processor.configure(options)
    
    # Process data
    processed_data = processor.process(data)
    
    # Get schema information
    schema = processor.get_schema()
    
    return {
        "processed_data": processed_data,
        "input_validation": input_validation,
        "metadata": {
            "processor_version": getattr(processor, 'version', 'unknown'),
            "processor_type": getattr(processor, 'processor_type', 'unknown'),
            "input_schema": schema.get("input", {}),
            "output_schema": schema.get("output", {}),
            "options_applied": options or {}
        }
    }


async def validate_data_with_plugin(validator_type: str, data: Any,
                                  rules: Dict[str, Any] = None,
                                  strict: bool = True) -> Dict[str, Any]:
    """Validate data via validator plugin (rules + strict)."""
    # Create validator instance
    validator = create_plugin_instance('validators', validator_type)
    
    # Apply rules if provided
    if rules and hasattr(validator, 'set_rules'):
        validator.set_rules(rules)
    
    # Set strict mode if supported
    if hasattr(validator, 'set_strict_mode'):
        validator.set_strict_mode(strict)
    
    # Validate data
    is_valid = validator.validate(data)
    errors = validator.get_errors()
    
    return {
        "is_valid": is_valid,
        "errors": errors,
        "details": {
            "validator_version": getattr(validator, 'version', 'unknown'),
            "validator_type": getattr(validator, 'validator_type', 'unknown'),
            "rules_applied": rules or {},
            "strict_mode": strict
        }
    }


async def transform_data_with_plugin(transformer_type: str, data: Any,
                                   options: Dict[str, Any] = None,
                                   reverse: bool = False) -> Dict[str, Any]:
    """Transform (or reverse) via transformer plugin."""
    # Create transformer instance
    transformer = create_plugin_instance('transformers', transformer_type)
    
    # Apply options if provided
    if options and hasattr(transformer, 'configure'):
        transformer.configure(options)
    
    # Transform data
    if reverse and hasattr(transformer, 'reverse_transform'):
        transformed_data = transformer.reverse_transform(data)
    else:
        transformed_data = transformer.transform(data)
    
    return {
        "transformed_data": transformed_data,
        "is_reversible": getattr(transformer, 'reversible', False),
        "metadata": {
            "transformer_version": getattr(transformer, 'version', 'unknown'),
            "transformer_type": getattr(transformer, 'transformer_type', 'unknown'),
            "options_applied": options or {},
            "reverse_applied": reverse
        }
    }


def get_system_health() -> Dict[str, Any]:
    """Compute health snapshot (contract compliance + perf)."""
    total_plugins = sum(len(plugins) for plugins in PLUGIN_REGISTRY.values())
    active_plugins = 0
    contract_violations = 0
    
    # Check each plugin for contract compliance
    for category, plugins in PLUGIN_REGISTRY.items():
        for name, info in plugins.items():
            plugin_class = info['class']
            contract = info['contract']
            violations = validate_contract_compliance(plugin_class, contract)
            if violations:
                contract_violations += len(violations)
            else:
                active_plugins += 1
    
    return {
        "healthy": contract_violations == 0,
        "plugin_registry_status": "operational" if contract_violations == 0 else "degraded",
        "active_plugins": active_plugins,
        "total_plugins": total_plugins,
        "contract_violations": contract_violations,
        "performance_metrics": {
            "total_tracked_methods": len(PERFORMANCE_METRICS),
            "total_method_calls": sum(len(times) for times in PERFORMANCE_METRICS.values()),
            "average_response_time_ms": (
                sum(sum(times) for times in PERFORMANCE_METRICS.values()) * 1000 /
                max(sum(len(times) for times in PERFORMANCE_METRICS.values()), 1)
            )
        }
    }
    # (end)
