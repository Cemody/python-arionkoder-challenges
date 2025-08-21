"""
Example validator plugins demonstrating metaclass contract enforcement.
"""

import re
from typing import Any, List
from utils import ValidatorBase


class SchemaValidator(ValidatorBase):
    """Validates data against a predefined schema"""
    
    validator_type = "schema"
    
    def __init__(self, schema: dict = None):
        self.schema = schema or {}
        self.errors = []
    
    def validate(self, data: Any) -> bool:
        """Validate data against the schema"""
        self.errors = []
        
        if not self.schema:
            self.errors.append("No schema defined")
            return False
        
        return self._validate_against_schema(data, self.schema, "")
    
    def _validate_against_schema(self, data: Any, schema: dict, path: str) -> bool:
        """Recursively validate data against schema"""
        valid = True
        
        # Check type
        expected_type = schema.get('type')
        if expected_type:
            if not self._check_type(data, expected_type):
                self.errors.append(f"Type mismatch at {path}: expected {expected_type}, got {type(data).__name__}")
                valid = False
        
        # Check required fields for objects
        if expected_type == 'object' and isinstance(data, dict):
            required = schema.get('required', [])
            for field in required:
                if field not in data:
                    self.errors.append(f"Missing required field: {path}.{field}")
                    valid = False
            
            # Validate properties
            properties = schema.get('properties', {})
            for field, field_schema in properties.items():
                if field in data:
                    field_valid = self._validate_against_schema(
                        data[field], field_schema, f"{path}.{field}"
                    )
                    valid = valid and field_valid
        
        # Check array items
        elif expected_type == 'array' and isinstance(data, list):
            items_schema = schema.get('items')
            if items_schema:
                for i, item in enumerate(data):
                    item_valid = self._validate_against_schema(
                        item, items_schema, f"{path}[{i}]"
                    )
                    valid = valid and item_valid
        
        # Check string patterns
        elif expected_type == 'string' and isinstance(data, str):
            pattern = schema.get('pattern')
            if pattern and not re.match(pattern, data):
                self.errors.append(f"Pattern mismatch at {path}: '{data}' does not match '{pattern}'")
                valid = False
        
        # Check numeric ranges
        elif expected_type in ['integer', 'number']:
            minimum = schema.get('minimum')
            maximum = schema.get('maximum')
            if minimum is not None and data < minimum:
                self.errors.append(f"Value at {path} ({data}) is below minimum ({minimum})")
                valid = False
            if maximum is not None and data > maximum:
                self.errors.append(f"Value at {path} ({data}) is above maximum ({maximum})")
                valid = False
        
        return valid
    
    def _check_type(self, data: Any, expected_type: str) -> bool:
        """Check if data matches expected type"""
        type_map = {
            'string': str,
            'integer': int,
            'number': (int, float),
            'boolean': bool,
            'array': list,
            'object': dict,
            'null': type(None)
        }
        
        expected_python_type = type_map.get(expected_type)
        if expected_python_type:
            return isinstance(data, expected_python_type)
        return True
    
    def get_errors(self) -> List[str]:
        """Return list of validation errors"""
        return self.errors.copy()


class RangeValidator(ValidatorBase):
    """Validates that numeric values are within specified ranges"""
    
    validator_type = "range"
    
    def __init__(self, min_value: float = None, max_value: float = None):
        self.min_value = min_value
        self.max_value = max_value
        self.errors = []
    
    def validate(self, data: Any) -> bool:
        """Validate that data is within range"""
        self.errors = []
        
        if not isinstance(data, (int, float)):
            self.errors.append(f"Expected numeric value, got {type(data).__name__}")
            return False
        
        valid = True
        
        if self.min_value is not None and data < self.min_value:
            self.errors.append(f"Value {data} is below minimum {self.min_value}")
            valid = False
        
        if self.max_value is not None and data > self.max_value:
            self.errors.append(f"Value {data} is above maximum {self.max_value}")
            valid = False
        
        return valid
    
    def get_errors(self) -> List[str]:
        """Return list of validation errors"""
        return self.errors.copy()


class FormatValidator(ValidatorBase):
    """Validates data against specific format patterns"""
    
    validator_type = "format"
    
    def __init__(self, format_type: str = "email"):
        self.format_type = format_type
        self.errors = []
        
        # Define format patterns
        self.patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'^\+?1?-?\.?\s?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
            'url': r'^https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*)?(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?$',
            'ipv4': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
            'date_iso': r'^\d{4}-\d{2}-\d{2}$',
            'time': r'^([01]?[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$',
            'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        }
    
    def validate(self, data: Any) -> bool:
        """Validate data against format pattern"""
        self.errors = []
        
        if not isinstance(data, str):
            self.errors.append(f"Expected string for format validation, got {type(data).__name__}")
            return False
        
        pattern = self.patterns.get(self.format_type)
        if not pattern:
            self.errors.append(f"Unknown format type: {self.format_type}")
            return False
        
        if not re.match(pattern, data, re.IGNORECASE):
            self.errors.append(f"Value '{data}' does not match {self.format_type} format")
            return False
        
        return True
    
    def get_errors(self) -> List[str]:
        """Return list of validation errors"""
        return self.errors.copy()


class CompositeValidator(ValidatorBase):
    """Validator that combines multiple validation rules"""
    
    validator_type = "composite"
    
    def __init__(self, validators: List[ValidatorBase] = None):
        self.validators = validators or []
        self.errors = []
    
    def add_validator(self, validator: ValidatorBase):
        """Add a validator to the composite"""
        self.validators.append(validator)
    
    def validate(self, data: Any) -> bool:
        """Validate data against all contained validators"""
        self.errors = []
        all_valid = True
        
        for i, validator in enumerate(self.validators):
            if not validator.validate(data):
                all_valid = False
                validator_errors = validator.get_errors()
                for error in validator_errors:
                    self.errors.append(f"Validator {i+1} ({validator.validator_type}): {error}")
        
        return all_valid
    
    def get_errors(self) -> List[str]:
        """Return list of validation errors from all validators"""
        return self.errors.copy()
