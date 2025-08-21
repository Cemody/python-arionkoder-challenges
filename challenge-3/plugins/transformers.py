"""
Example transformer plugins demonstrating metaclass contract enforcement.
"""

import re
from datetime import datetime, timedelta
from typing import Any
from utils import TransformerBase


class UppercaseTransformer(TransformerBase):
    """Transforms text to uppercase and back to lowercase"""
    
    transformer_type = "uppercase"
    reversible = True
    
    def transform(self, data: Any) -> Any:
        """Transform data to uppercase"""
        if isinstance(data, str):
            return data.upper()
        elif isinstance(data, dict):
            return {key: self.transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.transform(item) for item in data]
        else:
            return data
    
    def reverse_transform(self, data: Any) -> Any:
        """Reverse transform (uppercase to lowercase)"""
        if isinstance(data, str):
            return data.lower()
        elif isinstance(data, dict):
            return {key: self.reverse_transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.reverse_transform(item) for item in data]
        else:
            return data


class DateTransformer(TransformerBase):
    """Transforms dates between different formats"""
    
    transformer_type = "date"
    reversible = True
    
    def __init__(self, input_format: str = "%Y-%m-%d", output_format: str = "%d/%m/%Y"):
        self.input_format = input_format
        self.output_format = output_format
    
    def transform(self, data: Any) -> Any:
        """Transform date from input format to output format"""
        if isinstance(data, str):
            try:
                # Parse date with input format
                date_obj = datetime.strptime(data, self.input_format)
                # Return in output format
                return date_obj.strftime(self.output_format)
            except ValueError:
                # If parsing fails, return original data
                return data
        elif isinstance(data, dict):
            return {key: self.transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.transform(item) for item in data]
        else:
            return data
    
    def reverse_transform(self, data: Any) -> Any:
        """Reverse transform (output format back to input format)"""
        if isinstance(data, str):
            try:
                # Parse date with output format
                date_obj = datetime.strptime(data, self.output_format)
                # Return in input format
                return date_obj.strftime(self.input_format)
            except ValueError:
                # If parsing fails, return original data
                return data
        elif isinstance(data, dict):
            return {key: self.reverse_transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.reverse_transform(item) for item in data]
        else:
            return data


class NumberTransformer(TransformerBase):
    """Transforms numbers by applying mathematical operations"""
    
    transformer_type = "number"
    reversible = True
    
    def __init__(self, operation: str = "multiply", factor: float = 2.0):
        self.operation = operation
        self.factor = factor
        
        # Define operations and their reverses
        self.operations = {
            'multiply': lambda x: x * self.factor,
            'divide': lambda x: x / self.factor,
            'add': lambda x: x + self.factor,
            'subtract': lambda x: x - self.factor,
            'power': lambda x: x ** self.factor,
            'sqrt': lambda x: x ** (1.0 / self.factor)
        }
        
        self.reverse_operations = {
            'multiply': lambda x: x / self.factor,
            'divide': lambda x: x * self.factor,
            'add': lambda x: x - self.factor,
            'subtract': lambda x: x + self.factor,
            'power': lambda x: x ** (1.0 / self.factor),
            'sqrt': lambda x: x ** self.factor
        }
    
    def transform(self, data: Any) -> Any:
        """Apply mathematical transformation to numeric data"""
        if isinstance(data, (int, float)):
            operation_func = self.operations.get(self.operation)
            if operation_func:
                try:
                    return operation_func(data)
                except (ZeroDivisionError, ValueError, OverflowError):
                    return data
            return data
        elif isinstance(data, dict):
            return {key: self.transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.transform(item) for item in data]
        else:
            return data
    
    def reverse_transform(self, data: Any) -> Any:
        """Apply reverse mathematical transformation"""
        if isinstance(data, (int, float)):
            reverse_func = self.reverse_operations.get(self.operation)
            if reverse_func:
                try:
                    return reverse_func(data)
                except (ZeroDivisionError, ValueError, OverflowError):
                    return data
            return data
        elif isinstance(data, dict):
            return {key: self.reverse_transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.reverse_transform(item) for item in data]
        else:
            return data


class TextNormalizer(TransformerBase):
    """Normalizes text by removing special characters, extra spaces, etc."""
    
    transformer_type = "text_normalizer"
    reversible = False  # Text normalization is not reversible
    
    def __init__(self, remove_special_chars: bool = True, normalize_spaces: bool = True):
        self.remove_special_chars = remove_special_chars
        self.normalize_spaces = normalize_spaces
    
    def transform(self, data: Any) -> Any:
        """Normalize text data"""
        if isinstance(data, str):
            result = data
            
            # Remove special characters
            if self.remove_special_chars:
                result = re.sub(r'[^\w\s]', '', result)
            
            # Normalize spaces
            if self.normalize_spaces:
                result = re.sub(r'\s+', ' ', result.strip())
            
            return result
        elif isinstance(data, dict):
            return {key: self.transform(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.transform(item) for item in data]
        else:
            return data
    
    def reverse_transform(self, data: Any) -> Any:
        """Text normalization is not reversible"""
        raise NotImplementedError("Text normalization is not reversible")


class DataTypeConverter(TransformerBase):
    """Converts data between different types"""
    
    transformer_type = "type_converter"
    reversible = True
    
    def __init__(self, target_type: str = "string"):
        self.target_type = target_type
        self.original_types = {}  # Store original types for reversal
    
    def transform(self, data: Any) -> Any:
        """Convert data to target type"""
        # Store original type for potential reversal
        data_id = id(data)
        self.original_types[data_id] = type(data).__name__
        
        if self.target_type == "string":
            return str(data)
        elif self.target_type == "integer":
            try:
                if isinstance(data, str):
                    return int(float(data))  # Handle "3.14" -> 3
                return int(data)
            except (ValueError, TypeError):
                return data
        elif self.target_type == "float":
            try:
                return float(data)
            except (ValueError, TypeError):
                return data
        elif self.target_type == "boolean":
            if isinstance(data, str):
                return data.lower() in ('true', '1', 'yes', 'on')
            return bool(data)
        else:
            return data
    
    def reverse_transform(self, data: Any) -> Any:
        """Attempt to convert back to original type"""
        # This is a simplified reverse transformation
        # In practice, perfect reversal would require storing more context
        data_id = id(data)
        original_type = self.original_types.get(data_id, "unknown")
        
        if original_type == "int":
            try:
                return int(float(str(data)))
            except (ValueError, TypeError):
                return data
        elif original_type == "float":
            try:
                return float(str(data))
            except (ValueError, TypeError):
                return data
        elif original_type == "bool":
            if isinstance(data, str):
                return data.lower() in ('true', '1', 'yes', 'on')
            return bool(data)
        else:
            return data
