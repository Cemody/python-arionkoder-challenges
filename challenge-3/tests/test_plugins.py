"""
Tests for plugin system functionality.
"""

import pytest
from typing import Dict, Any

from utils import (
    get_registered_plugins,
    create_plugin_instance,
    get_performance_summary,
    clear_all_metrics,
    ContractViolationError
)

# Import plugins to ensure they're registered
from plugins.processors import JSONProcessor, CSVProcessor, XMLProcessor
from plugins.validators import SchemaValidator, RangeValidator, FormatValidator, CompositeValidator
from plugins.transformers import (
    UppercaseTransformer, 
    DateTransformer, 
    NumberTransformer,
    TextNormalizer,
    DataTypeConverter
)


class TestPluginRegistration:
    """Test plugin registration system"""
    
    def test_processors_registered(self):
        """Test that processor plugins are registered"""
        processors = get_registered_plugins('processors')
        
        assert 'JSONProcessor' in processors
        assert 'CSVProcessor' in processors
        assert 'XMLProcessor' in processors
        
        # Check registration details
        json_info = processors['JSONProcessor']
        assert json_info['class'] == JSONProcessor
        assert json_info['contract'].name == "DataProcessor"
        assert 'registered_at' in json_info
    
    def test_validators_registered(self):
        """Test that validator plugins are registered"""
        validators = get_registered_plugins('validators')
        
        assert 'SchemaValidator' in validators
        assert 'RangeValidator' in validators
        assert 'FormatValidator' in validators
        assert 'CompositeValidator' in validators
        
        # Check registration details
        schema_info = validators['SchemaValidator']
        assert schema_info['class'] == SchemaValidator
        assert schema_info['contract'].name == "Validator"
    
    def test_transformers_registered(self):
        """Test that transformer plugins are registered"""
        transformers = get_registered_plugins('transformers')
        
        assert 'UppercaseTransformer' in transformers
        assert 'DateTransformer' in transformers
        assert 'NumberTransformer' in transformers
        assert 'TextNormalizer' in transformers
        assert 'DataTypeConverter' in transformers
        
        # Check registration details
        upper_info = transformers['UppercaseTransformer']
        assert upper_info['class'] == UppercaseTransformer
        assert upper_info['contract'].name == "Transformer"
    
    def test_get_all_plugins(self):
        """Test getting all registered plugins"""
        all_plugins = get_registered_plugins()
        
        assert 'processors' in all_plugins
        assert 'validators' in all_plugins
        assert 'transformers' in all_plugins
        
        # Should have multiple plugins in each category
        assert len(all_plugins['processors']) >= 3
        assert len(all_plugins['validators']) >= 4
        assert len(all_plugins['transformers']) >= 5


class TestPluginInstantiation:
    """Test plugin instance creation"""
    
    def test_create_processor_instances(self):
        """Test creating processor plugin instances"""
        
        # JSON Processor
        json_proc = create_plugin_instance('processors', 'JSONProcessor')
        assert isinstance(json_proc, JSONProcessor)
        assert json_proc.processor_type == "json"
        assert json_proc.version == "1.0.0"
        
        # CSV Processor
        csv_proc = create_plugin_instance('processors', 'CSVProcessor')
        assert isinstance(csv_proc, CSVProcessor)
        assert csv_proc.processor_type == "csv"
        
        # XML Processor
        xml_proc = create_plugin_instance('processors', 'XMLProcessor')
        assert isinstance(xml_proc, XMLProcessor)
        assert xml_proc.processor_type == "xml"
    
    def test_create_validator_instances(self):
        """Test creating validator plugin instances"""
        
        # Schema Validator (no args)
        schema_val = create_plugin_instance('validators', 'SchemaValidator')
        assert isinstance(schema_val, SchemaValidator)
        assert schema_val.validator_type == "schema"
        
        # Range Validator (with args)
        range_val = create_plugin_instance('validators', 'RangeValidator', 0, 100)
        assert isinstance(range_val, RangeValidator)
        assert range_val.validator_type == "range"
        assert range_val.min_value == 0
        assert range_val.max_value == 100
        
        # Format Validator (with kwargs)
        format_val = create_plugin_instance('validators', 'FormatValidator', format_type="email")
        assert isinstance(format_val, FormatValidator)
        assert format_val.validator_type == "format"
        assert format_val.format_type == "email"
    
    def test_create_transformer_instances(self):
        """Test creating transformer plugin instances"""
        
        # Uppercase Transformer
        upper_trans = create_plugin_instance('transformers', 'UppercaseTransformer')
        assert isinstance(upper_trans, UppercaseTransformer)
        assert upper_trans.transformer_type == "uppercase"
        assert upper_trans.reversible is True
        
        # Number Transformer (with args)
        num_trans = create_plugin_instance('transformers', 'NumberTransformer', "multiply", 3.0)
        assert isinstance(num_trans, NumberTransformer)
        assert num_trans.transformer_type == "number"
        assert num_trans.operation == "multiply"
        assert num_trans.factor == 3.0
    
    def test_invalid_plugin_creation(self):
        """Test error handling for invalid plugin creation"""
        
        # Invalid category
        with pytest.raises(ValueError, match="Unknown plugin category"):
            create_plugin_instance('invalid_category', 'SomePlugin')
        
        # Invalid plugin name
        with pytest.raises(ValueError, match="Unknown plugin"):
            create_plugin_instance('processors', 'NonExistentProcessor')


class TestProcessorPlugins:
    """Test processor plugin functionality"""
    
    def test_json_processor(self):
        """Test JSON processor functionality"""
        processor = JSONProcessor()
        
        # Test valid input
        test_data = {
            "Name": "John Doe",
            "Age": 30,
            "Email Address": "john@example.com",
            "nested": {"City": "New York", "Country": "USA"}
        }
        
        assert processor.validate_input(test_data) is True
        
        result = processor.process(test_data)
        assert "processed_data" in result
        assert "original_keys" in result
        assert "processed_keys" in result
        
        # Check key normalization
        processed = result["processed_data"]
        assert "name" in processed
        assert "age" in processed
        assert "email_address" in processed
        assert "nested" in processed
        assert processed["nested"]["city"] == "New York"
        
        # Test schema
        schema = processor.get_schema()
        assert "input" in schema
        assert "output" in schema
    
    def test_csv_processor(self):
        """Test CSV processor functionality"""
        processor = CSVProcessor()
        
        # Test valid CSV
        csv_data = {
            "csv_content": "name,age,salary\nAlice,25,50000\nBob,30,60000",
            "delimiter": ","
        }
        
        assert processor.validate_input(csv_data) is True
        
        result = processor.process(csv_data)
        assert "processed_data" in result
        assert "row_count" in result
        assert "column_count" in result
        assert "columns" in result
        
        # Check parsed data
        assert result["row_count"] == 2
        assert result["column_count"] == 3
        assert "name" in result["columns"]
        assert "age" in result["columns"]
        assert "salary" in result["columns"]
        
        # Check data types (numbers should be converted)
        rows = result["processed_data"]
        assert rows[0]["age"] == 25  # Should be int
        assert rows[0]["salary"] == 50000  # Should be int
        assert rows[1]["name"] == "Bob"  # Should remain string
    
    def test_xml_processor(self):
        """Test XML processor functionality"""
        processor = XMLProcessor()
        
        # Test valid XML
        xml_data = {
            "xml_content": """
            <person id="1">
                <name>Alice</name>
                <age>25</age>
                <address>
                    <city>New York</city>
                    <country>USA</country>
                </address>
            </person>
            """
        }
        
        assert processor.validate_input(xml_data) is True
        
        result = processor.process(xml_data)
        assert "processed_data" in result
        assert "root_tag" in result
        assert "element_count" in result
        
        # Check structure
        assert result["root_tag"] == "person"
        processed = result["processed_data"]
        assert "@attributes" in processed
        assert processed["@attributes"]["id"] == "1"
        assert "name" in processed
        assert "address" in processed


class TestValidatorPlugins:
    """Test validator plugin functionality"""
    
    def test_range_validator(self):
        """Test range validator functionality"""
        validator = RangeValidator(0, 100)
        
        # Valid values
        assert validator.validate(50) is True
        assert validator.validate(0) is True
        assert validator.validate(100) is True
        
        # Invalid values
        assert validator.validate(-1) is False
        assert validator.validate(101) is False
        assert validator.validate("not a number") is False
        
        # Check errors
        validator.validate(-5)
        errors = validator.get_errors()
        assert len(errors) >= 1
        assert "below minimum" in errors[0]
    
    def test_format_validator(self):
        """Test format validator functionality"""
        email_validator = FormatValidator("email")
        
        # Valid emails
        assert email_validator.validate("user@domain.com") is True
        assert email_validator.validate("test.email@example.org") is True
        
        # Invalid emails
        assert email_validator.validate("not-an-email") is False
        assert email_validator.validate("@domain.com") is False
        assert email_validator.validate(123) is False
        
        # Check errors
        email_validator.validate("invalid")
        errors = email_validator.get_errors()
        assert len(errors) >= 1
        assert "email format" in errors[0]
    
    def test_composite_validator(self):
        """Test composite validator functionality"""
        range_val = RangeValidator(18, 65)
        composite = CompositeValidator([range_val])
        
        # Valid value
        assert composite.validate(30) is True
        
        # Invalid value
        assert composite.validate(10) is False
        
        # Check composite errors
        composite.validate(70)
        errors = composite.get_errors()
        assert len(errors) >= 1
        assert "range" in errors[0]


class TestTransformerPlugins:
    """Test transformer plugin functionality"""
    
    def test_uppercase_transformer(self):
        """Test uppercase transformer functionality"""
        transformer = UppercaseTransformer()
        
        # Transform string
        result = transformer.transform("hello world")
        assert result == "HELLO WORLD"
        
        # Reverse transform
        reversed_result = transformer.reverse_transform(result)
        assert reversed_result == "hello world"
        
        # Transform nested data
        nested_data = {"message": "hello", "items": ["world", "test"]}
        transformed = transformer.transform(nested_data)
        assert transformed["message"] == "HELLO"
        assert transformed["items"] == ["WORLD", "TEST"]
    
    def test_number_transformer(self):
        """Test number transformer functionality"""
        multiplier = NumberTransformer("multiply", 2.0)
        
        # Transform number
        result = multiplier.transform(5)
        assert result == 10
        
        # Reverse transform
        reversed_result = multiplier.reverse_transform(result)
        assert reversed_result == 5
        
        # Transform nested data
        data = {"value": 3, "list": [1, 2, 3]}
        transformed = multiplier.transform(data)
        assert transformed["value"] == 6
        assert transformed["list"] == [2, 4, 6]
    
    def test_text_normalizer(self):
        """Test text normalizer functionality"""
        normalizer = TextNormalizer()
        
        # Normalize text
        messy_text = "Hello,   World!!!   How are you???"
        result = normalizer.transform(messy_text)
        assert result == "Hello World How are you"
        
        # Test non-reversible
        with pytest.raises(NotImplementedError):
            normalizer.reverse_transform(result)


class TestPerformanceMonitoring:
    """Test performance monitoring functionality"""
    
    def setup_method(self):
        """Clear metrics before each test"""
        clear_all_metrics()
    
    def test_performance_tracking(self):
        """Test that performance is tracked automatically"""
        processor = JSONProcessor()
        
        # Perform operations
        test_data = {"test": "data"}
        for _ in range(3):
            processor.process(test_data)
            processor.validate_input(test_data)
        
        # Check global metrics
        global_metrics = get_performance_summary()
        assert len(global_metrics) >= 2
        
        # Check individual plugin metrics
        plugin_metrics = processor.get_performance_stats()
        assert 'process' in plugin_metrics
        assert 'validate_input' in plugin_metrics
        
        process_stats = plugin_metrics['process']
        assert process_stats['call_count'] == 3
        assert process_stats['avg_time'] > 0
        assert process_stats['total_time'] > 0
    
    def test_metrics_reset(self):
        """Test metrics can be reset"""
        processor = JSONProcessor()
        
        # Generate metrics
        processor.process({"test": "data"})
        
        # Check metrics exist
        stats_before = processor.get_performance_stats()
        assert stats_before['process']['call_count'] == 1
        
        # Reset metrics
        processor.reset_performance_stats()
        
        # Check metrics cleared
        stats_after = processor.get_performance_stats()
        assert stats_after['process']['call_count'] == 0
