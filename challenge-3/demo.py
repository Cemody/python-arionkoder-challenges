"""
Challenge 3 - Meta-Programming Demonstration

Interactive demonstration of advanced meta-programming features including:
- Metaclass-based API contract enforcement
- Automatic plugin registration
- Performance monitoring
- Runtime contract validation
"""

import time
import json
from typing import Dict, Any

from utils import (
    DataProcessorBase, 
    ValidatorBase, 
    TransformerBase,
    get_registered_plugins,
    create_plugin_instance,
    get_performance_summary,
    clear_all_metrics,
    ContractViolationError,
    MethodSignatureError
)

# Import plugins to trigger registration
from plugins.processors import JSONProcessor, CSVProcessor, XMLProcessor
from plugins.validators import SchemaValidator, RangeValidator, FormatValidator, CompositeValidator
from plugins.transformers import (
    UppercaseTransformer, 
    DateTransformer, 
    NumberTransformer, 
    TextNormalizer,
    DataTypeConverter
)


def demonstrate_metaclass_enforcement():
    """Demonstrate how metaclasses enforce API contracts"""
    print("\n" + "="*60)
    print("🔧 METACLASS CONTRACT ENFORCEMENT DEMONSTRATION")
    print("="*60)
    
    print("\n1. ✅ Valid Plugin Creation:")
    
    # This will work - follows contract
    try:
        class ValidPlugin(DataProcessorBase):
            processor_type = "valid"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {"result": "processed", "input": data}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return isinstance(data, dict)
            
            def get_schema(self) -> Dict[str, Any]:
                return {"input": "dict", "output": "dict"}
        
        print("   ✅ ValidPlugin created successfully")
        print("   📝 Automatically registered in plugin registry")
        
        # Test the plugin
        plugin = ValidPlugin()
        result = plugin.process({"test": "data"})
        print(f"   🔄 Plugin test result: {result}")
        
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
    
    print("\n2. ❌ Invalid Plugin Creation (Contract Violation):")
    
    # This will fail - missing required methods
    try:
        class InvalidPlugin(DataProcessorBase):
            processor_type = "invalid"
            # Missing version attribute
            # Missing required methods
            pass
        
        print("   ❌ This should not print - plugin creation should fail")
        
    except ContractViolationError as e:
        print(f"   ✅ Contract violation caught: {e}")
    except Exception as e:
        print(f"   ⚠️  Unexpected error type: {e}")
    
    print("\n3. 🔍 Method Signature Validation:")
    
    try:
        class BadSignaturePlugin(DataProcessorBase):
            processor_type = "bad_signature"
            version = "1.0.0"
            
            # Wrong signature - missing 'data' parameter
            def process(self) -> Dict[str, Any]:
                return {}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        print("   ⚠️  Plugin created despite signature issues (warnings logged)")
        
    except Exception as e:
        print(f"   ✅ Signature validation caught: {e}")


def demonstrate_automatic_registration():
    """Demonstrate automatic plugin registration"""
    print("\n" + "="*60)
    print("📋 AUTOMATIC PLUGIN REGISTRATION DEMONSTRATION")
    print("="*60)
    
    plugins = get_registered_plugins()
    
    print(f"\n📊 Registration Summary:")
    print(f"   Total Categories: {len(plugins)}")
    
    for category, plugin_dict in plugins.items():
        print(f"\n   📂 {category.upper()}:")
        for name, info in plugin_dict.items():
            print(f"      • {name} (v{getattr(info['class'], 'version', 'unknown')})")
            print(f"        Contract: {info['contract'].name}")
            print(f"        Registered: {time.ctime(info['registered_at'])}")
    
    print(f"\n🔧 Creating Plugin Instances:")
    
    # Test creating instances
    try:
        json_processor = create_plugin_instance('processors', 'JSONProcessor')
        print(f"   ✅ Created JSONProcessor: {json_processor}")
        
        range_validator = create_plugin_instance('validators', 'RangeValidator', 0, 100)
        print(f"   ✅ Created RangeValidator: {range_validator}")
        
        uppercase_transformer = create_plugin_instance('transformers', 'UppercaseTransformer')
        print(f"   ✅ Created UppercaseTransformer: {uppercase_transformer}")
        
    except Exception as e:
        print(f"   ❌ Plugin creation error: {e}")


def demonstrate_performance_monitoring():
    """Demonstrate automatic performance monitoring"""
    print("\n" + "="*60)
    print("📈 PERFORMANCE MONITORING DEMONSTRATION")
    print("="*60)
    
    print("\n🔄 Running Plugin Operations (with automatic monitoring):")
    
    # Clear previous metrics
    clear_all_metrics()
    
    # Create and use various plugins
    json_proc = JSONProcessor()
    range_val = RangeValidator(0, 100)
    upper_trans = UppercaseTransformer()
    
    # Test data
    test_data = {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "nested": {"city": "New York", "country": "USA"}
    }
    
    print(f"   📝 Test data: {test_data}")
    
    # Run operations multiple times to generate metrics
    for i in range(5):
        print(f"\n   🔄 Iteration {i+1}:")
        
        # Process with JSON processor
        processed = json_proc.process(test_data)
        print(f"      📄 JSON processed: {len(str(processed))} chars")
        
        # Validate age
        age_valid = range_val.validate(test_data["age"])
        print(f"      ✅ Age validation: {age_valid}")
        
        # Transform text
        transformed = upper_trans.transform(test_data["name"])
        print(f"      🔤 Text transform: {transformed}")
        
        time.sleep(0.1)  # Small delay to see timing differences
    
    print("\n📊 Performance Metrics Summary:")
    metrics = get_performance_summary()
    
    for method_key, stats in metrics.items():
        print(f"\n   📈 {method_key}:")
        print(f"      Calls: {stats['call_count']}")
        print(f"      Avg Time: {stats['avg_time']:.4f}s")
        print(f"      Min Time: {stats['min_time']:.4f}s")
        print(f"      Max Time: {stats['max_time']:.4f}s")
        print(f"      Total Time: {stats['total_time']:.4f}s")


def demonstrate_runtime_validation():
    """Demonstrate runtime contract validation"""
    print("\n" + "="*60)
    print("🛡️ RUNTIME CONTRACT VALIDATION DEMONSTRATION")
    print("="*60)
    
    print("\n1. ✅ Valid Operations:")
    
    # Create processors
    json_proc = JSONProcessor()
    csv_proc = CSVProcessor()
    
    # Valid JSON processing
    json_data = {"users": [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]}
    result = json_proc.process(json_data)
    print(f"   ✅ JSON processing successful: {result['processed_keys']} keys processed")
    
    # Valid CSV processing
    csv_data = {
        "csv_content": "name,age,city\nAlice,25,NYC\nBob,30,LA",
        "delimiter": ","
    }
    result = csv_proc.process(csv_data)
    print(f"   ✅ CSV processing successful: {result['row_count']} rows processed")
    
    print("\n2. ❌ Invalid Operations (Contract Violations):")
    
    # Invalid input type for JSON processor
    try:
        json_proc.process("invalid string input")
        print("   ❌ This should not print")
    except ValueError as e:
        print(f"   ✅ Input validation caught: {e}")
    
    # Missing required field for CSV processor
    try:
        csv_proc.process({"wrong_field": "no csv_content here"})
        print("   ❌ This should not print")
    except Exception as e:
        print(f"   ✅ Validation caught: {e}")


def demonstrate_advanced_features():
    """Demonstrate advanced meta-programming features"""
    print("\n" + "="*60)
    print("🚀 ADVANCED META-PROGRAMMING FEATURES")
    print("="*60)
    
    print("\n1. 🔧 Enhanced Method Capabilities:")
    
    # Create a validator with schema
    schema = {
        "type": "object",
        "required": ["name", "age"],
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer", "minimum": 0, "maximum": 150},
            "email": {"type": "string", "pattern": r"^[^@]+@[^@]+\.[^@]+$"}
        }
    }
    
    schema_validator = SchemaValidator(schema)
    
    # Test with valid data
    valid_data = {"name": "Alice", "age": 25, "email": "alice@example.com"}
    is_valid = schema_validator.validate(valid_data)
    print(f"   ✅ Schema validation (valid data): {is_valid}")
    
    # Test with invalid data
    invalid_data = {"name": "Bob", "age": 200, "email": "invalid-email"}
    is_valid = schema_validator.validate(invalid_data)
    errors = schema_validator.get_errors()
    print(f"   ❌ Schema validation (invalid data): {is_valid}")
    print(f"      Errors: {errors}")
    
    print("\n2. 🔄 Complex Transformations:")
    
    # Chain multiple transformers
    text_data = {"message": "Hello, World!", "timestamp": "2024-01-15"}
    
    # Uppercase transformation
    upper_trans = UppercaseTransformer()
    uppercase_result = upper_trans.transform(text_data)
    print(f"   🔤 Uppercase transform: {uppercase_result}")
    
    # Reverse transformation
    lowercase_result = upper_trans.reverse_transform(uppercase_result)
    print(f"   🔄 Reverse transform: {lowercase_result}")
    
    # Date transformation
    date_trans = DateTransformer("%Y-%m-%d", "%B %d, %Y")
    date_result = date_trans.transform(text_data)
    print(f"   📅 Date transform: {date_result}")
    
    print("\n3. 📊 Individual Plugin Performance Stats:")
    
    # Create a fresh processor to demonstrate performance stats
    json_proc_stats = JSONProcessor()
    
    # Run a few operations to generate stats
    test_data = {"sample": "data", "for": "stats"}
    for _ in range(3):
        json_proc_stats.process(test_data)
    
    # Get performance stats for this specific plugin
    stats = json_proc_stats.get_performance_stats()
    print(f"   📈 JSONProcessor performance:")
    for method, method_stats in stats.items():
        print(f"      {method}: {method_stats['call_count']} calls, "
              f"avg {method_stats['avg_time']:.4f}s")


def demonstrate_composite_patterns():
    """Demonstrate composite patterns and plugin combinations"""
    print("\n" + "="*60)
    print("🔗 COMPOSITE PATTERNS DEMONSTRATION")
    print("="*60)
    
    print("\n🔧 Creating Composite Validator:")
    
    # Create multiple validators
    range_validator = RangeValidator(18, 65)  # Age range
    format_validator = FormatValidator("email")  # Email format
    
    # Create composite validator
    composite = CompositeValidator([range_validator, format_validator])
    
    # Test with valid data
    print(f"   ✅ Testing age 25: {composite.validate(25)} (using range validator)")
    
    # Test with valid email
    composite_email = CompositeValidator([FormatValidator("email")])
    print(f"   ✅ Testing email 'user@domain.com': {composite_email.validate('user@domain.com')}")
    
    # Test with invalid email
    print(f"   ❌ Testing invalid email 'not-an-email': {composite_email.validate('not-an-email')}")
    errors = composite_email.get_errors()
    print(f"      Errors: {errors}")


if __name__ == "__main__":
    print("🚀 CHALLENGE 3: ADVANCED META-PROGRAMMING DEMONSTRATION")
    print("Using metaclasses to enforce API contracts across multiple plugin classes")
    
    try:
        demonstrate_metaclass_enforcement()
        demonstrate_automatic_registration()
        demonstrate_performance_monitoring()
        demonstrate_runtime_validation()
        demonstrate_advanced_features()
        demonstrate_composite_patterns()
        
        print("\n" + "="*60)
        print("🎉 DEMONSTRATION COMPLETE!")
        print("✅ All meta-programming features working correctly")
        print("✅ API contracts enforced successfully")
        print("✅ Plugins registered and functioning")
        print("✅ Performance monitoring active")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Demonstration failed: {e}")
        import traceback
        traceback.print_exc()
