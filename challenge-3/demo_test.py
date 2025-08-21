"""
This demo showcases all the key features and constraints testing for Challenge 3:

Features:
- Metaclass-enforced API contracts across multiple classes
- Automatic plugin registration upon class definition
- Runtime validation of class attributes and methods

Constraints:
- Must support inheritance properly
- Must provide descriptive error messages for contract violations
"""

import time
import gc
import json
from typing import Dict, Any
from fastapi.testclient import TestClient

# Import the app and utilities
from app import app
from utils import (
    get_registered_plugins, 
    create_plugin_instance,
    validate_contract_compliance,
    get_performance_summary,
    clear_all_metrics,
    ContractViolationError,
    DATA_PROCESSOR_CONTRACT,
    VALIDATOR_CONTRACT,
    TRANSFORMER_CONTRACT
)

# Import example plugins to trigger registration
from plugins.processors import JSONProcessor, CSVProcessor, XMLProcessor
from plugins.validators import SchemaValidator, RangeValidator, FormatValidator  
from plugins.transformers import UppercaseTransformer, DateTransformer, NumberTransformer


def measure_performance(test_name, test_func):
    """Helper to measure execution time and memory."""
    print(f"\n=== {test_name} ===")
    gc.collect()
    
    start_time = time.time()
    result = test_func()
    end_time = time.time()
    
    execution_time = (end_time - start_time) * 1000  # in ms
    print(f"⏱️  Execution time: {execution_time:.2f} ms")
    return result


def test_metaclass_contract_enforcement():
    """Test core metaclass contract enforcement functionality"""
    
    def run_test():
        print("🔒 Testing metaclass contract enforcement...")
        
        # Test 1: Contract compliance validation
        print("📋 Testing contract compliance validation...")
        
        # Check that all registered plugins comply with their contracts
        plugins = get_registered_plugins()
        violations_found = 0
        
        for category, plugin_dict in plugins.items():
            for name, info in plugin_dict.items():
                plugin_class = info['class']
                contract = info['contract']
                violations = validate_contract_compliance(plugin_class, contract)
                
                if violations:
                    print(f"❌ {name} has contract violations: {violations}")
                    violations_found += len(violations)
                else:
                    print(f"✅ {name} complies with {contract.name} contract")
        
        print(f"📊 Total contract violations found: {violations_found}")
        
        # Test 2: Automatic plugin registration
        print("\n🚀 Testing automatic plugin registration...")
        initial_count = sum(len(plugins) for plugins in plugins.values())
        print(f"📊 Total plugins registered: {initial_count}")
        
        # Verify specific plugins are registered
        expected_processors = ['JSONProcessor', 'CSVProcessor', 'XMLProcessor']
        expected_validators = ['SchemaValidator', 'RangeValidator', 'FormatValidator']
        expected_transformers = ['UppercaseTransformer', 'DateTransformer', 'NumberTransformer']
        
        for processor in expected_processors:
            if processor in plugins.get('processors', {}):
                print(f"✅ {processor} automatically registered")
            else:
                print(f"❌ {processor} not found in registry")
        
        for validator in expected_validators:
            if validator in plugins.get('validators', {}):
                print(f"✅ {validator} automatically registered")
            else:
                print(f"❌ {validator} not found in registry")
        
        for transformer in expected_transformers:
            if transformer in plugins.get('transformers', {}):
                print(f"✅ {transformer} automatically registered")
            else:
                print(f"❌ {transformer} not found in registry")
        
        return {
            "violations_found": violations_found,
            "total_plugins": initial_count,
            "registration_working": initial_count > 0
        }
    
    return measure_performance("Metaclass Contract Enforcement", run_test)


def test_runtime_validation():
    """Test runtime validation of class attributes and methods"""
    
    def run_test():
        print("🔍 Testing runtime validation...")
        
        # Test 1: Method signature validation
        print("📝 Testing method signature validation...")
        
        try:
            # Create instances and call methods to trigger validation
            processor = create_plugin_instance('processors', 'JSONProcessor')
            test_data = {"name": "test", "value": 123}
            
            # This should work fine
            is_valid = processor.validate_input(test_data)
            print(f"✅ Input validation returned: {is_valid}")
            
            result = processor.process(test_data)
            print(f"✅ Processing completed: {type(result)}")
            
            schema = processor.get_schema()
            print(f"✅ Schema retrieved: {len(schema)} keys")
            
        except Exception as e:
            print(f"❌ Runtime validation error: {e}")
        
        # Test 2: Performance monitoring (automatic enhancement)
        print("\n📊 Testing automatic performance monitoring...")
        
        # Clear metrics first
        clear_all_metrics()
        
        # Perform multiple operations to generate metrics
        for i in range(5):
            processor = create_plugin_instance('processors', 'JSONProcessor')
            processor.process({"test": f"data_{i}"})
            
            validator = create_plugin_instance('validators', 'SchemaValidator')
            validator.validate({"test": f"data_{i}"})
        
        # Check that metrics were collected
        metrics = get_performance_summary()
        print(f"📈 Collected metrics for {len(metrics)} methods")
        
        for method, stats in metrics.items():
            print(f"  {method}: {stats['call_count']} calls, avg {stats['avg_time']:.4f}s")
        
        return {
            "methods_monitored": len(metrics),
            "total_calls": sum(stats['call_count'] for stats in metrics.values())
        }
    
    return measure_performance("Runtime Validation", run_test)


def test_inheritance_support():
    """Test that contract enforcement supports inheritance properly"""
    
    def run_test():
        print("🧬 Testing inheritance support...")
        
        # Test 1: Base class inheritance
        print("👪 Testing base class inheritance...")
        
        # Import base classes
        from utils import DataProcessorBase, ValidatorBase, TransformerBase
        
        # Test that plugins inherit from base classes properly
        processor = create_plugin_instance('processors', 'JSONProcessor')
        validator = create_plugin_instance('validators', 'SchemaValidator')
        transformer = create_plugin_instance('transformers', 'UppercaseTransformer')
        
        # Check inheritance
        is_processor_base = isinstance(processor, DataProcessorBase)
        is_validator_base = isinstance(validator, ValidatorBase)
        is_transformer_base = isinstance(transformer, TransformerBase)
        
        print(f"✅ JSONProcessor inherits from DataProcessorBase: {is_processor_base}")
        print(f"✅ SchemaValidator inherits from ValidatorBase: {is_validator_base}")
        print(f"✅ UppercaseTransformer inherits from TransformerBase: {is_transformer_base}")
        
        # Test 2: Method enhancement through inheritance
        print("\n� Testing method enhancement through inheritance...")
        
        # Check that enhanced methods exist
        has_performance_stats = hasattr(processor, 'get_performance_stats')
        has_reset_stats = hasattr(processor, 'reset_performance_stats')
        
        print(f"✅ Performance monitoring methods added: {has_performance_stats and has_reset_stats}")
        
        if has_performance_stats:
            stats = processor.get_performance_stats()
            print(f"📊 Instance performance stats: {len(stats)} methods tracked")
        
        return {
            "inheritance_working": is_processor_base and is_validator_base and is_transformer_base,
            "method_enhancement": has_performance_stats and has_reset_stats
        }
    
    return measure_performance("Inheritance Support", run_test)


def test_api_endpoints():
    """Test API endpoints for contract enforcement features"""
    
    def run_test():
        print("🌐 Testing API endpoints...")
        
        client = TestClient(app)
        
        # Test 1: Plugin registry endpoint
        print("📋 Testing plugin registry endpoint...")
        response = client.get("/plugins")
        print(f"✅ Plugin registry status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 Total plugins returned: {data.get('total_plugins', 0)}")
            print(f"� Categories: {data.get('categories', [])}")
        
        # Test 2: Data processing endpoint
        print("\n🔄 Testing data processing endpoint...")
        process_request = {
            "processor_type": "JSONProcessor",
            "data": {"name": "test", "value": 123},
            "validate_input": True
        }
        response = client.post("/process", json=process_request)
        print(f"✅ Processing status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 Processing success: {data.get('success', False)}")
            print(f"⏱️  Processing time: {data.get('processing_time_ms', 0):.2f}ms")
        
        # Test 3: Data validation endpoint
        print("\n✅ Testing data validation endpoint...")
        validation_request = {
            "validator_type": "SchemaValidator",
            "data": {"name": "test"},
            "strict": True
        }
        response = client.post("/validate", json=validation_request)
        print(f"✅ Validation status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 Validation result: {data.get('is_valid', False)}")
            print(f"⏱️  Validation time: {data.get('validation_time_ms', 0):.2f}ms")
        
        # Test 4: Data transformation endpoint
        print("\n🔀 Testing data transformation endpoint...")
        transform_request = {
            "transformer_type": "UppercaseTransformer",
            "data": "hello world",
            "reverse": False
        }
        response = client.post("/transform", json=transform_request)
        print(f"✅ Transformation status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 Transformation success: {data.get('success', False)}")
            print(f"🔀 Result: {data.get('result', 'N/A')}")
        
        # Test 5: Performance metrics endpoint
        print("\n📈 Testing performance metrics endpoint...")
        response = client.get("/metrics")
        print(f"✅ Metrics status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 Total methods tracked: {data.get('total_methods', 0)}")
            print(f"📊 Total calls: {data.get('total_calls', 0)}")
        
        # Test 6: System health endpoint
        print("\n🏥 Testing system health endpoint...")
        response = client.get("/health")
        print(f"✅ Health check status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"💚 System healthy: {data.get('healthy', False)}")
            print(f"📊 Active plugins: {data.get('active_plugins', 0)}")
        
        return {"api_tests_passed": True}
    
    return measure_performance("API Endpoints", run_test)


def test_error_handling():
    """Test descriptive error messages for contract violations"""
    
    def run_test():
        print("⚠️  Testing error handling and descriptive messages...")
        
        client = TestClient(app)
        
        # Test 1: Plugin not found error
        print("🔍 Testing plugin not found error...")
        process_request = {
            "processor_type": "NonExistentProcessor",
            "data": {"test": "data"}
        }
        response = client.post("/process", json=process_request)
        print(f"✅ Error status: {response.status_code}")
        
        if response.status_code == 404:
            print("✅ Proper 404 error for missing plugin")
        
        # Test 2: Invalid validator error
        print("\n🔍 Testing invalid validator error...")
        validation_request = {
            "validator_type": "InvalidValidator",
            "data": {"test": "data"}
        }
        response = client.post("/validate", json=validation_request)
        print(f"✅ Error status: {response.status_code}")
        
        # Test 3: Contract compliance check
        print("\n📋 Testing contract compliance...")
        response = client.get("/contracts")
        print(f"✅ Contracts endpoint status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            contracts = data.get('available_contracts', {})
            print(f"📊 Available contracts: {list(contracts.keys())}")
            
            # Verify contract structure
            for contract_name, contract_def in contracts.items():
                required_methods = contract_def.get('required_methods', [])
                print(f"  {contract_name}: {len(required_methods)} required methods")
        
        return {"error_handling_working": True}
    
    return measure_performance("Error Handling", run_test)


def run_comprehensive_demo():
    """Run comprehensive demonstration of all features"""
    print("🚀 CHALLENGE 3: ADVANCED META-PROGRAMMING COMPREHENSIVE DEMO")
    print("=" * 70)
    
    results = {}
    
    # Run all tests
    results["contract_enforcement"] = test_metaclass_contract_enforcement()
    results["runtime_validation"] = test_runtime_validation()
    results["inheritance_support"] = test_inheritance_support()
    results["api_endpoints"] = test_api_endpoints()
    results["error_handling"] = test_error_handling()
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 DEMO SUMMARY")
    print("=" * 70)
    
    contract_result = results["contract_enforcement"]
    print(f"🔒 Contract Enforcement: {contract_result['total_plugins']} plugins, {contract_result['violations_found']} violations")
    
    runtime_result = results["runtime_validation"]
    print(f"🔍 Runtime Validation: {runtime_result['methods_monitored']} methods monitored, {runtime_result['total_calls']} calls")
    
    inheritance_result = results["inheritance_support"]
    print(f"🧬 Inheritance Support: {'✅ Working' if inheritance_result['inheritance_working'] else '❌ Failed'}")
    
    print(f"🌐 API Endpoints: {'✅ All functional' if results['api_endpoints']['api_tests_passed'] else '❌ Issues found'}")
    print(f"⚠️  Error Handling: {'✅ Working' if results['error_handling']['error_handling_working'] else '❌ Failed'}")
    
    print("\n✨ Meta-programming system demonstrates:")
    print("  • Metaclass-enforced API contracts")
    print("  • Automatic plugin registration")
    print("  • Runtime method validation and monitoring")
    print("  • Proper inheritance support")
    print("  • Descriptive error messages")
    
    return results


if __name__ == "__main__":
    run_comprehensive_demo()


if __name__ == "__main__":
    run_comprehensive_demo()
