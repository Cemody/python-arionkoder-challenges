"""
Tests for metaclass functionality and contract enforcement.
"""

import pytest
from typing import Dict, Any

from utils import (
    ContractEnforcerMeta,
    DataProcessorBase,
    ValidatorBase,
    TransformerBase,
    ContractViolationError,
    MethodSignatureError,
    DATA_PROCESSOR_CONTRACT,
    VALIDATOR_CONTRACT,
    TRANSFORMER_CONTRACT,
    get_registered_plugins,
    clear_all_metrics
)


class TestMetaclassContractEnforcement:
    """Test metaclass contract enforcement functionality"""
    
    def setup_method(self):
        """Clear metrics before each test"""
        clear_all_metrics()
    
    def test_valid_processor_creation(self):
        """Test that valid processors are created successfully"""
        
        class ValidProcessor(DataProcessorBase):
            processor_type = "test"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {"processed": True, "data": data}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return isinstance(data, dict)
            
            def get_schema(self) -> Dict[str, Any]:
                return {"input": "dict", "output": "dict"}
        
        # Should create without error
        processor = ValidProcessor()
        assert processor.processor_type == "test"
        assert processor.version == "1.0.0"
        
        # Should be able to call methods
        test_data = {"test": "data"}
        result = processor.process(test_data)
        assert result["processed"] is True
        assert result["data"] == test_data
        
        # Should validate input
        assert processor.validate_input(test_data) is True
        assert processor.validate_input("invalid") is False
    
    def test_missing_required_attribute(self):
        """Test that missing required attributes cause contract violations"""
        
        with pytest.raises(ContractViolationError, match="missing required attribute"):
            class InvalidProcessor(DataProcessorBase):
                # Missing processor_type attribute
                version = "1.0.0"
                
                def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                    return {}
                
                def validate_input(self, data: Dict[str, Any]) -> bool:
                    return True
                
                def get_schema(self) -> Dict[str, Any]:
                    return {}
    
    def test_missing_required_method(self):
        """Test that missing required methods cause contract violations"""
        
        with pytest.raises(ContractViolationError, match="missing required method"):
            class InvalidProcessor(DataProcessorBase):
                processor_type = "test"
                version = "1.0.0"
                
                # Missing process method
                def validate_input(self, data: Dict[str, Any]) -> bool:
                    return True
                
                def get_schema(self) -> Dict[str, Any]:
                    return {}
    
    def test_automatic_registration(self):
        """Test that valid plugins are automatically registered"""
        initial_processors = len(get_registered_plugins('processors'))
        
        class AutoRegisteredProcessor(DataProcessorBase):
            processor_type = "auto_registered"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {"auto": "registered"}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        # Check that plugin was registered
        processors = get_registered_plugins('processors')
        assert len(processors) == initial_processors + 1
        assert 'AutoRegisteredProcessor' in processors
        
        # Check registration details
        plugin_info = processors['AutoRegisteredProcessor']
        assert plugin_info['class'] == AutoRegisteredProcessor
        assert plugin_info['contract'] == DATA_PROCESSOR_CONTRACT
        assert 'registered_at' in plugin_info
    
    def test_performance_monitoring_enhancement(self):
        """Test that methods are enhanced with performance monitoring"""
        
        class MonitoredProcessor(DataProcessorBase):
            processor_type = "monitored"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {"monitored": True}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        processor = MonitoredProcessor()
        
        # Call methods to generate metrics
        for _ in range(3):
            processor.process({"test": "data"})
            processor.validate_input({"test": "data"})
        
        # Check that performance stats are available
        stats = processor.get_performance_stats()
        assert 'process' in stats
        assert 'validate_input' in stats
        
        # Check stats structure
        process_stats = stats['process']
        assert process_stats['call_count'] == 3
        assert 'avg_time' in process_stats
        assert 'min_time' in process_stats
        assert 'max_time' in process_stats
        assert 'total_time' in process_stats
    
    def test_validator_contract_enforcement(self):
        """Test that validator contract is enforced"""
        
        class ValidValidator(ValidatorBase):
            validator_type = "test_validator"
            
            def validate(self, data: Any) -> bool:
                return isinstance(data, str)
            
            def get_errors(self) -> list:
                return []
        
        validator = ValidValidator()
        assert validator.validate("test") is True
        assert validator.validate(123) is False
    
    def test_transformer_contract_enforcement(self):
        """Test that transformer contract is enforced"""
        
        class ValidTransformer(TransformerBase):
            transformer_type = "test_transformer"
            reversible = True
            
            def transform(self, data: Any) -> Any:
                return str(data).upper()
            
            def reverse_transform(self, data: Any) -> Any:
                return str(data).lower()
        
        transformer = ValidTransformer()
        assert transformer.transform("hello") == "HELLO"
        assert transformer.reverse_transform("HELLO") == "hello"
    
    def test_abstract_base_classes_skip_enforcement(self):
        """Test that abstract base classes skip contract enforcement"""
        
        # This should not raise an error even though it's "incomplete"
        class AbstractTestBase(metaclass=ContractEnforcerMeta):
            __abstract__ = True
            # No required methods or attributes
        
        # Should create successfully
        base = AbstractTestBase()
        assert base is not None
    
    def test_method_enhancement_error_handling(self):
        """Test that enhanced methods properly handle errors"""
        
        class ErrorProneProcessor(DataProcessorBase):
            processor_type = "error_prone"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                if data.get("cause_error"):
                    raise ValueError("Intentional error for testing")
                return {"success": True}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return isinstance(data, dict)
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        processor = ErrorProneProcessor()
        
        # Normal operation should work
        result = processor.process({"normal": "data"})
        assert result["success"] is True
        
        # Error should be propagated
        with pytest.raises(ValueError, match="Intentional error"):
            processor.process({"cause_error": True})
    
    def test_performance_stats_reset(self):
        """Test that performance stats can be reset"""
        
        class ResetTestProcessor(DataProcessorBase):
            processor_type = "reset_test"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        processor = ResetTestProcessor()
        
        # Generate some stats
        for _ in range(5):
            processor.process({})
        
        stats_before = processor.get_performance_stats()
        assert stats_before['process']['call_count'] == 5
        
        # Reset stats
        processor.reset_performance_stats()
        
        stats_after = processor.get_performance_stats()
        assert stats_after['process']['call_count'] == 0
