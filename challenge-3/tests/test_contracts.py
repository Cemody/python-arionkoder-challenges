"""
Tests for API contract definitions and validation.
"""

import pytest
from typing import Dict, Any, List

from utils import (
    MethodContract,
    ClassContract,
    DATA_PROCESSOR_CONTRACT,
    VALIDATOR_CONTRACT,
    TRANSFORMER_CONTRACT,
    validate_contract_compliance,
    DataProcessorBase,
    ValidatorBase,
    TransformerBase
)


class TestContractDefinitions:
    """Test contract definition functionality"""
    
    def test_method_contract_creation(self):
        """Test creating method contracts"""
        contract = MethodContract(
            name="test_method",
            required_params=["self", "data"],
            param_types={"data": dict},
            return_type=bool,
            description="Test method"
        )
        
        assert contract.name == "test_method"
        assert contract.required_params == ["self", "data"]
        assert contract.param_types == {"data": dict}
        assert contract.return_type == bool
        assert contract.description == "Test method"
        assert contract.validation_rules == []
    
    def test_class_contract_creation(self):
        """Test creating class contracts"""
        method_contract = MethodContract(
            name="test_method",
            required_params=["self"],
            param_types={},
            return_type=str,
            description="Test"
        )
        
        contract = ClassContract(
            name="TestContract",
            required_methods=[method_contract],
            class_attributes=["test_attr"]
        )
        
        assert contract.name == "TestContract"
        assert len(contract.required_methods) == 1
        assert contract.required_methods[0] == method_contract
        assert contract.class_attributes == ["test_attr"]
        assert contract.optional_methods == []
        assert contract.inheritance_requirements == []
    
    def test_data_processor_contract_structure(self):
        """Test the structure of DATA_PROCESSOR_CONTRACT"""
        contract = DATA_PROCESSOR_CONTRACT
        
        assert contract.name == "DataProcessor"
        assert len(contract.required_methods) == 3
        
        # Check required methods
        method_names = [m.name for m in contract.required_methods]
        assert "process" in method_names
        assert "validate_input" in method_names
        assert "get_schema" in method_names
        
        # Check required attributes
        assert "processor_type" in contract.class_attributes
        assert "version" in contract.class_attributes
    
    def test_validator_contract_structure(self):
        """Test the structure of VALIDATOR_CONTRACT"""
        contract = VALIDATOR_CONTRACT
        
        assert contract.name == "Validator"
        assert len(contract.required_methods) == 2
        
        method_names = [m.name for m in contract.required_methods]
        assert "validate" in method_names
        assert "get_errors" in method_names
        
        assert "validator_type" in contract.class_attributes
    
    def test_transformer_contract_structure(self):
        """Test the structure of TRANSFORMER_CONTRACT"""
        contract = TRANSFORMER_CONTRACT
        
        assert contract.name == "Transformer"
        assert len(contract.required_methods) == 2
        
        method_names = [m.name for m in contract.required_methods]
        assert "transform" in method_names
        assert "reverse_transform" in method_names
        
        assert "transformer_type" in contract.class_attributes
        assert "reversible" in contract.class_attributes


class TestContractValidation:
    """Test contract validation functionality"""
    
    def test_validate_compliant_class(self):
        """Test validation of a compliant class"""
        
        class CompliantProcessor(DataProcessorBase):
            processor_type = "compliant"
            version = "1.0.0"
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        violations = validate_contract_compliance(CompliantProcessor, DATA_PROCESSOR_CONTRACT)
        assert violations == []
    
    def test_validate_non_compliant_class_missing_attribute(self):
        """Test validation of class missing required attribute"""
        
        class NonCompliantClass:
            # Missing processor_type and version
            
            def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
                return {}
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        violations = validate_contract_compliance(NonCompliantClass, DATA_PROCESSOR_CONTRACT)
        assert len(violations) >= 2  # Missing processor_type and version
        assert any("processor_type" in v for v in violations)
        assert any("version" in v for v in violations)
    
    def test_validate_non_compliant_class_missing_method(self):
        """Test validation of class missing required method"""
        
        class NonCompliantClass:
            processor_type = "test"
            version = "1.0.0"
            
            # Missing process method
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        violations = validate_contract_compliance(NonCompliantClass, DATA_PROCESSOR_CONTRACT)
        assert len(violations) >= 1
        assert any("process" in v for v in violations)
    
    def test_validate_non_callable_method(self):
        """Test validation when required method is not callable"""
        
        class NonCompliantClass:
            processor_type = "test"
            version = "1.0.0"
            process = "not a method"  # Not callable
            
            def validate_input(self, data: Dict[str, Any]) -> bool:
                return True
            
            def get_schema(self) -> Dict[str, Any]:
                return {}
        
        violations = validate_contract_compliance(NonCompliantClass, DATA_PROCESSOR_CONTRACT)
        assert len(violations) >= 1
        assert any("not callable" in v for v in violations)
    
    def test_validate_validator_compliance(self):
        """Test validation of validator classes"""
        
        class CompliantValidator(ValidatorBase):
            validator_type = "test"
            
            def validate(self, data: Any) -> bool:
                return True
            
            def get_errors(self) -> List[str]:
                return []
        
        violations = validate_contract_compliance(CompliantValidator, VALIDATOR_CONTRACT)
        assert violations == []
        
        class NonCompliantValidator:
            # Missing validator_type
            def validate(self, data: Any) -> bool:
                return True
            # Missing get_errors method
        
        violations = validate_contract_compliance(NonCompliantValidator, VALIDATOR_CONTRACT)
        assert len(violations) >= 2
        assert any("validator_type" in v for v in violations)
        assert any("get_errors" in v for v in violations)
    
    def test_validate_transformer_compliance(self):
        """Test validation of transformer classes"""
        
        class CompliantTransformer(TransformerBase):
            transformer_type = "test"
            reversible = True
            
            def transform(self, data: Any) -> Any:
                return data
            
            def reverse_transform(self, data: Any) -> Any:
                return data
        
        violations = validate_contract_compliance(CompliantTransformer, TRANSFORMER_CONTRACT)
        assert violations == []
        
        class NonCompliantTransformer:
            # Missing required attributes and methods
            pass
        
        violations = validate_contract_compliance(NonCompliantTransformer, TRANSFORMER_CONTRACT)
        assert len(violations) >= 4  # 2 attributes + 2 methods
        assert any("transformer_type" in v for v in violations)
        assert any("reversible" in v for v in violations)
        assert any("transform" in v for v in violations)
        assert any("reverse_transform" in v for v in violations)


class TestMethodContracts:
    """Test individual method contract validation"""
    
    def test_method_contract_param_types(self):
        """Test method contract parameter type definitions"""
        
        # Process method contract
        process_methods = [m for m in DATA_PROCESSOR_CONTRACT.required_methods if m.name == "process"]
        assert len(process_methods) == 1
        
        process_contract = process_methods[0]
        assert "self" in process_contract.required_params
        assert "data" in process_contract.required_params
        assert process_contract.param_types["data"] == Dict[str, Any]
        assert process_contract.return_type == Dict[str, Any]
    
    def test_validation_method_contract(self):
        """Test validation method contracts"""
        
        # Validator validate method
        validate_methods = [m for m in VALIDATOR_CONTRACT.required_methods if m.name == "validate"]
        assert len(validate_methods) == 1
        
        validate_contract = validate_methods[0]
        assert "self" in validate_contract.required_params
        assert "data" in validate_contract.required_params
        assert validate_contract.return_type == bool
    
    def test_transformer_method_contracts(self):
        """Test transformer method contracts"""
        
        # Transform method
        transform_methods = [m for m in TRANSFORMER_CONTRACT.required_methods if m.name == "transform"]
        assert len(transform_methods) == 1
        
        transform_contract = transform_methods[0]
        assert "self" in transform_contract.required_params
        assert "data" in transform_contract.required_params
        assert transform_contract.return_type == Any
        
        # Reverse transform method
        reverse_methods = [m for m in TRANSFORMER_CONTRACT.required_methods if m.name == "reverse_transform"]
        assert len(reverse_methods) == 1
        
        reverse_contract = reverse_methods[0]
        assert "self" in reverse_contract.required_params
        assert "data" in reverse_contract.required_params
        assert reverse_contract.return_type == Any
