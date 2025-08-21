"""
Challenge 1 - Generator and Iterator Efficiency Tests

Tests for the core implementation: Transforms and aggregates data using generators and iterators.
Tests the efficiency and correctness of the underlying streaming mechanisms.
"""

from utils import (
    _iter_records, _project, _aggregate_in_place
)


class TestGeneratorAndIteratorEfficiency:
    """Test the efficiency of generators and iterators in data processing"""
    
    def test_record_iteration_efficiency(self):
        """Test that record iteration uses generators efficiently"""
        # Test the internal _iter_records function
        large_payload = {
            "events": [{"id": i, "type": "test"} for i in range(1000)],
            "data": {"records": [{"id": i + 1000, "type": "test"} for i in range(500)]}
        }
        
        # Count records using generator (should be memory efficient)
        record_count = 0
        for record in _iter_records(large_payload):
            record_count += 1
            if record_count > 2000:  # Safety break
                break
        
        # Should find all records from both events and data.records
        assert record_count >= 1500  # Original dict + events + records

    def test_projection_efficiency(self):
        """Test field projection efficiency"""
        # Create projector for specific fields
        projector = _project({"name", "department", "salary"})
        
        large_record = {
            "name": "John Doe",
            "age": 30,
            "department": "engineering", 
            "salary": 80000,
            "address": "123 Main St",
            "phone": "555-1234",
            "email": "john@company.com",
            "emergency_contact": "Jane Doe",
            "ssn": "123-45-6789",  # Sensitive field to be filtered out
            "notes": "A very long note field " * 100  # Large field to be filtered out
        }
        
        projected = projector(large_record)
        
        # Should only contain requested fields
        assert set(projected.keys()) == {"name", "department", "salary"}
        assert projected["name"] == "John Doe"
        assert projected["salary"] == 80000
        assert "ssn" not in projected
        assert "notes" not in projected

    def test_streaming_aggregation_efficiency(self):
        """Test in-place aggregation efficiency"""
        # Test _aggregate_in_place function
        aggregation = {}
        
        # Simulate streaming records
        records = [
            {"department": "eng", "salary": 80000},
            {"department": "sales", "salary": 70000}, 
            {"department": "eng", "salary": 90000},
            {"department": "marketing", "salary": 65000},
            {"department": "eng", "salary": 85000},
        ]
        
        for record in records:
            _aggregate_in_place(aggregation, record, "department", "salary")
        
        expected = {"eng": 255000.0, "sales": 70000.0, "marketing": 65000.0}
        assert aggregation == expected

    def test_projection_with_no_fields(self):
        """Test projection when no fields are specified (pass-through)"""
        projector = _project(None)
        
        original_record = {
            "name": "Jane Smith",
            "age": 28,
            "department": "sales",
            "salary": 75000
        }
        
        projected = projector(original_record)
        
        # Should return the original record unchanged
        assert projected == original_record
        assert projected is original_record  # Should be the same object

    def test_projection_with_empty_fields(self):
        """Test projection with empty field set returns original record"""
        projector = _project(set())
        
        record = {"name": "Bob Wilson", "age": 35, "department": "marketing"}
        projected = projector(record)
        
        # Should return original record when no fields specified
        assert projected == record

    def test_aggregation_with_missing_group_field(self):
        """Test aggregation when group field is missing from some records"""
        aggregation = {}
        
        records = [
            {"department": "eng", "salary": 80000},
            {"salary": 70000},  # Missing department field
            {"department": "sales", "salary": 75000},
            {"name": "John", "salary": 60000},  # Missing department field
            {"department": "eng", "salary": 90000},
        ]
        
        for record in records:
            _aggregate_in_place(aggregation, record, "department", "salary")
        
        # Should only aggregate records that have the group field
        expected = {"eng": 170000.0, "sales": 75000.0}
        assert aggregation == expected

    def test_aggregation_with_non_numeric_sum_field(self):
        """Test aggregation when sum field contains non-numeric values"""
        aggregation = {}
        
        records = [
            {"department": "eng", "score": 85},
            {"department": "eng", "score": "invalid"},  # Non-numeric
            {"department": "sales", "score": 92},
            {"department": "eng", "score": 88},
            {"department": "sales", "score": None},  # None value
        ]
        
        for record in records:
            _aggregate_in_place(aggregation, record, "department", "score")
        
        # Should only sum numeric values
        expected = {"eng": 173.0, "sales": 92.0}
        assert aggregation == expected

    def test_count_aggregation_without_sum_field(self):
        """Test count aggregation (no sum field specified)"""
        aggregation = {}
        
        records = [
            {"category": "A"},
            {"category": "B"},
            {"category": "A"},
            {"category": "C"},
            {"category": "A"},
            {"category": "B"},
        ]
        
        for record in records:
            _aggregate_in_place(aggregation, record, "category", None)
        
        # Should count occurrences
        expected = {"A": 3.0, "B": 2.0, "C": 1.0}
        assert aggregation == expected

    def test_nested_record_extraction_complex(self):
        """Test complex nested record extraction"""
        complex_payload = {
            "top_level": {"id": "root", "type": "root"},
            "events": [
                {"id": "event1", "type": "event"},
                {"id": "event2", "type": "event"}
            ],
            "data": {
                "items": [
                    {"id": "item1", "type": "item"},
                    {"id": "item2", "type": "item"}
                ],
                "records": [
                    {"id": "record1", "type": "record"}
                ]
            },
            "rows": [
                {"id": "row1", "type": "row"}
            ]
        }
        
        extracted_records = list(_iter_records(complex_payload))
        
        # Should extract from all levels and collections
        ids = [record.get("id") for record in extracted_records if "id" in record]
        
        # Should find records from all nested locations (no "root" id in top-level)
        assert "event1" in ids and "event2" in ids  # events array
        assert "item1" in ids and "item2" in ids  # data.items
        assert "record1" in ids  # data.records
        assert "row1" in ids  # rows array
        
        # Verify we get the expected number of records total
        assert len(extracted_records) >= 7  # top-level + events + data + items + records + rows

    def test_projection_preserves_data_types(self):
        """Test that projection preserves original data types"""
        projector = _project({"count", "active", "score", "tags"})
        
        record = {
            "count": 42,
            "active": True,
            "score": 95.5,
            "tags": ["python", "fastapi"],
            "metadata": {"ignored": "field"}
        }
        
        projected = projector(record)
        
        # Should preserve data types
        assert isinstance(projected["count"], int)
        assert isinstance(projected["active"], bool)
        assert isinstance(projected["score"], float)
        assert isinstance(projected["tags"], list)
        
        # Should have exact values
        assert projected["count"] == 42
        assert projected["active"] is True
        assert projected["score"] == 95.5
        assert projected["tags"] == ["python", "fastapi"]

    def test_large_scale_iteration_efficiency(self):
        """Test iteration efficiency with large datasets"""
        # Create large nested structure
        large_structure = {
            "events": [{"batch": 1, "id": i, "value": i * 2} for i in range(1000)],
            "data": {
                "records": [{"batch": 2, "id": i + 1000, "value": i * 3} for i in range(500)],
                "items": [{"batch": 3, "id": i + 1500, "value": i * 4} for i in range(300)]
            }
        }
        
        # Use generator to process without loading all into memory
        total_value = 0
        record_count = 0
        
        for record in _iter_records(large_structure):
            if "value" in record:
                total_value += record["value"]
                record_count += 1
        
        # Should process all records with value field (not including top-level dict)
        assert record_count == 1800  # 1000 events + 500 records + 300 items
        
        # Verify some records were processed
        assert total_value > 0

    def test_aggregation_memory_efficiency(self):
        """Test that aggregation doesn't create unnecessary intermediate objects"""
        # Start with empty aggregation
        aggregation = {}
        
        # Process many records efficiently
        for i in range(10000):
            record = {
                "group": f"group_{i % 100}",  # 100 unique groups
                "value": i
            }
            _aggregate_in_place(aggregation, record, "group", "value")
        
        # Should have exactly 100 groups
        assert len(aggregation) == 100
        
        # Verify aggregation correctness for first group
        # group_0 should have values: 0, 100, 200, ..., 9900 (100 values)
        expected_group_0_sum = sum(range(0, 10000, 100))
        assert aggregation["group_0"] == float(expected_group_0_sum)
