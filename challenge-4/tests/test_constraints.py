import pytest
from lazy import LazyCollection


class TestConstraints:
    """Test system constraints and edge cases"""
    
    def test_negative_skip_constraint(self):
        """Test behavior with negative skip values"""
        # Should treat negative skip as 0
        result = LazyCollection(range(5)).skip(-1).to_list()
        assert result == [0, 1, 2, 3, 4], f"Negative skip should be treated as 0, got {result}"
        
        result = LazyCollection(range(5)).skip(-10).take(3).to_list()
        assert result == [0, 1, 2], f"Large negative skip should be treated as 0, got {result}"
    
    def test_negative_take_constraint(self):
        """Test behavior with negative take values"""
        # Should return empty list for negative take
        result = LazyCollection(range(5)).take(-1).to_list()
        assert result == [], f"Negative take should return empty list, got {result}"
        
        result = LazyCollection(range(5)).take(-10).to_list()
        assert result == [], f"Large negative take should return empty list, got {result}"
    
    def test_zero_take_constraint(self):
        """Test behavior with zero take value"""
        result = LazyCollection(range(5)).take(0).to_list()
        assert result == [], f"Zero take should return empty list, got {result}"
    
    def test_large_skip_constraint(self):
        """Test behavior when skip is larger than collection size"""
        result = LazyCollection(range(5)).skip(10).to_list()
        assert result == [], f"Skip larger than collection should return empty list, got {result}"
        
        result = LazyCollection(range(5)).skip(100).take(3).to_list()
        assert result == [], f"Large skip should return empty list, got {result}"
    
    def test_large_take_constraint(self):
        """Test behavior when take is larger than available items"""
        result = LazyCollection(range(5)).take(10).to_list()
        assert result == [0, 1, 2, 3, 4], f"Take larger than collection should return all items, got {result}"
        
        result = LazyCollection(range(5)).skip(2).take(10).to_list()
        assert result == [2, 3, 4], f"Take larger than remaining should return all remaining, got {result}"
    
    def test_empty_collection_constraint(self):
        """Test behavior with empty collections"""
        empty_lazy = LazyCollection([])
        
        assert empty_lazy.to_list() == [], "Empty collection should return empty list"
        assert empty_lazy.count() == 0, "Empty collection count should be 0"
        assert empty_lazy.sum() == 0, "Empty collection sum should be 0"
        
        # Operations on empty collections
        result = empty_lazy.map(lambda x: x * 2).to_list()
        assert result == [], f"Map on empty should return empty, got {result}"
        
        result = empty_lazy.filter(lambda x: True).to_list()
        assert result == [], f"Filter on empty should return empty, got {result}"
        
        result = empty_lazy.skip(5).take(3).to_list()
        assert result == [], f"Skip/take on empty should return empty, got {result}"
    
    def test_none_values_constraint(self):
        """Test behavior with None values in collection"""
        data_with_none = [1, None, 3, None, 5]
        
        # Should handle None values in basic operations
        result = LazyCollection(data_with_none).to_list()
        assert result == [1, None, 3, None, 5], f"Should preserve None values, got {result}"
        
        # Filter out None values
        result = LazyCollection(data_with_none).filter(lambda x: x is not None).to_list()
        assert result == [1, 3, 5], f"Should filter out None values, got {result}"
        
        # Count including None
        count = LazyCollection(data_with_none).count()
        assert count == 5, f"Should count None values, got {count}"
    
    def test_function_exception_constraint(self):
        """Test behavior when map/filter functions raise exceptions"""
        def failing_function(x):
            if x == 3:
                raise ValueError("Test exception")
            return x * 2
        
        # Should propagate exceptions during evaluation
        lazy_col = LazyCollection(range(5)).map(failing_function)
        
        with pytest.raises(ValueError, match="Test exception"):
            lazy_col.to_list()
    
    def test_iterator_exhaustion_constraint(self):
        """Test behavior with exhausted iterators"""
        def limited_generator():
            yield 1
            yield 2
            yield 3
        
        lazy_col = LazyCollection(limited_generator())
        
        # First consumption
        result1 = lazy_col.to_list()
        assert result1 == [1, 2, 3], f"First consumption failed: {result1}"
        
        # Second consumption will be empty because generator is exhausted
        # This is the current behavior - generator functions should be wrapped
        # in a factory function if multiple consumption is needed
        result2 = lazy_col.to_list()
        assert result2 == [], f"Second consumption should be empty due to generator exhaustion: {result2}"
    
    def test_infinite_sequence_constraint(self):
        """Test behavior with potentially infinite sequences"""
        def infinite_counter():
            i = 0
            while True:
                yield i
                i += 1
        
        # Should handle infinite sequences with take
        result = LazyCollection(infinite_counter()).take(5).to_list()
        assert result == [0, 1, 2, 3, 4], f"Infinite sequence with take failed: {result}"
        
        # Should handle infinite sequences with filter and take
        result = (
            LazyCollection(infinite_counter())
            .filter(lambda x: x % 2 == 0)
            .take(5)
            .to_list()
        )
        assert result == [0, 2, 4, 6, 8], f"Infinite sequence with filter failed: {result}"
    
    def test_type_consistency_constraint(self):
        """Test type consistency in operations"""
        # Mixed types should be handled
        mixed_data = [1, 2.5, "3", 4]
        
        result = LazyCollection(mixed_data).to_list()
        assert result == [1, 2.5, "3", 4], f"Should handle mixed types: {result}"
        
        # Type-specific operations
        numbers = LazyCollection([1, 2, 3, 4, 5])
        sum_result = numbers.sum()
        assert sum_result == 15, f"Sum should work with numbers: {sum_result}"
        assert isinstance(sum_result, int), f"Sum should return int: {type(sum_result)}"
    
    def test_memory_constraint_with_large_objects(self):
        """Test memory constraints with large objects"""
        def create_large_dict(x):
            return {"id": x, "data": [0] * 1000}  # Large object
        
        # Should handle large objects efficiently with take
        result = (
            LazyCollection(range(1000))
            .map(create_large_dict)
            .take(3)
            .to_list()
        )
        
        assert len(result) == 3, f"Expected 3 results, got {len(result)}"
        assert all("id" in item and "data" in item for item in result), "All items should have expected structure"
        assert result[0]["id"] == 0, f"First item ID should be 0, got {result[0]['id']}"

