import pytest
from lazy import LazyCollection


class TestReductions:
    """Test reduction operations (sum, count, etc.)"""
    
    def test_sum_reduction(self):
        """Test sum reduction operation"""
        result = LazyCollection(range(1, 6)).sum()  # 1+2+3+4+5
        assert result == 15, f"Expected 15, got {result}"
        
        # Test with transformations
        result = LazyCollection(range(5)).map(lambda x: x * 2).sum()  # 0+2+4+6+8
        assert result == 20, f"Expected 20, got {result}"
    
    def test_sum_with_filtering(self):
        """Test sum with filtering"""
        result = (
            LazyCollection(range(10))
            .filter(lambda x: x % 2 == 0)  # Even numbers: 0,2,4,6,8
            .sum()
        )
        assert result == 20, f"Expected 20, got {result}"
    
    def test_count_reduction(self):
        """Test count reduction operation"""
        result = LazyCollection(range(10)).count()
        assert result == 10, f"Expected 10, got {result}"
        
        # Test with filtering
        result = LazyCollection(range(20)).filter(lambda x: x % 3 == 0).count()
        # Numbers divisible by 3: 0,3,6,9,12,15,18 = 7 numbers
        assert result == 7, f"Expected 7, got {result}"
    
    def test_count_with_transformations(self):
        """Test count with various transformations"""
        result = (
            LazyCollection(range(100))
            .map(lambda x: x * x)
            .filter(lambda x: x > 50)
            .count()
        )
        # Squares > 50: 64,81,100,121,... (from 8² onwards)
        # 8² to 99² = 92 numbers
        assert result == 92, f"Expected 92, got {result}"
    
    def test_reduction_with_empty_collection(self):
        """Test reductions on empty collections"""
        empty_sum = LazyCollection([]).sum()
        assert empty_sum == 0, f"Expected 0 for empty sum, got {empty_sum}"
        
        empty_count = LazyCollection([]).count()
        assert empty_count == 0, f"Expected 0 for empty count, got {empty_count}"
        
        # Empty after filtering
        filtered_sum = LazyCollection([1, 2, 3]).filter(lambda x: x > 10).sum()
        assert filtered_sum == 0, f"Expected 0 for filtered empty sum, got {filtered_sum}"
    
    def test_reduction_with_single_item(self):
        """Test reductions on single-item collections"""
        single_sum = LazyCollection([42]).sum()
        assert single_sum == 42, f"Expected 42, got {single_sum}"
        
        single_count = LazyCollection([42]).count()
        assert single_count == 1, f"Expected 1, got {single_count}"
    
    def test_reduction_lazy_evaluation(self):
        """Test that reductions trigger lazy evaluation properly"""
        call_count = 0
        
        def track_calls(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # Create lazy collection
        lazy_col = LazyCollection(range(10)).map(track_calls)
        assert call_count == 0, "Should not execute during definition"
        
        # Perform reduction
        result = lazy_col.sum()
        assert call_count == 10, f"Should execute all 10 operations, got {call_count}"
        assert result == 90, f"Expected 90, got {result}"  # (0+2+4+6+8+10+12+14+16+18)
    
    def test_reduction_with_skip_take(self):
        """Test reductions combined with skip and take"""
        result = (
            LazyCollection(range(20))
            .skip(5)    # Skip first 5: [5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
            .take(10)   # Take 10: [5,6,7,8,9,10,11,12,13,14]
            .sum()
        )
        expected = sum(range(5, 15))  # 5+6+7+8+9+10+11+12+13+14 = 95
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_multiple_reductions_on_same_collection(self):
        """Test that multiple reductions work on the same lazy collection"""
        lazy_col = LazyCollection(range(1, 6))  # [1,2,3,4,5]
        
        sum_result = lazy_col.sum()
        count_result = lazy_col.count()
        
        assert sum_result == 15, f"Expected sum 15, got {sum_result}"
        assert count_result == 5, f"Expected count 5, got {count_result}"
    
    def test_reduction_memory_efficiency(self):
        """Test that reductions are memory efficient for large collections"""
        large_size = 100000
        
        # Should be able to sum large collection without storing all items
        result = LazyCollection(range(large_size)).sum()
        expected = sum(range(large_size))
        assert result == expected, f"Expected {expected}, got {result}"
        
        # Should be able to count large filtered collection
        count = (
            LazyCollection(range(large_size))
            .filter(lambda x: x % 1000 == 0)
            .count()
        )
        assert count == 100, f"Expected 100, got {count}"  # 0, 1000, 2000, ..., 99000
    
    def test_complex_reduction_pipeline(self):
        """Test complex pipeline ending with reduction"""
        result = (
            LazyCollection(range(100))
            .map(lambda x: x * x)           # Square numbers
            .filter(lambda x: x % 2 == 0)   # Even squares
            .skip(5)                        # Skip first 5 even squares
            .take(10)                       # Take next 10
            .map(lambda x: x // 4)          # Divide by 4
            .filter(lambda x: x > 10)       # Greater than 10
            .sum()                          # Sum the results
        )
        
        # Should execute the entire pipeline and return a sum
        assert isinstance(result, int), f"Result should be integer, got {type(result)}"
        assert result > 0, f"Result should be positive, got {result}"
