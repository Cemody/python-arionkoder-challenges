import pytest
from lazy import LazyCollection


class TestComposability:
    """Test operation composability and method chaining"""
    
    def test_method_chaining(self):
        """Test that methods can be chained together"""
        result = (
            LazyCollection(range(20))
            .map(lambda x: x * 2)
            .filter(lambda x: x > 10)
            .skip(3)
            .take(5)
            .to_list()
        )
        
        expected = [18, 20, 22, 24, 26]
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_multiple_maps(self):
        """Test composing multiple map operations"""
        result = (
            LazyCollection([1, 2, 3, 4, 5])
            .map(lambda x: x * 2)
            .map(lambda x: x + 1)
            .map(lambda x: x * 3)
            .to_list()
        )
        
        # First map: [2, 4, 6, 8, 10]
        # Second map: [3, 5, 7, 9, 11] 
        # Third map: [9, 15, 21, 27, 33]
        expected = [9, 15, 21, 27, 33]  # ((x*2)+1)*3
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_multiple_filters(self):
        """Test composing multiple filter operations"""
        result = (
            LazyCollection(range(20))
            .filter(lambda x: x % 2 == 0)  # Even numbers
            .filter(lambda x: x % 3 == 0)  # Divisible by 3
            .filter(lambda x: x > 5)       # Greater than 5
            .to_list()
        )
        
        expected = [6, 12, 18]  # Numbers that are even, divisible by 3, and > 5
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_skip_and_take_composition(self):
        """Test composing skip and take operations"""
        result = (
            LazyCollection(range(20))
            .skip(5)
            .take(10)
            .skip(2)
            .take(5)
            .to_list()
        )
        
        # Skip 5: [5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
        # Take 10: [5,6,7,8,9,10,11,12,13,14]
        # Skip 2: [7,8,9,10,11,12,13,14]
        # Take 5: [7,8,9,10,11]
        expected = [7, 8, 9, 10, 11]
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_complex_composition(self):
        """Test complex composition of all operations"""
        data = range(100)
        result = (
            LazyCollection(data)
            .map(lambda x: x * x)           # Square numbers
            .filter(lambda x: x % 4 == 0)   # Divisible by 4
            .skip(10)                       # Skip first 10
            .map(lambda x: x // 4)          # Divide by 4
            .filter(lambda x: x < 1000)     # Less than 1000
            .take(5)                        # Take 5
            .to_list()
        )
        
        assert len(result) == 5, f"Expected 5 results, got {len(result)}"
        assert all(isinstance(x, int) for x in result), "All results should be integers"
        assert all(x < 1000 for x in result), "All results should be < 1000"
    
    def test_composability_with_empty_results(self):
        """Test composability when intermediate operations produce empty results"""
        result = (
            LazyCollection([1, 2, 3, 4, 5])
            .filter(lambda x: x > 10)  # No matches
            .map(lambda x: x * 2)      # Should not be called
            .take(3)                   # Should get empty list
            .to_list()
        )
        
        assert result == [], f"Expected empty list, got {result}"
    
    def test_map_filter_map_chain(self):
        """Test alternating map and filter operations"""
        result = (
            LazyCollection(range(10))
            .map(lambda x: x * 2)          # [0,2,4,6,8,10,12,14,16,18]
            .filter(lambda x: x > 5)       # [6,8,10,12,14,16,18]
            .map(lambda x: x + 1)          # [7,9,11,13,15,17,19]
            .filter(lambda x: x % 3 == 0)  # [9,15]
            .to_list()
        )
        
        expected = [9, 15]
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_operation_order_matters(self):
        """Test that the order of operations affects the result"""
        data = range(10)
        
        # Filter then map - different example to show order matters
        result1 = (
            LazyCollection(data)
            .filter(lambda x: x > 5)       # Numbers > 5 first: [6,7,8,9]
            .map(lambda x: x * 2)          # Then multiply: [12,14,16,18]
            .to_list()
        )
        
        # Map then filter 
        result2 = (
            LazyCollection(data)
            .map(lambda x: x * 2)          # Multiply first: [0,2,4,6,8,10,12,14,16,18]
            .filter(lambda x: x > 5)       # Then filter > 5: [6,8,10,12,14,16,18]
            .to_list()
        )
        
        assert result1 != result2, "Different operation orders should yield different results"
        assert result1 == [12, 14, 16, 18], f"Result1 unexpected: {result1}"
        assert result2 == [6, 8, 10, 12, 14, 16, 18], f"Result2 unexpected: {result2}"

