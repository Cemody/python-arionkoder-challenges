import pytest
import time
from lazy import LazyCollection


class TestLazyEvaluation:
    """Test core lazy evaluation functionality"""
    
    def test_deferred_execution(self):
        """Test that operations are not executed immediately"""
        call_count = 0
        
        def track_calls(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # Create lazy collection - should not execute yet
        lazy_col = LazyCollection(range(10)).map(track_calls)
        assert call_count == 0, "Operations should not execute during definition"
        
        # Take only first 3 items
        result = lazy_col.take(3).to_list()
        # May call 1 extra function to check for more items (generator behavior)
        assert call_count <= 4 and call_count >= 3, f"Expected 3-4 calls, got {call_count}"
        assert result == [0, 2, 4], f"Unexpected result: {result}"
    
    def test_lazy_chaining(self):
        """Test that chained operations remain lazy"""
        lazy_col = (
            LazyCollection(range(100))
            .map(lambda x: x * x)
            .filter(lambda x: x % 2 == 0)
            .skip(5)
            .take(10)
        )
        
        # Should still be lazy
        assert hasattr(lazy_col, '_ops'), "Should maintain operation queue"
        assert len(lazy_col._ops) > 0, "Should have pending operations"
        
        # Execute and verify
        result = lazy_col.to_list()
        assert len(result) == 10, f"Expected 10 items, got {len(result)}"
    
    def test_multiple_consumption(self):
        """Test that lazy collections can be consumed multiple times"""
        lazy_col = LazyCollection(range(5)).map(lambda x: x * 2)
        
        # First consumption
        result1 = lazy_col.to_list()
        # Second consumption
        result2 = lazy_col.to_list()
        
        assert result1 == result2, "Multiple consumptions should yield same result"
        assert result1 == [0, 2, 4, 6, 8], f"Unexpected result: {result1}"
    
    def test_lazy_evaluation_with_side_effects(self):
        """Test that side effects only occur when operations are executed"""
        side_effects = []
        
        def side_effect_map(x):
            side_effects.append(f"processed {x}")
            return x * 2
        
        # Create lazy collection with side effects
        lazy_col = LazyCollection([1, 2, 3, 4, 5]).map(side_effect_map)
        
        # No side effects should have occurred yet
        assert len(side_effects) == 0, "Side effects should not occur during definition"
        
        # Take only 2 items
        result = lazy_col.take(2).to_list()
        
        # Only 2-3 side effects should have occurred (may check one extra)
        assert len(side_effects) <= 3, f"Too many side effects: {side_effects}"
        assert len(side_effects) >= 2, f"Too few side effects: {side_effects}"
        assert result == [2, 4], f"Unexpected result: {result}"
    
    def test_lazy_evaluation_performance(self):
        """Test that lazy evaluation improves performance for small outputs"""
        large_size = 100000
        small_output = 5
        
        # Measure time for lazy evaluation
        start_time = time.perf_counter()
        result = (
            LazyCollection(range(large_size))
            .map(lambda x: x * x)  # Expensive operation
            .filter(lambda x: x % 1000 == 0)
            .take(small_output)
            .to_list()
        )
        lazy_time = time.perf_counter() - start_time
        
        # Should complete quickly and return correct results
        assert len(result) == small_output, f"Expected {small_output} results, got {len(result)}"
        assert lazy_time < 1.0, f"Lazy evaluation took too long: {lazy_time:.2f}s"
