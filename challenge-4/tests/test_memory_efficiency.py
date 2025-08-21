import pytest
import gc
import tracemalloc
from lazy import LazyCollection


class TestMemoryEfficiency:
    """Test memory efficiency of lazy evaluation"""
    
    def test_memory_scales_with_output_not_input(self):
        """Test that memory usage scales with output size, not input size"""
        large_input_size = 100000
        small_output_size = 10
        
        # Create large input but take small output
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        
        result = (
            LazyCollection(range(large_input_size))
            .map(lambda x: x * x)
            .filter(lambda x: x % 1000 == 0)
            .take(small_output_size)
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        memory_used = peak - baseline
        
        # Should use reasonable memory regardless of large input
        assert len(result) == small_output_size, f"Expected {small_output_size} results"
        assert memory_used < 50000000, f"Used too much memory: {memory_used} bytes"  # 50MB limit
    
    def test_no_intermediate_collection_storage(self):
        """Test that intermediate results are not stored in memory"""
        def memory_intensive_operation(x):
            # Simulate memory-intensive operation
            return [x] * 1000  # Create large list per item
        
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        
        # Process with lazy evaluation - should not store all intermediate results
        result = (
            LazyCollection(range(100))
            .map(memory_intensive_operation)
            .take(5)  # Only take 5 results
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        memory_used = peak - baseline
        
        assert len(result) == 5, f"Expected 5 results, got {len(result)}"
        # Should not store all 100 intermediate results
        assert memory_used < 10000000, f"Used too much memory: {memory_used} bytes"  # 10MB limit
    
    def test_memory_efficiency_with_large_skip(self):
        """Test memory efficiency when skipping large amounts of data"""
        large_skip = 50000
        
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        
        result = (
            LazyCollection(range(large_skip + 10))
            .map(lambda x: x * x)
            .skip(large_skip)
            .take(5)
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        memory_used = peak - baseline
        
        assert len(result) == 5, f"Expected 5 results, got {len(result)}"
        # Should not store skipped items
        assert memory_used < 5000000, f"Used too much memory: {memory_used} bytes"  # 5MB limit
    
    def test_garbage_collection_of_processed_items(self):
        """Test that processed items can be garbage collected"""
        def create_large_object(x):
            return {"data": [x] * 10000, "value": x}
        
        # Force garbage collection before test
        gc.collect()
        initial_objects = len(gc.get_objects())
        
        # Process items one by one - should allow GC of previous items
        result = (
            LazyCollection(range(20))
            .map(create_large_object)
            .map(lambda obj: obj["value"])  # Extract just the value
            .take(5)
            .to_list()
        )
        
        # Force garbage collection after processing
        gc.collect()
        final_objects = len(gc.get_objects())
        
        assert result == [0, 1, 2, 3, 4], f"Unexpected result: {result}"
        # Should not have accumulated many objects
        object_growth = final_objects - initial_objects
        assert object_growth < 1000, f"Too many objects accumulated: {object_growth}"
    
    def test_iterator_memory_efficiency(self):
        """Test that iterator approach is memory efficient"""
        def generate_data():
            """Generator that yields data without storing it all"""
            for i in range(100000):
                yield {"id": i, "data": f"item_{i}", "payload": [i] * 100}
        
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        
        # Use generator as source - should not load all data
        result = (
            LazyCollection(generate_data())
            .map(lambda item: item["id"])
            .filter(lambda id: id % 1000 == 0)
            .take(5)
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        memory_used = peak - baseline
        
        assert result == [0, 1000, 2000, 3000, 4000], f"Unexpected result: {result}"
        # Should use minimal memory despite large generator
        assert memory_used < 20000000, f"Used too much memory: {memory_used} bytes"  # 20MB limit
    
    def test_memory_with_complex_operations(self):
        """Test memory efficiency with complex operation chains"""
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        
        result = (
            LazyCollection(range(50000))
            .map(lambda x: {"value": x, "square": x * x})
            .filter(lambda item: item["square"] % 100 == 0)
            .map(lambda item: item["value"])
            .skip(10)
            .take(10)
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        memory_used = peak - baseline
        
        assert len(result) == 10, f"Expected 10 results, got {len(result)}"
        # Should use reasonable memory for complex operations
        assert memory_used < 30000000, f"Used too much memory: {memory_used} bytes"  # 30MB limit
