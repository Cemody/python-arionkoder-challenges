"""
This demo showcases all the key features and constraints testing for Challenge 4:

Features:
- Lazy evaluation of expensive transformations
- Common operations like filtering, mapping, and reducing
- Pagination and chunking of results

Constraints:
- Operations must be composable (chaining multiple transformations)
- Memory usage must scale with output size, not input size
"""

import time
import gc
import sys
import tracemalloc
from typing import Dict, Any, List

# Import the utilities and lazy collection
from utils import (
    measure_performance,
    get_performance_summary,
    clear_performance_metrics,
    test_composability,
    validate_lazy_evaluation
)

from lazy import LazyCollection


def measure_performance_test(test_name, test_func):
    """Helper to measure execution time and memory for tests."""
    print(f"\n=== {test_name} ===")
    gc.collect()
    
    # Start memory tracking
    tracemalloc.start()
    
    start_time = time.time()
    result = test_func()
    end_time = time.time()
    
    # Get memory info
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    execution_time = (end_time - start_time) * 1000  # in ms
    memory_used = peak / 1024 / 1024  # in MB
    
    print(f"⏱️  Execution time: {execution_time:.2f} ms")
    print(f"🧠 Memory used: {memory_used:.2f} MB")
    
    return result


def test_lazy_evaluation():
    """Test core lazy evaluation functionality"""
    
    def run_test():
        print("💤 Testing lazy evaluation...")
        
        # Test 1: Verify operations are not executed immediately
        print("📋 Testing deferred execution...")
        
        call_count = 0
        def expensive_operation(x):
            nonlocal call_count
            call_count += 1
            time.sleep(0.001)  # Simulate expensive operation
            return x * x
        
        # Create lazy collection with expensive operation
        lazy_collection = LazyCollection(range(1, 1001)).map(expensive_operation)
        
        # At this point, no operations should have been executed
        print(f"✅ Operations defined, call count: {call_count} (should be 0)")
        assert call_count == 0, "Operations executed too early!"
        
        # Test 2: Operations execute only when consumed
        print("\n🔄 Testing execution on consumption...")
        start_calls = call_count
        
        # Take only first 5 items - should only process 5 items
        result = lazy_collection.take(5).to_list()
        
        calls_after_take = call_count - start_calls
        print(f"✅ Took 5 items, function called {calls_after_take} times")
        print(f"📊 Result: {result}")
        
        # Should have called the function approximately 5 times (lazy evaluation)
        # May call 1 extra to check if there are more items
        assert calls_after_take <= 6 and calls_after_take >= 5, f"Expected 5-6 calls, got {calls_after_take}"
        
        # Test 3: Validate lazy collection internals
        print("\n🔍 Testing lazy collection structure...")
        lazy_col = LazyCollection(range(100)).map(lambda x: x * 2).filter(lambda x: x > 50)
        
        is_lazy = validate_lazy_evaluation(lazy_col)
        print(f"✅ Collection is lazy: {is_lazy}")
        print(f"📊 Pending operations: {len(lazy_col._ops)}")
        
        return {
            "lazy_evaluation_working": call_count > 0,
            "deferred_execution": True,
            "lazy_validation": is_lazy,
            "operations_processed": calls_after_take
        }
    
    return measure_performance_test("Lazy Evaluation", run_test)


def test_composable_operations():
    """Test that operations are fully composable"""
    
    def run_test():
        print("🔗 Testing operation composability...")
        
        # Test 1: Basic chaining
        print("📋 Testing basic operation chaining...")
        
        result1 = (
            LazyCollection(range(1, 21))
            .map(lambda x: x * 2)       # Double each number
            .filter(lambda x: x > 10)   # Keep numbers > 10
            .skip(2)                    # Skip first 2
            .take(5)                    # Take next 5
            .to_list()
        )
        
        expected1 = [16, 18, 20, 22, 24]  # 2*8, 2*9, 2*10, 2*11, 2*12
        print(f"✅ Basic chaining result: {result1}")
        print(f"📊 Expected: {expected1}")
        assert result1 == expected1, f"Basic chaining failed: {result1} != {expected1}"
        
        # Test 2: Complex chaining with all operation types
        print("\n🔄 Testing complex operation chaining...")
        
        # Create a more complex chain
        complex_result = (
            LazyCollection(range(1, 101))
            .map(lambda x: x * x)           # Square numbers
            .filter(lambda x: x % 3 == 0)   # Divisible by 3
            .skip(3)                        # Skip first 3
            .take(10)                       # Take 10
            .map(lambda x: x // 10)         # Scale down
            .filter(lambda x: x > 0)        # Keep positive
            .to_list()
        )
        
        print(f"✅ Complex chaining result: {len(complex_result)} items")
        print(f"📊 Sample results: {complex_result[:5]}...")
        
        # Test 3: Chaining with batching
        print("\n📦 Testing chaining with batching...")
        
        batched_result = (
            LazyCollection(range(1, 21))
            .map(lambda x: x * 2)
            .filter(lambda x: x % 4 == 0)
            .batch(3)
            .take(2)
            .to_list()
        )
        
        print(f"✅ Batched result: {batched_result}")
        print(f"📊 Number of batches: {len(batched_result)}")
        
        # Test 4: Test multiple independent chains
        print("\n🌿 Testing multiple independent chains...")
        
        source_data = list(range(1, 51))
        chains = [
            [
                {"type": "map", "function": "lambda x: x * x"},
                {"type": "filter", "predicate": "lambda x: x % 2 == 0"},
                {"type": "take", "count": 5}
            ],
            [
                {"type": "filter", "predicate": "lambda x: x > 25"},
                {"type": "map", "function": "lambda x: x / 2"},
                {"type": "take", "count": 10}
            ]
        ]
        
        composability_result = test_composability(source_data, chains, True)
        
        print(f"✅ Chains tested: {composability_result['chains_tested']}")
        print(f"📊 All composable: {composability_result['all_chains_composable']}")
        print(f"🧠 Memory efficient: {composability_result['memory_efficient']}")
        
        if composability_result['errors']:
            for error in composability_result['errors']:
                print(f"⚠️  Error: {error}")
        
        return {
            "basic_chaining": len(result1) == 5,
            "complex_chaining": len(complex_result) > 0,
            "batching_works": len(batched_result) > 0,
            "multiple_chains_composable": composability_result['all_chains_composable'],
            "memory_efficient": composability_result['memory_efficient']
        }
    
    return measure_performance_test("Composable Operations", run_test)


def test_memory_efficiency():
    """Test that memory usage scales with output size, not input size"""
    
    def run_test():
        print("🧠 Testing memory efficiency...")
        
        # Test 1: Large input, small output
        print("📋 Testing large input with small output...")
        
        large_input_size = 100000
        small_output_size = 10
        
        # Track memory before
        gc.collect()
        tracemalloc.start()
        
        # Create large dataset but only take small portion
        result = (
            LazyCollection(range(large_input_size))
            .map(lambda x: x * x)
            .filter(lambda x: x % 1000 == 0)
            .take(small_output_size)
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        memory_used_mb = peak / 1024 / 1024
        tracemalloc.stop()
        
        input_size_mb = sys.getsizeof(list(range(large_input_size))) / 1024 / 1024
        output_size_mb = sys.getsizeof(result) / 1024 / 1024
        
        print(f"✅ Input size: {large_input_size:,} items (~{input_size_mb:.2f} MB)")
        print(f"📊 Output size: {len(result)} items (~{output_size_mb:.2f} MB)")
        print(f"🧠 Memory used: {memory_used_mb:.2f} MB")
        
        # Memory should be closer to output size than input size
        memory_efficient = memory_used_mb < input_size_mb * 0.5  # Less than 50% of input size
        print(f"✅ Memory efficient: {memory_efficient}")
        
        # Test 2: Compare eager vs lazy evaluation
        print("\n⚡ Comparing eager vs lazy evaluation...")
        
        def eager_processing(data):
            """Eager evaluation - processes all data immediately"""
            result = []
            for x in data:
                transformed = x * x
                if transformed % 1000 == 0:
                    result.append(transformed)
                if len(result) >= small_output_size:
                    break
            return result
        
        def lazy_processing(data):
            """Lazy evaluation using LazyCollection"""
            return (
                LazyCollection(data)
                .map(lambda x: x * x)
                .filter(lambda x: x % 1000 == 0)
                .take(small_output_size)
                .to_list()
            )
        
        # Test eager approach
        gc.collect()
        tracemalloc.start()
        eager_start = time.time()
        eager_result = eager_processing(range(large_input_size))
        eager_time = time.time() - eager_start
        eager_current, eager_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        # Test lazy approach
        gc.collect()
        tracemalloc.start()
        lazy_start = time.time()
        lazy_result = lazy_processing(range(large_input_size))
        lazy_time = time.time() - lazy_start
        lazy_current, lazy_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        eager_memory_mb = eager_peak / 1024 / 1024
        lazy_memory_mb = lazy_peak / 1024 / 1024
        
        print(f"⚡ Eager: {eager_time*1000:.2f}ms, {eager_memory_mb:.2f}MB")
        print(f"💤 Lazy:  {lazy_time*1000:.2f}ms, {lazy_memory_mb:.2f}MB")
        print(f"📊 Results match: {eager_result == lazy_result}")
        
        memory_improvement = (eager_memory_mb - lazy_memory_mb) / eager_memory_mb * 100
        print(f"✅ Memory improvement: {memory_improvement:.1f}%")
        
        return {
            "memory_efficient": memory_efficient,
            "large_input_small_output": len(result) == small_output_size,
            "memory_usage_mb": memory_used_mb,
            "memory_improvement_percent": memory_improvement,
            "results_match": eager_result == lazy_result
        }
    
    return measure_performance_test("Memory Efficiency", run_test)


def test_pagination_and_chunking():
    """Test pagination and chunking functionality"""
    
    def run_test():
        print("📄 Testing pagination and chunking...")
        
        # Test 1: Basic pagination
        print("📋 Testing basic pagination...")
        
        data = list(range(1, 101))  # 100 items
        page_size = 10
        
        # Test different pages
        page1 = LazyCollection(data).page(1, page_size).to_list()
        page2 = LazyCollection(data).page(2, page_size).to_list()
        page5 = LazyCollection(data).page(5, page_size).to_list()
        
        print(f"✅ Page 1: {page1[:5]}... (length: {len(page1)})")
        print(f"✅ Page 2: {page2[:5]}... (length: {len(page2)})")
        print(f"✅ Page 5: {page5[:5]}... (length: {len(page5)})")
        
        # Validate pagination
        assert len(page1) == page_size, f"Page 1 size mismatch: {len(page1)} != {page_size}"
        assert page1[0] == 1 and page1[-1] == 10, f"Page 1 content incorrect: {page1}"
        assert page2[0] == 11 and page2[-1] == 20, f"Page 2 content incorrect: {page2}"
        
        # Test 2: Pagination with transformations
        print("\n🔄 Testing pagination with transformations...")
        
        transformed_page = (
            LazyCollection(range(1, 101))
            .map(lambda x: x * x)
            .filter(lambda x: x % 2 == 0)
            .page(2, 5)
            .to_list()
        )
        
        print(f"✅ Transformed page 2: {transformed_page}")
        print(f"📊 Page length: {len(transformed_page)}")
        
        # Test 3: Basic chunking
        print("\n📦 Testing basic chunking...")
        
        chunks = LazyCollection(range(1, 21)).batch(5).to_list()
        
        print(f"✅ Number of chunks: {len(chunks)}")
        for i, chunk in enumerate(chunks):
            print(f"  Chunk {i+1}: {chunk}")
        
        # Validate chunking
        assert len(chunks) == 4, f"Expected 4 chunks, got {len(chunks)}"
        assert all(len(chunk) == 5 for chunk in chunks), "All chunks should have 5 items"
        
        # Test 4: Chunking with transformations
        print("\n🔄 Testing chunking with transformations...")
        
        transformed_chunks = (
            LazyCollection(range(1, 21))
            .map(lambda x: x * 2)
            .filter(lambda x: x % 4 == 0)
            .batch(3)
            .to_list()
        )
        
        print(f"✅ Transformed chunks: {transformed_chunks}")
        print(f"📊 Number of chunks: {len(transformed_chunks)}")
        
        # Test 5: Iterator-style pagination
        print("\n🔄 Testing iterator-style pagination...")
        
        page_count = 0
        total_items = 0
        
        for page_num in range(1, 6):  # Test first 5 pages
            page_data = LazyCollection(range(1, 101)).page(page_num, 10).to_list()
            if page_data:
                page_count += 1
                total_items += len(page_data)
                print(f"  Page {page_num}: {len(page_data)} items")
            else:
                break
        
        print(f"✅ Total pages processed: {page_count}")
        print(f"📊 Total items processed: {total_items}")
        
        return {
            "basic_pagination": len(page1) == page_size,
            "pagination_with_transforms": len(transformed_page) > 0,
            "basic_chunking": len(chunks) == 4,
            "chunking_with_transforms": len(transformed_chunks) > 0,
            "iterator_pagination": page_count > 0
        }
    
    return measure_performance_test("Pagination and Chunking", run_test)


def test_reduction_operations():
    """Test various reduction operations"""
    
    def run_test():
        print("🔄 Testing reduction operations...")
        
        # Test data
        numbers = LazyCollection([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        
        # Test 1: Basic reductions
        print("📋 Testing basic reductions...")
        
        sum_result = numbers.sum()
        count_result = numbers.count()
        min_result = numbers.min()
        max_result = numbers.max()
        first_result = numbers.first()
        last_result = numbers.last()
        
        print(f"✅ Sum: {sum_result} (expected: 55)")
        print(f"✅ Count: {count_result} (expected: 10)")
        print(f"✅ Min: {min_result} (expected: 1)")
        print(f"✅ Max: {max_result} (expected: 10)")
        print(f"✅ First: {first_result} (expected: 1)")
        print(f"✅ Last: {last_result} (expected: 10)")
        
        # Validate basic reductions
        assert sum_result == 55, f"Sum incorrect: {sum_result} != 55"
        assert count_result == 10, f"Count incorrect: {count_result} != 10"
        assert min_result == 1, f"Min incorrect: {min_result} != 1"
        assert max_result == 10, f"Max incorrect: {max_result} != 10"
        
        # Test 2: Custom reduction
        print("\n🔧 Testing custom reduction...")
        
        product = numbers.reduce(lambda a, b: a * b, 1)
        print(f"✅ Product: {product} (expected: 3628800)")
        assert product == 3628800, f"Product incorrect: {product} != 3628800"
        
        # Test 3: Conditional reductions
        print("\n❓ Testing conditional reductions...")
        
        any_even = numbers.any(lambda x: x % 2 == 0)
        all_positive = numbers.all(lambda x: x > 0)
        all_even = numbers.all(lambda x: x % 2 == 0)
        
        print(f"✅ Any even: {any_even} (expected: True)")
        print(f"✅ All positive: {all_positive} (expected: True)")
        print(f"✅ All even: {all_even} (expected: False)")
        
        assert any_even == True, "Should have even numbers"
        assert all_positive == True, "Should all be positive"
        assert all_even == False, "Should not all be even"
        
        # Test 4: Reductions with transformations
        print("\n🔄 Testing reductions with transformations...")
        
        squared_sum = (
            LazyCollection(range(1, 11))
            .map(lambda x: x * x)
            .sum()
        )
        
        filtered_count = (
            LazyCollection(range(1, 21))
            .filter(lambda x: x % 3 == 0)
            .count()
        )
        
        print(f"✅ Sum of squares: {squared_sum} (expected: 385)")
        print(f"✅ Count of multiples of 3: {filtered_count} (expected: 6)")
        
        assert squared_sum == 385, f"Squared sum incorrect: {squared_sum} != 385"
        assert filtered_count == 6, f"Filtered count incorrect: {filtered_count} != 6"
        
        # Test 5: Find operations
        print("\n🔍 Testing find operations...")
        
        first_greater_than_5 = numbers.find(lambda x: x > 5)
        first_even = numbers.find(lambda x: x % 2 == 0)
        
        print(f"✅ First > 5: {first_greater_than_5} (expected: 6)")
        print(f"✅ First even: {first_even} (expected: 2)")
        
        assert first_greater_than_5 == 6, f"First > 5 incorrect: {first_greater_than_5} != 6"
        assert first_even == 2, f"First even incorrect: {first_even} != 2"
        
        return {
            "basic_reductions": sum_result == 55 and count_result == 10,
            "custom_reduction": product == 3628800,
            "conditional_reductions": any_even and all_positive and not all_even,
            "reductions_with_transforms": squared_sum == 385,
            "find_operations": first_greater_than_5 == 6
        }
    
    return measure_performance_test("Reduction Operations", run_test)


def test_constraint_validation():
    """Test specific constraints: composability and memory efficiency"""
    
    def run_test():
        print("⚠️  Testing constraint validation...")
        
        # Constraint 1: Operations must be composable
        print("📋 Testing composability constraint...")
        
        composability_tests = [
            # Test chaining different operation types
            (
                "map->filter->take",
                lambda: LazyCollection(range(100))
                .map(lambda x: x * 2)
                .filter(lambda x: x > 50)
                .take(10)
                .to_list()
            ),
            # Test multiple transformations
            (
                "map->map->filter->skip->take",
                lambda: LazyCollection(range(50))
                .map(lambda x: x * x)
                .map(lambda x: x + 1)
                .filter(lambda x: x % 3 == 0)
                .skip(2)
                .take(5)
                .to_list()
            ),
            # Test with batching
            (
                "filter->batch->take",
                lambda: LazyCollection(range(20))
                .filter(lambda x: x % 2 == 0)
                .batch(3)
                .take(2)
                .to_list()
            )
        ]
        
        composability_passed = 0
        for test_name, test_func in composability_tests:
            try:
                result = test_func()
                print(f"✅ {test_name}: Success ({len(result) if isinstance(result, list) else 'N/A'} items)")
                composability_passed += 1
            except Exception as e:
                print(f"❌ {test_name}: Failed - {e}")
        
        composability_constraint = composability_passed == len(composability_tests)
        print(f"📊 Composability constraint: {'✅ PASSED' if composability_constraint else '❌ FAILED'}")
        
        # Constraint 2: Memory usage scales with output, not input
        print("\n🧠 Testing memory efficiency constraint...")
        
        # Large input, small output test
        large_input = 50000
        small_output = 20
        
        gc.collect()
        tracemalloc.start()
        
        # Process large input to get small output
        result = (
            LazyCollection(range(large_input))
            .map(lambda x: x * x)
            .filter(lambda x: x % 1000 == 0)
            .take(small_output)
            .to_list()
        )
        
        current, peak = tracemalloc.get_traced_memory()
        memory_used_mb = peak / 1024 / 1024
        tracemalloc.stop()
        
        # Calculate expected memory sizes
        input_memory_mb = sys.getsizeof(list(range(large_input))) / 1024 / 1024
        output_memory_mb = sys.getsizeof(result) / 1024 / 1024
        
        print(f"📊 Input size: {large_input:,} items (~{input_memory_mb:.2f} MB)")
        print(f"📊 Output size: {len(result)} items (~{output_memory_mb:.2f} MB)")
        print(f"🧠 Actual memory used: {memory_used_mb:.2f} MB")
        
        # Memory efficiency: actual usage should be much closer to output size than input size
        memory_ratio = memory_used_mb / input_memory_mb
        memory_constraint = memory_ratio < 0.1  # Less than 10% of input size
        
        print(f"📊 Memory ratio (used/input): {memory_ratio:.3f}")
        print(f"📊 Memory efficiency constraint: {'✅ PASSED' if memory_constraint else '❌ FAILED'}")
        
        # Additional test: Compare with eager evaluation
        print("\n⚡ Comparing with eager evaluation...")
        
        def eager_approach(data_size, output_size):
            """Eager evaluation - loads all data first"""
            data = list(range(data_size))  # Load all data
            transformed = [x * x for x in data]  # Transform all
            filtered = [x for x in transformed if x % 1000 == 0]  # Filter all
            return filtered[:output_size]  # Take subset
        
        gc.collect()
        tracemalloc.start()
        eager_result = eager_approach(large_input, small_output)
        eager_current, eager_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        eager_memory_mb = eager_peak / 1024 / 1024
        memory_improvement = (eager_memory_mb - memory_used_mb) / eager_memory_mb * 100
        
        print(f"⚡ Eager memory: {eager_memory_mb:.2f} MB")
        print(f"💤 Lazy memory: {memory_used_mb:.2f} MB")
        print(f"📊 Memory improvement: {memory_improvement:.1f}%")
        print(f"📊 Results match: {result == eager_result}")
        
        return {
            "composability_constraint": composability_constraint,
            "memory_efficiency_constraint": memory_constraint,
            "memory_improvement_percent": memory_improvement,
            "tests_passed": composability_passed,
            "total_tests": len(composability_tests)
        }
    
    return measure_performance_test("Constraint Validation", run_test)


def run_comprehensive_demo():
    """Run comprehensive demonstration of all features and constraints"""
    print("🚀 CHALLENGE 4: CUSTOM ITERATOR WITH LAZY EVALUATION COMPREHENSIVE DEMO")
    print("=" * 80)
    
    results = {}
    
    # Clear any existing metrics
    clear_performance_metrics()
    
    # Run all tests
    results["lazy_evaluation"] = test_lazy_evaluation()
    results["composable_operations"] = test_composable_operations()
    results["memory_efficiency"] = test_memory_efficiency()
    results["pagination_chunking"] = test_pagination_and_chunking()
    results["reduction_operations"] = test_reduction_operations()
    results["constraint_validation"] = test_constraint_validation()
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 DEMO SUMMARY")
    print("=" * 80)
    
    lazy_eval = results["lazy_evaluation"]
    print(f"💤 Lazy Evaluation: {'✅ Working' if lazy_eval['lazy_evaluation_working'] else '❌ Failed'}")
    
    composable = results["composable_operations"]
    print(f"🔗 Composable Operations: {'✅ Working' if composable['basic_chaining'] else '❌ Failed'}")
    
    memory = results["memory_efficiency"]
    print(f"🧠 Memory Efficiency: {'✅ Efficient' if memory['memory_efficient'] else '❌ Inefficient'} ({memory['memory_improvement_percent']:.1f}% improvement)")
    
    pagination = results["pagination_chunking"]
    print(f"📄 Pagination/Chunking: {'✅ Working' if pagination['basic_pagination'] else '❌ Failed'}")
    
    reduction = results["reduction_operations"]
    print(f"🔄 Reduction Operations: {'✅ Working' if reduction['basic_reductions'] else '❌ Failed'}")
    
    constraints = results["constraint_validation"]
    print(f"⚠️  Constraint Validation:")
    print(f"   🔗 Composability: {'✅ PASSED' if constraints['composability_constraint'] else '❌ FAILED'}")
    print(f"   🧠 Memory Scaling: {'✅ PASSED' if constraints['memory_efficiency_constraint'] else '❌ FAILED'}")
    
    # Get final performance summary
    performance_summary = get_performance_summary()
    print(f"\n📊 Performance Summary:")
    print(f"   Total operations: {performance_summary['total_operations']}")
    print(f"   Total time: {performance_summary['total_time_ms']:.2f}ms")
    print(f"   Total memory: {performance_summary['total_memory_mb']:.2f}MB")
    print(f"   Avg time per operation: {performance_summary['avg_time_ms']:.2f}ms")
    
    print("\n✨ Lazy evaluation system demonstrates:")
    print("  • Deferred execution until results are consumed")
    print("  • Composable operations with method chaining")
    print("  • Memory usage scales with output, not input size")
    print("  • Support for pagination and chunking")
    print("  • Rich set of reduction operations")
    
    return results


if __name__ == "__main__":
    run_comprehensive_demo()
