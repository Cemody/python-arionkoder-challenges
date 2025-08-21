"""
Utility functions for Challenge 4: Custom Iterator with Lazy Evaluation

This module provides helper functions for testing and measuring performance
of lazy evaluation operations.
"""

import time
import gc
import tracemalloc
from typing import List, Dict, Any, Optional
from lazy import LazyCollection


# Global performance tracking
_performance_metrics = {
    "operations": [],
    "total_time_ms": 0.0,
    "total_memory_mb": 0.0,
    "operation_count": 0
}


def measure_performance(operation_name: str, func, *args, **kwargs) -> Dict[str, Any]:
    """Measure performance of a function call with memory tracking"""
    
    # Start memory tracking
    tracemalloc.start()
    gc.collect()
    
    # Start timing
    start_time = time.perf_counter()
    
    try:
        # Execute function
        result = func(*args, **kwargs)
        
        # End timing
        end_time = time.perf_counter()
        execution_time_ms = (end_time - start_time) * 1000
        
        # Get memory info
        current, peak = tracemalloc.get_traced_memory()
        memory_mb = peak / 1024 / 1024
        
        # Track metrics
        performance_info = {
            "operation": operation_name,
            "execution_time_ms": execution_time_ms,
            "memory_usage_mb": memory_mb,
            "success": True,
            "result_size": len(result) if hasattr(result, "__len__") else None,
            "timestamp": time.time()
        }
        
        # Add to global metrics
        _performance_metrics["operations"].append(performance_info)
        _performance_metrics["total_time_ms"] += execution_time_ms
        _performance_metrics["total_memory_mb"] += memory_mb
        _performance_metrics["operation_count"] += 1
        
        return performance_info
        
    except Exception as e:
        # End timing even on error
        end_time = time.perf_counter()
        execution_time_ms = (end_time - start_time) * 1000
        
        # Get memory info
        current, peak = tracemalloc.get_traced_memory()
        memory_mb = peak / 1024 / 1024
        
        performance_info = {
            "operation": operation_name,
            "execution_time_ms": execution_time_ms,
            "memory_usage_mb": memory_mb,
            "success": False,
            "error": str(e),
            "timestamp": time.time()
        }
        
        # Add to global metrics
        _performance_metrics["operations"].append(performance_info)
        _performance_metrics["total_time_ms"] += execution_time_ms
        _performance_metrics["total_memory_mb"] += memory_mb
        _performance_metrics["operation_count"] += 1
        
        raise e
        
    finally:
        tracemalloc.stop()


def get_performance_summary() -> Dict[str, Any]:
    """Get summary of all performance metrics"""
    if _performance_metrics["operation_count"] == 0:
        return {
            "total_operations": 0,
            "total_time_ms": 0.0,
            "total_memory_mb": 0.0,
            "avg_time_ms": 0.0,
            "avg_memory_mb": 0.0
        }
    
    return {
        "total_operations": _performance_metrics["operation_count"],
        "total_time_ms": _performance_metrics["total_time_ms"],
        "total_memory_mb": _performance_metrics["total_memory_mb"],
        "avg_time_ms": _performance_metrics["total_time_ms"] / _performance_metrics["operation_count"],
        "avg_memory_mb": _performance_metrics["total_memory_mb"] / _performance_metrics["operation_count"]
    }


def clear_performance_metrics():
    """Clear all performance metrics"""
    global _performance_metrics
    _performance_metrics = {
        "operations": [],
        "total_time_ms": 0.0,
        "total_memory_mb": 0.0,
        "operation_count": 0
    }


def validate_lazy_evaluation(lazy_collection: LazyCollection) -> bool:
    """Validate that a collection is properly lazy"""
    try:
        # Check if it has the lazy collection structure
        if not hasattr(lazy_collection, '_ops'):
            return False
        
        # Check if it has pending operations
        if not hasattr(lazy_collection, '_source'):
            return False
        
        # It's a lazy collection
        return True
        
    except Exception:
        return False


def test_composability(source_data: List[Any], operation_chains: List[List[Dict]], 
                      validate_memory: bool = True) -> Dict[str, Any]:
    """Test composability of operation chains"""
    
    results = {
        "chains_tested": 0,
        "all_chains_composable": True,
        "memory_efficient": True,
        "errors": [],
        "chain_results": []
    }
    
    for i, chain in enumerate(operation_chains):
        try:
            # Start with source data
            lazy_col = LazyCollection(source_data)
            
            # Apply operations in chain
            for op in chain:
                op_type = op.get("type")
                
                if op_type == "map":
                    func_str = op.get("function", "lambda x: x")
                    func = eval(func_str)
                    lazy_col = lazy_col.map(func)
                    
                elif op_type == "filter":
                    pred_str = op.get("predicate", "lambda x: True")
                    pred = eval(pred_str)
                    lazy_col = lazy_col.filter(pred)
                    
                elif op_type == "take":
                    count = op.get("count", 10)
                    lazy_col = lazy_col.take(count)
                    
                elif op_type == "skip":
                    count = op.get("count", 0)
                    lazy_col = lazy_col.skip(count)
                    
                elif op_type == "batch":
                    size = op.get("size", 5)
                    lazy_col = lazy_col.batch(size)
            
            # Execute the chain and measure memory
            if validate_memory:
                tracemalloc.start()
                gc.collect()
                
            result = lazy_col.to_list()
            
            if validate_memory:
                current, peak = tracemalloc.get_traced_memory()
                memory_mb = peak / 1024 / 1024
                tracemalloc.stop()
                
                # Very basic memory efficiency check
                input_size_mb = len(source_data) * 8 / 1024 / 1024  # Rough estimate
                if input_size_mb > 0 and memory_mb > input_size_mb * 2:
                    results["memory_efficient"] = False
                    results["errors"].append(f"Chain {i}: Memory usage ({memory_mb:.2f}MB) too high for input size ({input_size_mb:.2f}MB)")
            
            results["chain_results"].append({
                "chain_index": i,
                "success": True,
                "result_length": len(result),
                "operations_count": len(chain)
            })
            
            results["chains_tested"] += 1
            
        except Exception as e:
            results["all_chains_composable"] = False
            results["errors"].append(f"Chain {i}: {str(e)}")
            
            results["chain_results"].append({
                "chain_index": i,
                "success": False,
                "error": str(e),
                "operations_count": len(chain)
            })
    
    return results


def process_lazy_operations(source_data: List[Any], operations: List[Dict], 
                          enable_caching: bool = False) -> Dict[str, Any]:
    """Process a sequence of lazy operations"""
    
    start_time = time.perf_counter()
    
    try:
        # Create lazy collection
        lazy_col = LazyCollection(source_data, cache_enabled=enable_caching)
        
        operations_applied = []
        
        # Apply operations
        for op in operations:
            op_type = op.get("type")
            operations_applied.append(op_type)
            
            if op_type == "map":
                func_str = op.get("function", "lambda x: x")
                func = eval(func_str)
                lazy_col = lazy_col.map(func)
                
            elif op_type == "filter":
                pred_str = op.get("predicate", "lambda x: True")
                pred = eval(pred_str)
                lazy_col = lazy_col.filter(pred)
                
            elif op_type == "take":
                count = op.get("count", 10)
                lazy_col = lazy_col.take(count)
                
            elif op_type == "skip":
                count = op.get("count", 0)
                lazy_col = lazy_col.skip(count)
                
            elif op_type == "batch":
                size = op.get("size", 5)
                lazy_col = lazy_col.batch(size)
        
        # Execute and get results
        tracemalloc.start()
        gc.collect()
        
        result = lazy_col.to_list()
        
        current, peak = tracemalloc.get_traced_memory()
        memory_mb = peak / 1024 / 1024
        tracemalloc.stop()
        
        end_time = time.perf_counter()
        processing_time_ms = (end_time - start_time) * 1000
        
        return {
            "result": result,
            "operations_applied": operations_applied,
            "performance": {
                "processing_time_ms": processing_time_ms,
                "memory_usage_mb": memory_mb,
                "input_size": len(source_data),
                "output_size": len(result),
                "lazy_evaluation": True,
                "operation": f"lazy_chain"
            }
        }
        
    except Exception as e:
        end_time = time.perf_counter()
        processing_time_ms = (end_time - start_time) * 1000
        
        return {
            "error": str(e),
            "operations_applied": operations_applied,
            "performance": {
                "processing_time_ms": processing_time_ms,
                "error": True
            }
        }


def process_pagination(source_data: List[Any], page_number: int, page_size: int,
                      operations: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """Process pagination with optional operations"""
    
    start_time = time.perf_counter()
    
    try:
        # Create lazy collection
        lazy_col = LazyCollection(source_data)
        
        operations_applied = []
        
        # Apply operations if provided
        if operations:
            for op in operations:
                op_type = op.get("type")
                operations_applied.append(op_type)
                
                if op_type == "map":
                    func_str = op.get("function", "lambda x: x")
                    func = eval(func_str)
                    lazy_col = lazy_col.map(func)
                    
                elif op_type == "filter":
                    pred_str = op.get("predicate", "lambda x: True")
                    pred = eval(pred_str)
                    lazy_col = lazy_col.filter(pred)
        
        # Apply pagination
        paginated = lazy_col.page(page_number, page_size)
        
        # Execute and get results
        tracemalloc.start()
        gc.collect()
        
        page_data = paginated.to_list()
        
        current, peak = tracemalloc.get_traced_memory()
        memory_mb = peak / 1024 / 1024
        tracemalloc.stop()
        
        end_time = time.perf_counter()
        processing_time_ms = (end_time - start_time) * 1000
        
        # Calculate pagination info
        has_next_page = len(page_data) == page_size  # Simple heuristic
        has_previous_page = page_number > 1
        
        return {
            "page_data": page_data,
            "current_page": page_number,
            "page_size": page_size,
            "has_next_page": has_next_page,
            "has_previous_page": has_previous_page,
            "operations_applied": operations_applied,
            "performance": {
                "processing_time_ms": processing_time_ms,
                "memory_usage_mb": memory_mb,
                "input_size": len(source_data),
                "output_size": len(page_data),
                "operation": f"pagination_page_{page_number}_size_{page_size}"
            }
        }
        
    except Exception as e:
        end_time = time.perf_counter()
        processing_time_ms = (end_time - start_time) * 1000
        
        return {
            "error": str(e),
            "page_data": [],
            "operations_applied": operations_applied,
            "performance": {
                "processing_time_ms": processing_time_ms,
                "error": True
            }
        }


def process_chunking(source_data: List[Any], chunk_size: int, max_chunks: Optional[int] = None,
                    operations: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """Process chunking with optional operations"""
    
    start_time = time.perf_counter()
    
    try:
        # Create lazy collection
        lazy_col = LazyCollection(source_data)
        
        operations_applied = []
        
        # Apply operations if provided
        if operations:
            for op in operations:
                op_type = op.get("type")
                operations_applied.append(op_type)
                
                if op_type == "map":
                    func_str = op.get("function", "lambda x: x")
                    func = eval(func_str)
                    lazy_col = lazy_col.map(func)
                    
                elif op_type == "filter":
                    pred_str = op.get("predicate", "lambda x: True")
                    pred = eval(pred_str)
                    lazy_col = lazy_col.filter(pred)
        
        # Apply chunking
        chunked = lazy_col.batch(chunk_size)
        
        # Limit chunks if specified
        if max_chunks:
            chunked = chunked.take(max_chunks)
        
        # Execute and get results
        tracemalloc.start()
        gc.collect()
        
        chunks = chunked.to_list()
        
        current, peak = tracemalloc.get_traced_memory()
        memory_mb = peak / 1024 / 1024
        tracemalloc.stop()
        
        end_time = time.perf_counter()
        processing_time_ms = (end_time - start_time) * 1000
        
        # Convert tuples to lists for JSON serialization
        chunks_as_lists = [list(chunk) for chunk in chunks]
        total_items = sum(len(chunk) for chunk in chunks_as_lists)
        
        return {
            "chunks": chunks_as_lists,
            "total_chunks": len(chunks_as_lists),
            "total_items": total_items,
            "chunk_size": chunk_size,
            "max_chunks": max_chunks,
            "operations_applied": operations_applied,
            "performance": {
                "processing_time_ms": processing_time_ms,
                "memory_usage_mb": memory_mb,
                "input_size": len(source_data),
                "output_size": total_items,
                "operation": f"chunking_size_{chunk_size}"
            }
        }
        
    except Exception as e:
        end_time = time.perf_counter()
        processing_time_ms = (end_time - start_time) * 1000
        
        return {
            "error": str(e),
            "chunks": [],
            "operations_applied": operations_applied,
            "performance": {
                "processing_time_ms": processing_time_ms,
                "error": True
            }
        }
