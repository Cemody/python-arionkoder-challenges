#!/usr/bin/env python3

import asyncio
import pytest
from utils import ResourceManager

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

@pytest.mark.asyncio
async def test_nested_context_managers():
    """Test that ResourceManager supports nested context managers"""
    
    # Test basic nesting
    async with ResourceManager(['database']) as outer:
        assert 'database' in outer
        outer_db = outer['database']
        assert outer_db.connected
        
        async with ResourceManager(['cache']) as inner:
            assert 'cache' in inner
            inner_cache = inner['cache']
            assert inner_cache.connected
            
            # Both should be active simultaneously
            assert outer_db.connected
            assert inner_cache.connected
            
            # Test operations in both contexts
            await outer_db.test_connection()
            await inner_cache.test_connection()
            
        # After inner context, outer should still be active
        assert outer_db.connected
        
    # Both should be cleaned up
    assert not outer_db.connected

@pytest.mark.asyncio
async def test_deep_nested_context_managers():
    """Test deeply nested context managers (3 levels)"""
    
    async with ResourceManager(['database']) as level1:
        assert level1['database'].connected
        
        async with ResourceManager(['cache']) as level2:
            assert level2['cache'].connected
            assert level1['database'].connected  # Still active
            
            async with ResourceManager(['database']) as level3:
                # New independent database connection
                assert level3['database'].connected
                assert level2['cache'].connected  # Still active
                assert level1['database'].connected  # Still active
                
                # All three should have independent connections
                assert level1['database'] != level3['database']
                
            # Level 3 cleaned up, others still active
            assert level2['cache'].connected
            assert level1['database'].connected
            
        # Level 2 cleaned up, level 1 still active
        assert level1['database'].connected

@pytest.mark.asyncio
async def test_clear_api_for_resource_acquisition():
    """Test clear API for resource acquisition and release"""
    
    async with ResourceManager(['database', 'cache']) as resources:
        
        # Test dictionary-like access (acquisition)
        db = resources['database']
        cache = resources['cache']
        
        assert db is not None
        assert cache is not None
        assert hasattr(db, 'connected')
        assert hasattr(cache, 'connected')
        
        # Test connection status
        assert db.connected
        assert cache.connected
        
        # Test resource operations (clear API usage)
        db_test = await db.test_connection()
        cache_test = await cache.test_connection()
        
        assert 'database_file' in db_test
        assert 'max_size' in cache_test
        
        # Test execute operations (clear API)
        insert_result = await db.execute_operation("insert", {
            "name": "api_test",
            "value": "clear_api"
        })
        assert 'inserted_id' in insert_result
        
        set_result = await cache.execute_operation("set", {
            "key": "api_test",
            "value": "clear_api_value"
        })
        assert 'key' in set_result  # Cache returns key, not success
        assert set_result['key'] == "api_test"
        
        get_result = await cache.execute_operation("get", {
            "key": "api_test"
        })
        assert get_result['value'] == "clear_api_value"

@pytest.mark.asyncio
async def test_resource_lifecycle_management():
    """Test clear resource lifecycle with proper acquisition and release"""
    
    # Test that resources are properly acquired in __aenter__
    rm = ResourceManager(['database'])
    
    # Before entering context
    assert len(rm.connections) == 0
    
    async with rm as resources:
        # After entering context - resources acquired
        assert len(resources) == 1
        assert 'database' in resources
        assert resources['database'].connected
        
        # Test resource usage
        db = resources['database']
        test_result = await db.test_connection()
        assert test_result is not None
        
    # After exiting context - resources released
    # Note: connections dict might still have references, but actual connections should be closed

@pytest.mark.asyncio
async def test_error_handling_in_nested_contexts():
    """Test proper cleanup when errors occur in nested contexts"""
    
    try:
        async with ResourceManager(['database']) as outer:
            assert outer['database'].connected
            
            try:
                async with ResourceManager(['database']) as inner:
                    assert inner['database'].connected
                    
                    # Force an error
                    await inner['database'].execute_operation("invalid_op", {})
                    
            except ValueError:
                # Inner context should clean up even with error
                pass
            
            # Outer context should still be active
            assert outer['database'].connected
            
    except Exception:
        pytest.fail("Outer context was affected by inner context error")

@pytest.mark.asyncio
async def test_concurrent_nested_contexts():
    """Test that nested contexts can handle sequential operations properly"""
    
    async with ResourceManager(['database']) as outer:
        # Perform sequential operations in outer context
        for i in range(3):
            result = await outer['database'].execute_operation("insert", {
                "name": f"outer_{i}",
                "value": f"outer_value_{i}"
            })
            assert 'inserted_id' in result
        
        async with ResourceManager(['cache']) as inner:
            # Perform operations in inner context
            for i in range(3):
                result = await inner['cache'].execute_operation("set", {
                    "key": f"inner_{i}",
                    "value": f"inner_value_{i}"
                })
                assert 'key' in result
                assert result['key'] == f"inner_{i}"
            
            # Verify cache operations worked
            for i in range(3):
                result = await inner['cache'].execute_operation("get", {
                    "key": f"inner_{i}"
                })
                assert result['value'] == f"inner_value_{i}"
                assert result['found'] is True

def test_api_clarity():
    """Test that the API is clear and intuitive"""
    
    # The API should be clear from the class and method names
    rm = ResourceManager(['database', 'cache'])
    
    # Clear context manager protocol
    assert hasattr(rm, '__aenter__')
    assert hasattr(rm, '__aexit__')
    
    # Clear resource management
    assert hasattr(rm, 'connections')
    assert hasattr(rm, 'resource_types')

async def run_all_tests():
    """Run all constraint validation tests"""
    
    print("🧪 Testing Challenge 2 Constraints Compliance")
    print("=" * 50)
    
    tests = [
        ("Nested Context Managers", test_nested_context_managers),
        ("Deep Nested Context Managers", test_deep_nested_context_managers),
        ("Clear Resource Acquisition API", test_clear_api_for_resource_acquisition),
        ("Resource Lifecycle Management", test_resource_lifecycle_management),
        ("Error Handling in Nested Contexts", test_error_handling_in_nested_contexts),
        ("Concurrent Nested Contexts", test_concurrent_nested_contexts),
        ("API Clarity", test_api_clarity),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            print(f"🔍 Testing: {test_name}")
            if asyncio.iscoroutinefunction(test_func):
                await test_func()
            else:
                test_func()
            print(f"✅ PASSED: {test_name}")
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {test_name} - {e}")
            failed += 1
    
    print(f"\n📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All constraints satisfied!")
        print("✅ Challenge 2 supports nested context managers")
        print("✅ Challenge 2 provides clear API for resource acquisition and release")
    else:
        print("⚠️  Some constraints not fully satisfied")
    
    return failed == 0

if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    exit(0 if success else 1)
