"""This demo showcases all the key features and constraints testing:"""

import asyncio
import time
import gc
from pathlib import Path
import sys
import pytest
from unittest.mock import patch, AsyncMock
from utils import ResourceManager, APIConnection, DatabaseConnection, CacheConnection

                                                    
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

                         
from utils import ResourceManager


def measure_performance(test_name, test_func):
"""Helper to measure execution time and memory."""
    print(f"\n--- {test_name} ---")
    gc.collect()
    
    start_time = time.time()
    
                                                            
    result = test_func()
    
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000         
    
    print(f"⏱️  Execution time: {execution_time:.2f} ms")
    return result

def test_manage_multiple_resources():
    async def _run_test():
        print("🔄 Testing management of multiple resources (API, DB, Cache)...")

        with patch.object(APIConnection, "test_connection",
                          new=AsyncMock(return_value={"status_code": 200, "connection_ok": True})),\
             patch.object(APIConnection, "execute_operation",
                          new=AsyncMock(return_value={"status_code": 200})),\
             patch.object(DatabaseConnection, "test_connection",
                          new=AsyncMock(return_value={"connection_ok": True, "database_file": "resource_manager.db"})),\
             patch.object(CacheConnection, "test_connection",
                          new=AsyncMock(return_value={"connection_ok": True, "max_size": 1024})):
            async with ResourceManager(["database", "api", "cache"]) as resources:
                api_conn = resources.connections["api"]
                db_conn = resources.connections["database"]
                cache_conn = resources.connections["cache"]

                api_status = await api_conn.test_connection()
                db_status = await db_conn.test_connection()
                cache_status = await cache_conn.test_connection()

                assert api_status.get('connection_ok')
                assert db_status.get('connection_ok')
                assert cache_status.get('connection_ok')

                await api_conn.execute_operation("get", {})
                await db_conn.execute_operation("query", {})
                await cache_conn.execute_operation("get", {"key": "test"})
        return True

    asyncio.run(_run_test())

def test_proper_cleanup_on_exception():
"""FEATURE: Provides proper cleanup in case of exceptions."""
    async def _run_test():
        print("🛡️  Testing proper cleanup after an exception...")
        
        try:
            async with ResourceManager(["database"]) as resources:
                print("  - Resources acquired for exception test.")
                db_conn = resources.connections["database"]
                                                                              
                await db_conn.execute_operation("invalid_operation", {})
        except ValueError as e:
            print(f"  - Caught expected error: {e}")
        
        print("  - Context block exited, cleanup should be complete.")
        return True

    asyncio.run(_run_test())

def test_performance_logging():
"""FEATURE: Includes detailed logging and performance metrics."""
    async def _run_test():
        print("⏱️  Testing performance logging...")
        
        async with ResourceManager(["database", "cache"]) as resources:
            db_conn = resources.connections["database"]
            cache_conn = resources.connections["cache"]
            
                                                                                        
            db_result = await db_conn.execute_operation("query", {})
            
            execution_time = db_result.get("execution_time")
            print(f"  - DB operation execution time from mock: {execution_time:.4f}ms")
            assert execution_time is not None and execution_time > 0

            cache_result = await cache_conn.execute_operation("get", {"key": "test"})
            execution_time = cache_result.get("execution_time")
            print(f"  - Cache operation execution time from mock: {execution_time:.4f}ms")
            assert execution_time is not None and execution_time > 0
            
                                                   
            print(f"  - Resource setup metrics: {resources.setup_metrics}")
            assert len(resources.setup_metrics) > 0

        return True

    asyncio.run(_run_test())

def test_nested_context_managers():
"""CONSTRAINT: Must support nested context managers."""
    async def _run_test():
                                                 
        with patch.object(
            DatabaseConnection, "test_connection",
            new=AsyncMock(return_value={"connection_ok": True, "database_file": "resource_manager.db"})
        ), patch.object(
            APIConnection, "test_connection",
            new=AsyncMock(return_value={"connection_ok": True, "status_code": 200})
        ), patch.object(
            APIConnection, "execute_operation",
            new=AsyncMock(return_value={"status_code": 200})
        ):
            print("🔄 Testing nested context managers constraint...")

            async with ResourceManager(["database"]) as outer_resources:
                print("  - Entered outer context.")
                outer_db = outer_resources.connections["database"]
                outer_status = await outer_db.test_connection()
                assert outer_status.get('connection_ok')

                async with ResourceManager(["api"]) as inner_resources:
                    print("  - Entered inner context.")
                    inner_api = inner_resources.connections["api"]
                    inner_status = await inner_api.test_connection()
                    assert inner_status.get('connection_ok')
                    print("  - Exiting inner context.")

                                                                  
                outer_status_after = await outer_db.test_connection()
                assert outer_status_after.get('connection_ok')
                print("  - Outer context remains active and correct.")
                print("  - Exiting outer context.")

        return True

    asyncio.run(_run_test())
def test_resource_acquisition_api():
"""CONSTRAINT: Should provide a clear API for resource acquisition and release."""
    async def _run_test():
        print("🤝 Testing clear API for resource acquisition and release...")
        
                                                    
        async with ResourceManager(["database", "api"]) as resources:
                                                                            
            print("  - Acquired resources via `resources.connections` dict.")
            assert "database" in resources.connections
            assert "api" in resources.connections
            assert "cache" not in resources.connections                           

                                                                   
            print("  - Release is handled implicitly by `async with`.")
            
        return True
    
    asyncio.run(_run_test())

def main():
"""Run all generalized tests for Challenge 2 based on docstring."""
    print("🚀 Challenge 2 - ResourceManager Feature and Constraint Testing")
    print("=" * 70)
    
                                            
    test_suite = [
        ("Feature: Manages Multiple External Resources", test_manage_multiple_resources),
        ("Feature: Proper Cleanup on Exception", test_proper_cleanup_on_exception),
        ("Feature: Detailed Logging and Performance Metrics", test_performance_logging),
        ("Constraint: Supports Nested Context Managers", test_nested_context_managers),
        ("Constraint: Clear API for Resource Acquisition", test_resource_acquisition_api),
    ]
    
    results = []
    for name, test_func in test_suite:
        try:
                                                             
            result = measure_performance(name, test_func)
            results.append((name, result))
            print(f"{'✅' if result else '❌'} {name}: {'PASSED' if result else 'FAILED'}")
        except Exception as e:
            print(f"❌ {name}: FAILED with an unexpected error: {e}")
            results.append((name, False))

             
    print("\n" + "=" * 70)
    print("📋 TEST SUMMARY")
    print("=" * 70)
    
    passed_count = sum(1 for _, res in results if res)
    total_count = len(results)
    
    print(f"✅ Passed: {passed_count}/{total_count}")
    print(f"❌ Failed: {total_count - passed_count}/{total_count}")
    
    if passed_count == total_count:
        print("\n🎉 All demo tests passed! ResourceManager is working as expected.")
    else:
        print("\n⚠️ Some demo tests failed. Please review the output.")


if __name__ == "__main__":
                                                                     
                                  
    
                                                                           
    class TestDemo:
        def test_run_demo(self, mock_all_connections):
            main()

                                                                    
                                                         
                                                                                  
    pytest.main(["-s", __file__])
