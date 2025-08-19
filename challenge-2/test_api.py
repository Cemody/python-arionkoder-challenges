#!/usr/bin/env python3

import asyncio
import aiohttp
import json

async def test_api():
    """Test the FastAPI endpoints to verify functionality"""
    async with aiohttp.ClientSession() as session:
        try:
            print("🧪 Testing Challenge 2 FastAPI endpoints...\n")
            
            # Test the resources test endpoint
            print("1. Testing POST /resources/test")
            async with session.post('http://127.0.0.1:8000/resources/test', 
                                   json={'resource_types': ['database', 'cache']}) as response:
                print(f"   Status: {response.status}")
                data = await response.json()
                print(f"   Response: {json.dumps(data, indent=4)}")
                print()
                
            # Test the resources status endpoint
            print("2. Testing GET /resources/status")
            async with session.get('http://127.0.0.1:8000/resources/status') as response:
                print(f"   Status: {response.status}")
                data = await response.json()
                print(f"   Response: {json.dumps(data, indent=4)}")
                print()
                
            # Test the analytics endpoint
            print("3. Testing GET /resources/analytics")
            async with session.get('http://127.0.0.1:8000/resources/analytics') as response:
                print(f"   Status: {response.status}")
                data = await response.json()
                print(f"   Response: {json.dumps(data, indent=4)}")
                print()
                
            print("✅ All API tests completed!")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_api())
