#!/usr/bin/env python3
"""
Test script for search API endpoints
"""

import requests
import json
import time

BASE_URL = "http://localhost:8000"

def test_search_suggestions():
    """Test search suggestions endpoint"""
    print("Testing search suggestions endpoint...")
    
    try:
        response = requests.get(f"{BASE_URL}/api/search/suggestions/?term=jo&limit=10", timeout=5)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data.get('suggestions', []))} suggestions")
            return True
        else:
            print(f"Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Exception: {e}")
        return False

def test_universal_search():
    """Test universal search endpoint"""
    print("\nTesting universal search endpoint...")
    
    try:
        search_data = {
            "search_term": "john",
            "filters": {},
            "sort_by": "created_at",
            "sort_order": "desc",
            "page": 1,
            "page_size": 20
        }
        
        response = requests.post(
            f"{BASE_URL}/api/search/universal/",
            json=search_data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            pagination = data.get('pagination', {})
            print(f"Found {len(results)} results")
            print(f"Total count: {pagination.get('total_count', 0)}")
            print(f"Response time: {pagination.get('response_time', 0):.3f}s")
            return True
        else:
            print(f"Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Exception: {e}")
        return False

def test_search_health():
    """Test search health endpoint"""
    print("\nTesting search health endpoint...")
    
    try:
        response = requests.get(f"{BASE_URL}/api/search/health/", timeout=5)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Health status: {data.get('status', 'unknown')}")
            return True
        else:
            print(f"Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Exception: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 50)
    print("Testing Search API Endpoints")
    print("=" * 50)
    
    # Wait a moment for server to start
    time.sleep(2)
    
    results = []
    
    # Test each endpoint
    results.append(test_search_suggestions())
    results.append(test_universal_search())
    results.append(test_search_health())
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Summary:")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("All tests passed! Search endpoints are working correctly.")
    else:
        print("Some tests failed. Please check the server configuration.")
    
    return passed == total

if __name__ == "__main__":
    main()
