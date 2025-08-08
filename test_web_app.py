#!/usr/bin/env python3
"""Simple test script to verify the web application works."""

import asyncio
import time
import threading
import requests
from src.data_pipeline.web.app import create_web_app

def test_web_app():
    """Test that the web application starts and serves pages."""
    try:
        # Create web app instance
        web_app = create_web_app(host="127.0.0.1", port=8082, debug=True)
        
        # Start server in a separate thread
        def run_server():
            import uvicorn
            uvicorn.run(
                web_app.app,
                host="127.0.0.1",
                port=8082,
                log_level="info"
            )
        
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        
        # Wait for server to start
        time.sleep(3)
        
        # Test endpoints
        base_url = "http://127.0.0.1:8082"
        endpoints = [
            "/",
            "/pipelines", 
            "/create-pipeline",
            "/monitoring",
            "/data-quality",
            "/settings",
            "/api/health",
            "/api/metrics"
        ]
        
        print("Testing web application endpoints...")
        results = {}
        
        for endpoint in endpoints:
            try:
                response = requests.get(f"{base_url}{endpoint}", timeout=5)
                results[endpoint] = {
                    "status_code": response.status_code,
                    "success": response.status_code == 200
                }
                print(f"✅ {endpoint}: {response.status_code}")
            except requests.exceptions.RequestException as e:
                results[endpoint] = {
                    "error": str(e),
                    "success": False
                }
                print(f"❌ {endpoint}: {e}")
        
        # Summary
        successful = sum(1 for r in results.values() if r.get("success", False))
        total = len(results)
        print(f"\nSummary: {successful}/{total} endpoints working")
        
        if successful == total:
            print("🎉 All endpoints are functional!")
            return True
        else:
            print("⚠️ Some endpoints have issues")
            return False
            
    except Exception as e:
        print(f"❌ Failed to test web app: {e}")
        return False

if __name__ == "__main__":
    test_web_app()