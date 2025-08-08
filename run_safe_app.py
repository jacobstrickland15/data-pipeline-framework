#!/usr/bin/env python3
"""
SAFE web application startup - NO performance monitoring, NO system metrics collection.
This version will NOT break your computer.
"""

import sys
from pathlib import Path

# Add src directory to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def main():
    print("🛡️  SAFE MODE - Data Pipeline Web Interface")
    print("=" * 50)
    print("⚠️  ALL SYSTEM MONITORING DISABLED")
    print("⚠️  NO AUTO-REFRESH")
    print("⚠️  NO PERFORMANCE METRICS")
    print("=" * 50)
    
    try:
        from data_pipeline.web.app import WebApplication
        from fastapi import FastAPI
        
        # Create minimal web app without monitoring
        app = FastAPI(title="Data Pipeline Framework - Safe Mode")
        
        @app.get("/")
        async def safe_dashboard():
            return {"message": "Safe mode - monitoring disabled", "status": "safe"}
            
        @app.get("/api/health") 
        async def safe_health():
            return {"status": "safe", "monitoring": "disabled"}
            
        @app.get("/api/metrics")
        async def safe_metrics():
            return {
                "metrics": {
                    "system_cpu_usage_percent": {"latest": 0, "message": "disabled"},
                    "system_memory_usage_percent": {"latest": 0, "message": "disabled"},
                    "system_disk_usage_percent": {"latest": 0, "message": "disabled"},
                    "pipeline_executions_total": {"latest": 0, "message": "no executions"}
                },
                "status": "safe_mode"
            }
        
        print("🌐 Starting SAFE web server on http://127.0.0.1:8088")
        print("📊 Dashboard: http://127.0.0.1:8088/")
        print("🔍 Health: http://127.0.0.1:8088/api/health")
        print("📈 Metrics: http://127.0.0.1:8088/api/metrics")
        print("\n⏹  Press Ctrl+C to stop")
        
        import uvicorn
        uvicorn.run(app, host="127.0.0.1", port=8088, log_level="error")
        
    except KeyboardInterrupt:
        print("\n👋 Safe server stopped")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()