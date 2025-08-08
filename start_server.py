#!/usr/bin/env python3
"""
Quick server startup script to test the API endpoints.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from data_pipeline.web.app import DataPipelineApp
    import uvicorn
    
    print("🚀 Starting Data Pipeline Web Server...")
    print("📡 API Endpoints Available:")
    print("   • Quality Check: POST /api/data-quality/run-all-tests")
    print("   • Quality Metrics: GET /api/quality-metrics") 
    print("   • Export Metrics: GET /api/export-metrics")
    print("   • System Metrics: GET /api/metrics")
    print("   • Web Interface: http://127.0.0.1:8000")
    
    # Create app instance
    config = {
        'database': {'url': 'sqlite:///data/pipeline.db'},
        'server': {'host': '127.0.0.1', 'port': 8000}
    }
    
    app = DataPipelineApp(config)
    
    # Start server
    print("\n✅ Server starting on http://127.0.0.1:8000")
    print("   📊 Data Quality: http://127.0.0.1:8000/data-quality")  
    print("   📈 Monitoring: http://127.0.0.1:8000/monitoring")
    print("   🏠 Home: http://127.0.0.1:8000")
    print("\nPress Ctrl+C to stop the server")
    
    uvicorn.run(app.app, host="127.0.0.1", port=8000, log_level="info")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("💡 Try installing required packages:")
    print("   pip install fastapi uvicorn")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error starting server: {e}")
    sys.exit(1)