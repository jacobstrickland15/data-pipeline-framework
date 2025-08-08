#!/usr/bin/env python3
"""
Simple startup script for the Data Pipeline Framework web interface.

Usage:
    python3 start_web_app.py [--port PORT] [--host HOST] [--debug]

Example:
    python3 start_web_app.py --port 8080 --host 127.0.0.1
"""

import argparse
import sys
import os
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def main():
    parser = argparse.ArgumentParser(description="Start the Data Pipeline Framework web interface")
    parser.add_argument(
        "--host", 
        default="127.0.0.1", 
        help="Host to bind the server to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8080, 
        help="Port to bind the server to (default: 8080)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true", 
        help="Enable debug mode"
    )
    
    args = parser.parse_args()
    
    try:
        from data_pipeline.web.app import start_web_app
        
        print("🚀 Data Pipeline Framework Web Interface")
        print("=" * 50)
        print(f"📡 Starting server at http://{args.host}:{args.port}")
        print(f"🔧 Debug mode: {'enabled' if args.debug else 'disabled'}")
        print("=" * 50)
        print("\n🌐 Available pages:")
        print(f"   • Dashboard:     http://{args.host}:{args.port}/")
        print(f"   • Pipelines:     http://{args.host}:{args.port}/pipelines")
        print(f"   • Create:        http://{args.host}:{args.port}/create-pipeline")
        print(f"   • Monitoring:    http://{args.host}:{args.port}/monitoring")
        print(f"   • Data Quality:  http://{args.host}:{args.port}/data-quality")
        print(f"   • Settings:      http://{args.host}:{args.port}/settings")
        print("\n🔌 API endpoints:")
        print(f"   • Health:        http://{args.host}:{args.port}/api/health")
        print(f"   • Metrics:       http://{args.host}:{args.port}/api/metrics")
        print(f"   • Alerts:        http://{args.host}:{args.port}/api/alerts")
        print("\n⏹  Press Ctrl+C to stop the server")
        print("=" * 50)
        
        start_web_app(host=args.host, port=args.port, debug=args.debug)
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("\n💡 Make sure to install the required dependencies:")
        print("   pip install fastapi uvicorn pydantic pyyaml python-dotenv pandas psycopg2-binary sqlalchemy")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting web app: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()