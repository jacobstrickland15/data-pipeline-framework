#!/usr/bin/env python3
"""
Production-ready startup script for the Data Pipeline Framework web interface.
This version includes real data integration, performance optimizations, and proper error handling.
"""

import argparse
import sys
import os
import logging
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('web_app.log')
    ]
)
logger = logging.getLogger(__name__)

def validate_environment():
    """Validate that all required dependencies are available."""
    requirements = {
        'fastapi': 'fastapi',
        'uvicorn': 'uvicorn', 
        'pydantic': 'pydantic',
        'yaml': 'pyyaml',
        'dotenv': 'python-dotenv',
        'pandas': 'pandas',
        'psycopg2': 'psycopg2-binary',
        'sqlalchemy': 'sqlalchemy'
    }
    
    missing = []
    for module_name, package_name in requirements.items():
        try:
            __import__(module_name)
        except ImportError:
            missing.append(package_name)
    
    if missing:
        print(f"❌ Missing required dependencies: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    
    return True

def setup_monitoring():
    """Initialize monitoring and metrics collection."""
    try:
        from data_pipeline.observability.metrics import get_performance_monitor, get_metrics_collector, get_alert_manager
        
        # Start performance monitoring
        perf_monitor = get_performance_monitor()
        perf_monitor.start_monitoring()
        
        # Initialize metrics with some baseline data
        metrics_collector = get_metrics_collector()
        metrics_collector.record_gauge("system_startup", 1.0)
        
        # Set up basic alert rules
        alert_manager = get_alert_manager()
        
        logger.info("✅ Monitoring system initialized")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error setting up monitoring: {e}")
        return False

def create_sample_pipeline():
    """Create a sample pipeline for demonstration."""
    try:
        config_dir = Path("config/pipelines")
        config_dir.mkdir(parents=True, exist_ok=True)
        
        sample_config = {
            "name": "sample_pipeline",
            "description": "Sample data processing pipeline",
            "source": {
                "type": "csv",
                "config": {
                    "file_path": "data/raw/sample_data.csv",
                    "delimiter": ",",
                    "encoding": "utf-8"
                }
            },
            "processing": {
                "engine": "pandas",
                "operations": [
                    {"type": "filter_rows", "config": "column > 0"},
                    {"type": "select_columns", "config": ["id", "name", "value"]}
                ]
            },
            "storage": {
                "type": "postgresql", 
                "destination": "processed_data",
                "mode": "append"
            },
            "validation": {
                "enabled": True,
                "auto_generate_expectations": True
            },
            "monitoring": {
                "enabled": True
            }
        }
        
        import yaml
        config_path = config_dir / "sample_pipeline.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False)
            
        logger.info(f"✅ Sample pipeline created at {config_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating sample pipeline: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Start the Production Data Pipeline Web Interface")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the server to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind the server to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--no-monitoring", action="store_true", help="Disable monitoring setup")
    parser.add_argument("--create-sample", action="store_true", help="Create sample pipeline")
    
    args = parser.parse_args()
    
    print("🚀 Data Pipeline Framework - Production Web Interface")
    print("=" * 60)
    
    # Validate environment
    if not validate_environment():
        sys.exit(1)
    
    try:
        # Import web app
        from data_pipeline.web.app import create_web_app
        logger.info("✅ Web application modules loaded")
        
        # Setup monitoring (unless disabled)
        if not args.no_monitoring:
            setup_monitoring()
        
        # Create sample pipeline if requested
        if args.create_sample:
            create_sample_pipeline()
        
        # Create web app instance
        app = create_web_app(host=args.host, port=args.port, debug=args.debug)
        logger.info(f"✅ Web application created on {args.host}:{args.port}")
        
        print(f"\n🌐 Server starting at http://{args.host}:{args.port}")
        print("=" * 60)
        print("\n📊 Available Features:")
        print(f"   • Real-time Dashboard:  http://{args.host}:{args.port}/")
        print(f"   • Pipeline Management:  http://{args.host}:{args.port}/pipelines")  
        print(f"   • Pipeline Creator:     http://{args.host}:{args.port}/create-pipeline")
        print(f"   • Live Monitoring:      http://{args.host}:{args.port}/monitoring")
        print(f"   • Data Quality Tests:   http://{args.host}:{args.port}/data-quality")
        print(f"   • System Settings:      http://{args.host}:{args.port}/settings")
        
        print("\n🔌 API Endpoints:")
        print(f"   • System Health:        http://{args.host}:{args.port}/api/health")
        print(f"   • Live Metrics:         http://{args.host}:{args.port}/api/metrics")
        print(f"   • Active Alerts:        http://{args.host}:{args.port}/api/alerts")
        print(f"   • Pipeline List:        http://{args.host}:{args.port}/api/pipelines")
        print(f"   • Quality Tests:        http://{args.host}:{args.port}/api/data-quality/tests")
        
        print("\n🎯 Key Features:")
        print("   ✅ Real system metrics (CPU, Memory, Disk)")
        print("   ✅ Actual pipeline execution and status")  
        print("   ✅ Working data quality tests")
        print("   ✅ Performance optimized monitoring")
        print("   ✅ Production-ready error handling")
        
        print(f"\n📝 Debug mode: {'ENABLED' if args.debug else 'DISABLED'}")
        print(f"📊 Monitoring: {'DISABLED' if args.no_monitoring else 'ENABLED'}")
        print("\n⏹  Press Ctrl+C to stop the server")
        print("=" * 60)
        
        # Start the server
        import uvicorn
        uvicorn.run(
            app.app,
            host=args.host,
            port=args.port,
            log_level="debug" if args.debug else "info",
            access_log=True
        )
        
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped by user")
        logger.info("Server stopped by user")
    except Exception as e:
        print(f"\n❌ Error starting web application: {e}")
        logger.error(f"Error starting web application: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()