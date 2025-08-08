# Web Interface for Data Pipeline Framework

The Data Pipeline Framework now includes a fully functional web interface built with FastAPI, providing an intuitive way to manage your data pipelines through a modern web browser.

## 🚀 Quick Start

### Prerequisites

Install the required dependencies:
```bash
pip install fastapi uvicorn pydantic pyyaml python-dotenv pandas psycopg2-binary sqlalchemy
```

Or using the project's requirements file:
```bash
pip install -r requirements.txt
```

### Starting the Web Interface

#### Option 1: Using the startup script
```bash
python3 start_web_app.py
```

#### Option 2: Custom configuration
```bash
python3 start_web_app.py --host 0.0.0.0 --port 8080 --debug
```

#### Option 3: Direct import
```python
from src.data_pipeline.web.app import start_web_app
start_web_app(host="127.0.0.1", port=8080, debug=False)
```

## 📱 Features

### Dashboard (`/`)
- **Real-time metrics** - Pipeline executions, success rates, data throughput
- **System health** - Overall status and key performance indicators  
- **Interactive charts** - Pipeline performance and system resources
- **Recent activity** - Latest pipeline executions and alerts
- **Auto-refresh** - Updates every 30 seconds

### Pipeline Management (`/pipelines`)
- **Pipeline browser** - View all configured pipelines with status
- **Quick actions** - Run, view, edit pipelines directly from the interface
- **Search and filter** - Find pipelines by name, status, or description
- **Status indicators** - Visual status (active, inactive, error)
- **Performance metrics** - Success rates, execution times, row counts

### Create Pipeline (`/create-pipeline`)
- **Interactive form** - Step-by-step pipeline configuration
- **Multiple data sources** - CSV, JSON, S3, PostgreSQL support
- **Processing engines** - Choose between Pandas and Spark
- **Data transformations** - Add filtering, aggregation, and other operations
- **File upload** - Upload sample data for schema inference
- **Validation options** - Enable data quality checks and monitoring

### Monitoring (`/monitoring`)
- **Performance metrics** - Execution times, success rates, throughput
- **System resources** - CPU, memory, disk usage monitoring
- **Time series charts** - Historical performance data
- **Data quality scores** - Completeness, accuracy, consistency metrics
- **Real-time updates** - Live monitoring with configurable refresh intervals

### Data Quality (`/data-quality`)
- **Quality dashboard** - Overall quality score and test results
- **Quality dimensions** - Completeness, accuracy, consistency, validity, uniqueness
- **Test management** - View, run, and configure data quality tests
- **Issue tracking** - Identify and track data quality problems
- **Quality rules** - Configure and manage validation rules

### Settings (`/settings`)
- **Application config** - General settings and performance tuning
- **Database setup** - PostgreSQL connection configuration
- **Storage config** - Local and S3 storage settings
- **Monitoring setup** - Metrics collection and health checks
- **Notifications** - Email alerts and notification rules

## 🔌 API Endpoints

The web interface provides several REST API endpoints:

### Health & Status
- `GET /api/health` - System health check
- `GET /api/system/info` - System information

### Metrics & Monitoring  
- `GET /api/metrics?window=1h` - Performance metrics (1h, 24h, 7d, 30d)
- `GET /api/alerts` - Active and recent alerts

### Pipeline Operations
- `POST /api/pipelines` - Create new pipeline
- `POST /api/pipelines/{name}/execute` - Execute pipeline
- `GET /api/pipelines/{name}/status` - Get pipeline status

### File Management
- `POST /api/upload` - Upload data files

## 🎨 User Interface

The web interface features:
- **Responsive design** - Works on desktop, tablet, and mobile
- **Bootstrap 5** - Modern, clean UI components
- **Chart.js integration** - Interactive charts and visualizations  
- **FontAwesome icons** - Consistent iconography
- **Real-time updates** - AJAX-powered dynamic content
- **Dark/light themes** - Professional appearance

## 🛠 Technical Details

### Architecture
- **FastAPI backend** - High-performance async web framework
- **Jinja2 templates** - Server-side HTML rendering
- **RESTful API** - Clean separation between UI and backend
- **WebSocket support** - Ready for real-time features

### Security Features
- **Input validation** - Pydantic models for request validation
- **CORS support** - Configurable cross-origin resource sharing
- **Error handling** - Graceful error responses and logging

### Performance
- **Async operations** - Non-blocking request handling
- **Connection pooling** - Efficient database connections
- **Caching ready** - Template and response caching support
- **Static file serving** - Optimized asset delivery

## 🔧 Configuration

### Environment Variables
```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/pipeline_db

# Storage  
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your-bucket-name

# Application
DEBUG=false
LOG_LEVEL=info
MAX_WORKERS=4
```

### Template Customization
Templates are located in `templates/` directory:
- `base.html` - Base template with navigation
- `dashboard.html` - Main dashboard
- `pipelines.html` - Pipeline management
- `create_pipeline.html` - Pipeline creation form
- `monitoring.html` - Monitoring dashboard  
- `data_quality.html` - Data quality interface
- `settings.html` - Configuration settings

## 🚨 Troubleshooting

### Common Issues

**Templates not found**
- Ensure templates directory exists at project root
- Check template path configuration in `app.py`

**Module import errors**
- Install all required dependencies
- Check Python path configuration

**Database connection errors**
- Verify PostgreSQL is running
- Check database credentials in settings
- Test connection using the web interface

**Port already in use**
- Use different port: `python3 start_web_app.py --port 8081`
- Kill existing process using the port

## 📈 Performance Tips

1. **Use appropriate batch sizes** for large datasets
2. **Enable caching** for frequently accessed data  
3. **Configure connection pooling** for high concurrency
4. **Use reverse proxy** (nginx) for production deployments
5. **Monitor system resources** through the web interface

## 🔮 Future Enhancements

- WebSocket support for real-time updates
- User authentication and authorization
- Advanced pipeline scheduling interface  
- Data lineage visualization
- Custom dashboard widgets
- Export/import pipeline configurations
- Advanced data quality rule builder
- Integration with external monitoring systems

---

The web interface makes the Data Pipeline Framework accessible to users who prefer graphical interfaces over command-line tools, while maintaining all the powerful features of the underlying framework.