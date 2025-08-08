"""Real-time monitoring dashboard with web interface."""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path
import asyncio

try:
    from flask import Flask, render_template_string, jsonify, request
    from flask_socketio import SocketIO, emit
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

from .metrics import get_metrics_collector, get_alert_manager, get_health_checker

logger = logging.getLogger(__name__)


class DashboardServer:
    """Web-based monitoring dashboard."""
    
    def __init__(self, host: str = '127.0.0.1', port: int = 8050, debug: bool = False):
        if not FLASK_AVAILABLE:
            raise ImportError("Flask and Flask-SocketIO required for dashboard. Install with: pip install flask flask-socketio")
        
        self.host = host
        self.port = port
        self.debug = debug
        
        # Flask app setup
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'data-pipeline-dashboard'
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        
        # Components
        self.metrics_collector = get_metrics_collector()
        self.alert_manager = get_alert_manager()
        self.health_checker = get_health_checker()
        
        # Setup routes
        self._setup_routes()
        self._setup_websocket_events()
        
        # Background task for real-time updates
        self.update_interval = 5  # seconds
        
    def _setup_routes(self):
        """Setup Flask routes."""
        
        @self.app.route('/')
        def index():
            return render_template_string(DASHBOARD_HTML_TEMPLATE)
        
        @self.app.route('/api/metrics')
        def get_metrics():
            """Get current metrics data."""
            try:
                window = request.args.get('window', '1h')
                
                # Get key metrics
                metrics_data = {}
                
                # Pipeline metrics
                pipeline_metrics = [
                    'pipeline_executions_total',
                    'pipeline_success_total', 
                    'pipeline_failure_total',
                    'pipeline_duration_seconds',
                    'data_rows_processed_total',
                    'data_quality_score'
                ]
                
                for metric_name in pipeline_metrics:
                    aggregated = self.metrics_collector.get_aggregated_metrics(metric_name, window)
                    if aggregated:
                        metrics_data[metric_name] = aggregated
                
                # System metrics
                system_metrics = [
                    'system_cpu_usage_percent',
                    'system_memory_usage_percent',
                    'system_disk_usage_percent'
                ]
                
                for metric_name in system_metrics:
                    aggregated = self.metrics_collector.get_aggregated_metrics(metric_name, window)
                    if aggregated:
                        metrics_data[metric_name] = aggregated
                
                return jsonify({
                    'status': 'success',
                    'data': metrics_data,
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error getting metrics: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/alerts')
        def get_alerts():
            """Get current alerts."""
            try:
                active_alerts = self.alert_manager.get_active_alerts()
                recent_alerts = self.alert_manager.get_alert_history(hours=24)
                
                return jsonify({
                    'status': 'success',
                    'data': {
                        'active_alerts': [alert.to_dict() for alert in active_alerts],
                        'recent_alerts': [alert.to_dict() for alert in recent_alerts[-20:]]  # Last 20
                    },
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error getting alerts: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/health')
        def get_health():
            """Get system health status."""
            try:
                health_status = self.health_checker.run_checks()
                
                return jsonify({
                    'status': 'success',
                    'data': health_status,
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error getting health status: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
        
        @self.app.route('/api/pipeline-stats')
        def get_pipeline_stats():
            """Get pipeline execution statistics."""
            try:
                window = request.args.get('window', '24h')
                
                # Calculate pipeline statistics
                executions = self.metrics_collector.get_aggregated_metrics('pipeline_executions_total', window)
                successes = self.metrics_collector.get_aggregated_metrics('pipeline_success_total', window)
                failures = self.metrics_collector.get_aggregated_metrics('pipeline_failure_total', window)
                durations = self.metrics_collector.get_aggregated_metrics('pipeline_duration_seconds', window)
                rows_processed = self.metrics_collector.get_aggregated_metrics('data_rows_processed_total', window)
                
                stats = {
                    'total_executions': executions.get('sum', 0) if executions else 0,
                    'successful_executions': successes.get('sum', 0) if successes else 0,
                    'failed_executions': failures.get('sum', 0) if failures else 0,
                    'success_rate': 0,
                    'average_duration': durations.get('avg', 0) if durations else 0,
                    'total_rows_processed': rows_processed.get('sum', 0) if rows_processed else 0
                }
                
                # Calculate success rate
                if stats['total_executions'] > 0:
                    stats['success_rate'] = (stats['successful_executions'] / stats['total_executions']) * 100
                
                return jsonify({
                    'status': 'success',
                    'data': stats,
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error getting pipeline stats: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
    
    def _setup_websocket_events(self):
        """Setup WebSocket events for real-time updates."""
        
        @self.socketio.on('connect')
        def handle_connect():
            logger.info('Client connected to dashboard')
            emit('status', {'message': 'Connected to monitoring dashboard'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            logger.info('Client disconnected from dashboard')
        
        @self.socketio.on('request_data')
        def handle_data_request(data):
            """Handle real-time data requests."""
            try:
                data_type = data.get('type', 'metrics')
                
                if data_type == 'metrics':
                    # Send current metrics
                    metrics_data = self._get_current_metrics()
                    emit('metrics_update', metrics_data)
                
                elif data_type == 'alerts':
                    # Send current alerts
                    alerts_data = self._get_current_alerts()
                    emit('alerts_update', alerts_data)
                
                elif data_type == 'health':
                    # Send health status
                    health_data = self._get_current_health()
                    emit('health_update', health_data)
                    
            except Exception as e:
                logger.error(f"Error handling data request: {e}")
                emit('error', {'message': str(e)})
    
    def _get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics for real-time updates."""
        try:
            # Key dashboard metrics
            metrics = {}
            
            # Pipeline metrics
            for metric_name in ['pipeline_executions_total', 'pipeline_success_total', 
                              'pipeline_failure_total', 'data_rows_processed_total']:
                aggregated = self.metrics_collector.get_aggregated_metrics(metric_name, '5m')
                if aggregated:
                    metrics[metric_name] = aggregated.get('latest', 0)
                else:
                    metrics[metric_name] = 0
            
            # System metrics
            for metric_name in ['system_cpu_usage_percent', 'system_memory_usage_percent']:
                aggregated = self.metrics_collector.get_aggregated_metrics(metric_name, '1m')
                if aggregated:
                    metrics[metric_name] = round(aggregated.get('latest', 0), 1)
                else:
                    metrics[metric_name] = 0
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting current metrics: {e}")
            return {}
    
    def _get_current_alerts(self) -> List[Dict[str, Any]]:
        """Get current alerts for real-time updates."""
        try:
            active_alerts = self.alert_manager.get_active_alerts()
            return [alert.to_dict() for alert in active_alerts]
        except Exception as e:
            logger.error(f"Error getting current alerts: {e}")
            return []
    
    def _get_current_health(self) -> Dict[str, Any]:
        """Get current health status for real-time updates."""
        try:
            return self.health_checker.run_checks()
        except Exception as e:
            logger.error(f"Error getting current health: {e}")
            return {'overall_healthy': False, 'error': str(e)}
    
    def start_background_updates(self):
        """Start background task for real-time updates."""
        def update_clients():
            while True:
                try:
                    # Send metrics update
                    metrics_data = self._get_current_metrics()
                    self.socketio.emit('metrics_update', metrics_data)
                    
                    # Send alerts update
                    alerts_data = self._get_current_alerts()
                    self.socketio.emit('alerts_update', alerts_data)
                    
                    # Send health update
                    health_data = self._get_current_health()
                    self.socketio.emit('health_update', health_data)
                    
                    # Check for new alerts
                    new_alerts = self.alert_manager.check_alerts()
                    if new_alerts:
                        self.socketio.emit('new_alert', [alert.to_dict() for alert in new_alerts])
                    
                    asyncio.sleep(self.update_interval)
                    
                except Exception as e:
                    logger.error(f"Error in background updates: {e}")
                    asyncio.sleep(self.update_interval)
        
        # Start background task
        self.socketio.start_background_task(update_clients)
    
    def run(self):
        """Start the dashboard server."""
        logger.info(f"Starting monitoring dashboard at http://{self.host}:{self.port}")
        
        # Start background updates
        self.start_background_updates()
        
        # Run the Flask app with SocketIO
        self.socketio.run(self.app, host=self.host, port=self.port, debug=self.debug)


# HTML template for the dashboard
DASHBOARD_HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Pipeline Monitoring Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background-color: #f5f5f5; 
            color: #333;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .dashboard-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            padding: 20px;
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .card:hover {
            transform: translateY(-2px);
        }
        
        .card-title {
            font-size: 1.3em;
            font-weight: 600;
            margin-bottom: 15px;
            color: #4a5568;
            border-bottom: 2px solid #e2e8f0;
            padding-bottom: 10px;
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            grid-column: 1 / -1;
        }
        
        .metric-card {
            background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 5px;
        }
        
        .metric-label {
            font-size: 0.9em;
            opacity: 0.9;
        }
        
        .alert {
            padding: 10px;
            margin: 5px 0;
            border-radius: 5px;
            border-left: 4px solid;
        }
        
        .alert.warning {
            background-color: #fff3cd;
            border-color: #ffc107;
            color: #856404;
        }
        
        .alert.critical {
            background-color: #f8d7da;
            border-color: #dc3545;
            color: #721c24;
        }
        
        .alert.info {
            background-color: #d1ecf1;
            border-color: #17a2b8;
            color: #0c5460;
        }
        
        .health-status {
            display: flex;
            align-items: center;
            padding: 10px;
            margin: 5px 0;
            border-radius: 5px;
            background-color: #f8f9fa;
        }
        
        .health-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 10px;
        }
        
        .health-indicator.healthy { background-color: #28a745; }
        .health-indicator.unhealthy { background-color: #dc3545; }
        
        .status-indicator {
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 10px 20px;
            border-radius: 25px;
            color: white;
            font-weight: bold;
            z-index: 1000;
        }
        
        .status-indicator.connected {
            background-color: #28a745;
        }
        
        .status-indicator.disconnected {
            background-color: #dc3545;
        }
        
        .chart-container {
            position: relative;
            height: 300px;
            margin-top: 10px;
        }
        
        @media (max-width: 768px) {
            .dashboard-container {
                grid-template-columns: 1fr;
            }
            
            .metrics-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 Data Pipeline Monitoring Dashboard</h1>
        <p>Real-time monitoring and observability for your data pipelines</p>
    </div>
    
    <div class="status-indicator" id="connectionStatus">Connecting...</div>
    
    <div class="dashboard-container">
        <!-- Key Metrics -->
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value" id="totalExecutions">0</div>
                <div class="metric-label">Total Executions</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-value" id="successRate">0%</div>
                <div class="metric-label">Success Rate</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-value" id="rowsProcessed">0</div>
                <div class="metric-label">Rows Processed</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-value" id="activeAlerts">0</div>
                <div class="metric-label">Active Alerts</div>
            </div>
        </div>
        
        <!-- System Metrics Chart -->
        <div class="card">
            <div class="card-title">📊 System Performance</div>
            <div class="chart-container">
                <canvas id="systemChart"></canvas>
            </div>
        </div>
        
        <!-- Pipeline Metrics Chart -->
        <div class="card">
            <div class="card-title">⚡ Pipeline Performance</div>
            <div class="chart-container">
                <canvas id="pipelineChart"></canvas>
            </div>
        </div>
        
        <!-- Active Alerts -->
        <div class="card">
            <div class="card-title">🚨 Active Alerts</div>
            <div id="alertsContainer">
                <p style="text-align: center; color: #28a745;">No active alerts</p>
            </div>
        </div>
        
        <!-- Health Status -->
        <div class="card">
            <div class="card-title">💚 System Health</div>
            <div id="healthContainer">
                <div class="health-status">
                    <div class="health-indicator healthy"></div>
                    <span>All systems operational</span>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Initialize Socket.IO connection
        const socket = io();
        
        // Connection status indicator
        const statusIndicator = document.getElementById('connectionStatus');
        
        // Chart configurations
        const systemChart = new Chart(document.getElementById('systemChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'CPU Usage (%)',
                    data: [],
                    borderColor: '#ff6b6b',
                    tension: 0.4
                }, {
                    label: 'Memory Usage (%)',
                    data: [],
                    borderColor: '#4ecdc4',
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true, max: 100 }
                }
            }
        });
        
        const pipelineChart = new Chart(document.getElementById('pipelineChart'), {
            type: 'bar',
            data: {
                labels: ['Executions', 'Successes', 'Failures'],
                datasets: [{
                    label: 'Count',
                    data: [0, 0, 0],
                    backgroundColor: ['#74b9ff', '#00b894', '#fd79a8']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false
            }
        });
        
        // Socket event handlers
        socket.on('connect', function() {
            statusIndicator.textContent = 'Connected';
            statusIndicator.className = 'status-indicator connected';
            socket.emit('request_data', {type: 'metrics'});
            socket.emit('request_data', {type: 'alerts'});
            socket.emit('request_data', {type: 'health'});
        });
        
        socket.on('disconnect', function() {
            statusIndicator.textContent = 'Disconnected';
            statusIndicator.className = 'status-indicator disconnected';
        });
        
        socket.on('metrics_update', function(data) {
            updateMetrics(data);
            updateCharts(data);
        });
        
        socket.on('alerts_update', function(data) {
            updateAlerts(data);
        });
        
        socket.on('health_update', function(data) {
            updateHealth(data);
        });
        
        socket.on('new_alert', function(data) {
            // Handle new alert notification
            console.log('New alert:', data);
        });
        
        // Update functions
        function updateMetrics(data) {
            document.getElementById('totalExecutions').textContent = 
                data.pipeline_executions_total || 0;
            
            const successes = data.pipeline_success_total || 0;
            const total = data.pipeline_executions_total || 0;
            const successRate = total > 0 ? Math.round((successes / total) * 100) : 0;
            document.getElementById('successRate').textContent = successRate + '%';
            
            document.getElementById('rowsProcessed').textContent = 
                (data.data_rows_processed_total || 0).toLocaleString();
        }
        
        function updateCharts(data) {
            const now = new Date().toLocaleTimeString();
            
            // Update system chart
            if (systemChart.data.labels.length > 20) {
                systemChart.data.labels.shift();
                systemChart.data.datasets[0].data.shift();
                systemChart.data.datasets[1].data.shift();
            }
            
            systemChart.data.labels.push(now);
            systemChart.data.datasets[0].data.push(data.system_cpu_usage_percent || 0);
            systemChart.data.datasets[1].data.push(data.system_memory_usage_percent || 0);
            systemChart.update();
            
            // Update pipeline chart
            pipelineChart.data.datasets[0].data = [
                data.pipeline_executions_total || 0,
                data.pipeline_success_total || 0,
                data.pipeline_failure_total || 0
            ];
            pipelineChart.update();
        }
        
        function updateAlerts(alerts) {
            const container = document.getElementById('alertsContainer');
            
            if (alerts.length === 0) {
                container.innerHTML = '<p style="text-align: center; color: #28a745;">No active alerts</p>';
                document.getElementById('activeAlerts').textContent = '0';
                return;
            }
            
            document.getElementById('activeAlerts').textContent = alerts.length;
            
            container.innerHTML = alerts.map(alert => `
                <div class="alert ${alert.level}">
                    <strong>${alert.name}</strong><br>
                    ${alert.message}<br>
                    <small>${new Date(alert.timestamp).toLocaleString()}</small>
                </div>
            `).join('');
        }
        
        function updateHealth(health) {
            const container = document.getElementById('healthContainer');
            
            if (!health.checks) {
                container.innerHTML = '<div class="health-status"><div class="health-indicator unhealthy"></div><span>Health check failed</span></div>';
                return;
            }
            
            const healthItems = Object.entries(health.checks).map(([name, status]) => `
                <div class="health-status">
                    <div class="health-indicator ${status.healthy ? 'healthy' : 'unhealthy'}"></div>
                    <span>${name}: ${status.healthy ? 'Healthy' : 'Unhealthy'} (${status.duration_ms}ms)</span>
                </div>
            `).join('');
            
            container.innerHTML = healthItems;
        }
        
        // Periodic data requests
        setInterval(() => {
            if (socket.connected) {
                socket.emit('request_data', {type: 'metrics'});
                socket.emit('request_data', {type: 'alerts'});
                socket.emit('request_data', {type: 'health'});
            }
        }, 5000);
    </script>
</body>
</html>
'''


def create_dashboard(host: str = '127.0.0.1', port: int = 8050, debug: bool = False) -> DashboardServer:
    """Create a monitoring dashboard instance."""
    return DashboardServer(host, port, debug)


def start_dashboard(host: str = '127.0.0.1', port: int = 8050, debug: bool = False):
    """Start the monitoring dashboard server."""
    dashboard = create_dashboard(host, port, debug)
    dashboard.run()