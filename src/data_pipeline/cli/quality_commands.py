"""CLI commands for data quality monitoring."""

import json
from typing import Optional, List
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from ..utils.quality_monitor import DataQualityMonitor
from ..core.config import ConfigManager

console = Console()


@click.group(name="quality")
def quality_commands():
    """Data quality monitoring commands."""
    pass


@quality_commands.command()
@click.argument("table_name")
@click.option("--schema", "-s", default="public", help="Schema name")
@click.option("--config", "-c", type=click.Path(exists=True), help="Quality config file")
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table", help="Output format")
def check(table_name: str, schema: str, config: Optional[str], output: str):
    """Run data quality checks on a table."""
    
    try:
        # Load configuration
        config_manager = ConfigManager()
        db_config = config_manager.get_database_config()
        db_url = config_manager.get_database_url()
        
        # Load quality checks configuration
        quality_config = {}
        if config:
            with open(config, 'r') as f:
                quality_config = json.load(f)
        
        # Initialize quality monitor
        monitor = DataQualityMonitor(db_url, quality_config.get('monitoring', {}))
        
        console.print(f"Running quality checks on [bold]{schema}.{table_name}[/bold]...")
        
        # Run quality checks
        checks = quality_config.get('checks', [])
        metrics = monitor.run_quality_checks(table_name, schema, checks)
        
        if not metrics:
            console.print("[yellow]No metrics calculated[/yellow]")
            return
        
        # Check for alerts
        alerts = monitor.check_alerts(metrics)
        
        # Output results
        if output == "json":
            result = {
                'table': f"{schema}.{table_name}",
                'metrics': [
                    {
                        'metric': m.metric_name,
                        'column': m.column_name,
                        'value': m.metric_value,
                        'threshold': m.threshold_value,
                        'status': m.status
                    }
                    for m in metrics
                ],
                'alerts': [
                    {
                        'alert_id': a.alert_id,
                        'severity': a.severity,
                        'message': a.message
                    }
                    for a in alerts
                ]
            }
            console.print(json.dumps(result, indent=2, default=str))
        else:
            _display_quality_results(metrics, alerts, table_name)
        
        # Send alerts if configured
        if alerts and quality_config.get('monitoring', {}).get('send_alerts', False):
            monitor.send_alerts(alerts)
            console.print(f"[yellow]Sent {len(alerts)} alerts[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error running quality checks: {e}[/red]")
        raise click.ClickException(str(e))


@quality_commands.command()
@click.option("--table", "-t", help="Filter by table name")
@click.option("--limit", "-l", default=50, help="Number of recent metrics to show")
def dashboard(table: Optional[str], limit: int):
    """Show data quality dashboard."""
    
    try:
        config_manager = ConfigManager()
        db_url = config_manager.get_database_url()
        
        monitor = DataQualityMonitor(db_url)
        dashboard_data = monitor.get_quality_dashboard_data(table)
        
        _display_dashboard(dashboard_data, table)
        
    except Exception as e:
        console.print(f"[red]Error loading dashboard: {e}[/red]")
        raise click.ClickException(str(e))


@quality_commands.command()
@click.argument("table_name")
@click.option("--schema", "-s", default="public", help="Schema name")
@click.option("--output", "-o", type=click.Path(), help="Output file path")
def generate_config(table_name: str, schema: str, output: Optional[str]):
    """Generate a quality configuration template for a table."""
    
    try:
        config_manager = ConfigManager()
        db_url = config_manager.get_database_url()
        
        # Connect and analyze table structure
        from sqlalchemy import create_engine, inspect
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        columns = inspector.get_columns(table_name, schema=schema)
        
        # Generate basic quality checks configuration
        config = {
            "table": f"{schema}.{table_name}",
            "checks": [],
            "monitoring": {
                "send_alerts": True,
                "email_enabled": False,
                "slack_enabled": False,
                "alert_recipients": ["admin@company.com"],
                "slack_webhook_url": "${SLACK_WEBHOOK_URL}"
            }
        }
        
        # Add checks based on column types
        for col in columns:
            col_name = col['name']
            col_type = str(col['type'])
            
            # Completeness check for all columns
            config["checks"].append({
                "metric": "completeness",
                "column": col_name,
                "threshold": 0.95
            })
            
            # Uniqueness for ID columns
            if any(keyword in col_name.lower() for keyword in ['id', 'key', 'code']):
                config["checks"].append({
                    "metric": "uniqueness",
                    "column": col_name,
                    "threshold": 1.0
                })
            
            # Outlier detection for numeric columns
            if any(type_name in col_type.lower() for type_name in ['int', 'float', 'numeric', 'decimal']):
                config["checks"].append({
                    "metric": "outlier_detection",
                    "column": col_name,
                    "threshold": 0.95,
                    "params": {
                        "method": "iqr"
                    }
                })
        
        # Output configuration
        config_json = json.dumps(config, indent=2)
        
        if output:
            with open(output, 'w') as f:
                f.write(config_json)
            console.print(f"[green]Quality configuration saved to {output}[/green]")
        else:
            console.print(config_json)
        
    except Exception as e:
        console.print(f"[red]Error generating config: {e}[/red]")
        raise click.ClickException(str(e))


def _display_quality_results(metrics, alerts, table_name: str):
    """Display quality check results in a table format."""
    
    # Display metrics table
    table = Table(title=f"Data Quality Results: {table_name}", box=box.ROUNDED)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Column", style="magenta")
    table.add_column("Value", justify="right", style="green")
    table.add_column("Threshold", justify="right", style="yellow")
    table.add_column("Status", justify="center")
    
    for metric in metrics:
        status_style = {
            'PASS': '[green]✓ PASS[/green]',
            'WARN': '[yellow]⚠ WARN[/yellow]',
            'FAIL': '[red]✗ FAIL[/red]'
        }.get(metric.status, metric.status)
        
        table.add_row(
            metric.metric_name,
            metric.column_name or "-",
            f"{metric.metric_value:.2%}",
            f"{metric.threshold_value:.2%}",
            status_style
        )
    
    console.print(table)
    
    # Display alerts if any
    if alerts:
        console.print("\n")
        for alert in alerts:
            severity_color = "red" if alert.severity == "CRITICAL" else "yellow"
            panel = Panel(
                alert.message,
                title=f"[{severity_color}]{alert.severity} Alert[/{severity_color}]",
                border_style=severity_color
            )
            console.print(panel)
    else:
        console.print("\n[green]No alerts generated ✓[/green]")


def _display_dashboard(dashboard_data: dict, table_filter: Optional[str]):
    """Display the data quality dashboard."""
    
    summary = dashboard_data.get('summary', {})
    table_scores = dashboard_data.get('table_scores', {})
    recent_metrics = dashboard_data.get('recent_metrics', [])
    
    # Summary panel
    total_checks = summary.get('total_checks', 0)
    if total_checks > 0:
        pass_rate = (summary.get('passed', 0) / total_checks) * 100
        summary_text = f"""
Total Checks: {total_checks}
Passed: {summary.get('passed', 0)} ({pass_rate:.1f}%)
Warnings: {summary.get('warnings', 0)}
Failures: {summary.get('failures', 0)}
"""
    else:
        summary_text = "No quality checks found"
    
    title = "Data Quality Dashboard"
    if table_filter:
        title += f" - {table_filter}"
    
    console.print(Panel(summary_text, title=title, border_style="blue"))
    
    # Table scores
    if table_scores:
        scores_table = Table(title="Table Quality Scores", box=box.ROUNDED)
        scores_table.add_column("Table", style="cyan", no_wrap=True)
        scores_table.add_column("Quality Score", justify="right", style="green")
        scores_table.add_column("Status", justify="center")
        
        for table_name, score in table_scores.items():
            if score >= 0.95:
                status = "[green]✓ Excellent[/green]"
            elif score >= 0.8:
                status = "[yellow]⚠ Good[/yellow]"
            else:
                status = "[red]✗ Poor[/red]"
            
            scores_table.add_row(
                table_name,
                f"{score:.2%}",
                status
            )
        
        console.print(scores_table)
    
    # Recent metrics
    if recent_metrics:
        recent_table = Table(title="Recent Quality Checks", box=box.ROUNDED)
        recent_table.add_column("Table", style="cyan")
        recent_table.add_column("Metric", style="magenta")
        recent_table.add_column("Value", justify="right", style="green")
        recent_table.add_column("Status", justify="center")
        recent_table.add_column("Date", style="dim")
        
        for metric in recent_metrics[:20]:  # Show last 20
            status_style = {
                'PASS': '[green]✓[/green]',
                'WARN': '[yellow]⚠[/yellow]',
                'FAIL': '[red]✗[/red]'
            }.get(metric['status'], metric['status'])
            
            recent_table.add_row(
                metric['table_name'],
                metric['metric_name'],
                f"{metric['metric_value']:.3f}",
                status_style,
                str(metric['created_at'])[:19]  # Remove microseconds
            )
        
        console.print(recent_table)