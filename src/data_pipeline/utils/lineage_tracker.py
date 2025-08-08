"""Data lineage tracking utilities for the data pipeline framework."""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass, asdict
import uuid

import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


@dataclass
class LineageNode:
    """Represents a node in the data lineage graph."""
    table_name: str
    schema_name: str = "public"
    database_name: Optional[str] = None
    node_type: str = "table"  # table, view, file, api
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class LineageEdge:
    """Represents a transformation/dependency between lineage nodes."""
    source: LineageNode
    target: LineageNode
    transformation_type: str
    pipeline_name: str
    created_by: str = "system"
    metadata: Optional[Dict[str, Any]] = None


class LineageTracker:
    """Tracks data lineage and transformation history."""
    
    def __init__(self, db_url: str):
        """Initialize the lineage tracker.
        
        Args:
            db_url: Database connection URL
        """
        self.engine = create_engine(db_url)
        self._ensure_schema_exists()
    
    def _ensure_schema_exists(self):
        """Ensure the metadata schema exists."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS metadata"))
                conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"Failed to create metadata schema: {e}")
            raise
    
    def track_transformation(
        self,
        source_tables: List[str],
        target_table: str,
        transformation_type: str,
        pipeline_name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Track a data transformation between tables.
        
        Args:
            source_tables: List of source table names
            target_table: Target table name
            transformation_type: Type of transformation (load, transform, aggregate, etc.)
            pipeline_name: Name of the pipeline performing the transformation
            metadata: Additional metadata about the transformation
            
        Returns:
            String: Unique ID for this lineage record
        """
        lineage_id = str(uuid.uuid4())
        
        try:
            with self.engine.connect() as conn:
                # Insert lineage record for each source table
                for source_table in source_tables:
                    insert_query = text("""
                        INSERT INTO metadata.data_lineage 
                        (id, source_table, target_table, transformation_type, 
                         pipeline_name, created_at, metadata)
                        VALUES (:id, :source_table, :target_table, :transformation_type,
                                :pipeline_name, :created_at, :metadata)
                    """)
                    
                    conn.execute(insert_query, {
                        'id': lineage_id,
                        'source_table': source_table,
                        'target_table': target_table,
                        'transformation_type': transformation_type,
                        'pipeline_name': pipeline_name,
                        'created_at': datetime.now(),
                        'metadata': json.dumps(metadata) if metadata else None
                    })
                
                conn.commit()
                logger.info(f"Tracked lineage: {source_tables} -> {target_table} via {pipeline_name}")
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to track lineage: {e}")
            raise
        
        return lineage_id
    
    def get_upstream_dependencies(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all upstream dependencies for a table.
        
        Args:
            table_name: Name of the target table
            
        Returns:
            List of upstream dependencies
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT source_table, transformation_type, 
                           pipeline_name, created_at, metadata
                    FROM metadata.data_lineage
                    WHERE target_table = :table_name
                    ORDER BY created_at DESC
                """)
                
                result = conn.execute(query, {'table_name': table_name})
                
                dependencies = []
                for row in result:
                    dependencies.append({
                        'source_table': row.source_table,
                        'transformation_type': row.transformation_type,
                        'pipeline_name': row.pipeline_name,
                        'created_at': row.created_at,
                        'metadata': json.loads(row.metadata) if row.metadata else {}
                    })
                
                return dependencies
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get upstream dependencies: {e}")
            return []
    
    def get_downstream_dependencies(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all downstream dependencies for a table.
        
        Args:
            table_name: Name of the source table
            
        Returns:
            List of downstream dependencies
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT target_table, transformation_type, 
                           pipeline_name, created_at, metadata
                    FROM metadata.data_lineage
                    WHERE source_table = :table_name
                    ORDER BY created_at DESC
                """)
                
                result = conn.execute(query, {'table_name': table_name})
                
                dependencies = []
                for row in result:
                    dependencies.append({
                        'target_table': row.target_table,
                        'transformation_type': row.transformation_type,
                        'pipeline_name': row.pipeline_name,
                        'created_at': row.created_at,
                        'metadata': json.loads(row.metadata) if row.metadata else {}
                    })
                
                return dependencies
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get downstream dependencies: {e}")
            return []
    
    def get_lineage_graph(self, table_name: str, depth: int = 2) -> Dict[str, Any]:
        """Generate a lineage graph for a table.
        
        Args:
            table_name: Starting table name
            depth: How many levels deep to traverse
            
        Returns:
            Dictionary representing the lineage graph
        """
        graph = {
            'nodes': set(),
            'edges': []
        }
        
        def traverse(current_table: str, current_depth: int, direction: str = 'both'):
            if current_depth > depth:
                return
            
            graph['nodes'].add(current_table)
            
            if direction in ['both', 'upstream']:
                upstream = self.get_upstream_dependencies(current_table)
                for dep in upstream:
                    source = dep['source_table']
                    graph['nodes'].add(source)
                    graph['edges'].append({
                        'source': source,
                        'target': current_table,
                        'transformation': dep['transformation_type'],
                        'pipeline': dep['pipeline_name']
                    })
                    traverse(source, current_depth + 1, 'upstream')
            
            if direction in ['both', 'downstream']:
                downstream = self.get_downstream_dependencies(current_table)
                for dep in downstream:
                    target = dep['target_table']
                    graph['nodes'].add(target)
                    graph['edges'].append({
                        'source': current_table,
                        'target': target,
                        'transformation': dep['transformation_type'],
                        'pipeline': dep['pipeline_name']
                    })
                    traverse(target, current_depth + 1, 'downstream')
        
        traverse(table_name, 0)
        
        return {
            'nodes': list(graph['nodes']),
            'edges': graph['edges']
        }
    
    def generate_lineage_report(self, table_name: str) -> str:
        """Generate a human-readable lineage report.
        
        Args:
            table_name: Table to generate report for
            
        Returns:
            String containing the lineage report
        """
        upstream = self.get_upstream_dependencies(table_name)
        downstream = self.get_downstream_dependencies(table_name)
        
        report = []
        report.append(f"Data Lineage Report for: {table_name}")
        report.append("=" * 50)
        report.append("")
        
        if upstream:
            report.append("Upstream Dependencies:")
            report.append("-" * 25)
            for dep in upstream:
                report.append(f"  • {dep['source_table']} → {table_name}")
                report.append(f"    Transformation: {dep['transformation_type']}")
                report.append(f"    Pipeline: {dep['pipeline_name']}")
                report.append(f"    Date: {dep['created_at']}")
                report.append("")
        else:
            report.append("No upstream dependencies found.")
            report.append("")
        
        if downstream:
            report.append("Downstream Dependencies:")
            report.append("-" * 27)
            for dep in downstream:
                report.append(f"  • {table_name} → {dep['target_table']}")
                report.append(f"    Transformation: {dep['transformation_type']}")
                report.append(f"    Pipeline: {dep['pipeline_name']}")
                report.append(f"    Date: {dep['created_at']}")
                report.append("")
        else:
            report.append("No downstream dependencies found.")
            report.append("")
        
        return "\n".join(report)