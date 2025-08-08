"""Data catalog management utilities."""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Metadata for a database table."""
    table_name: str
    schema_name: str = "public"
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = None
    column_info: Optional[Dict[str, Any]] = None
    table_stats: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class ColumnInfo:
    """Information about a table column."""
    column_name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    description: Optional[str] = None
    data_quality_score: Optional[float] = None


class DataCatalog:
    """Manages data catalog and table metadata."""
    
    def __init__(self, db_url: str):
        """Initialize the data catalog.
        
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
    
    def register_table(
        self,
        table_name: str,
        schema_name: str = "public",
        description: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        auto_analyze: bool = True
    ) -> bool:
        """Register a table in the data catalog.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name
            description: Table description
            owner: Table owner
            tags: List of tags
            auto_analyze: Whether to automatically analyze the table structure
            
        Returns:
            Boolean indicating success
        """
        try:
            # Get column information if auto_analyze is enabled
            column_info = None
            table_stats = None
            
            if auto_analyze:
                column_info = self._analyze_table_structure(table_name, schema_name)
                table_stats = self._get_table_statistics(table_name, schema_name)
            
            with self.engine.connect() as conn:
                # Check if table already exists in catalog
                check_query = text("""
                    SELECT COUNT(*) as count 
                    FROM metadata.data_catalog 
                    WHERE table_name = :table_name AND schema_name = :schema_name
                """)
                
                result = conn.execute(check_query, {
                    'table_name': table_name,
                    'schema_name': schema_name
                })
                
                exists = result.scalar() > 0
                
                if exists:
                    # Update existing record
                    update_query = text("""
                        UPDATE metadata.data_catalog 
                        SET description = :description,
                            owner = :owner,
                            tags = :tags,
                            updated_at = :updated_at,
                            column_info = :column_info,
                            table_stats = :table_stats
                        WHERE table_name = :table_name AND schema_name = :schema_name
                    """)
                    
                    conn.execute(update_query, {
                        'table_name': table_name,
                        'schema_name': schema_name,
                        'description': description,
                        'owner': owner,
                        'tags': tags,
                        'updated_at': datetime.now(),
                        'column_info': json.dumps(column_info) if column_info else None,
                        'table_stats': json.dumps(table_stats) if table_stats else None
                    })
                else:
                    # Insert new record
                    insert_query = text("""
                        INSERT INTO metadata.data_catalog 
                        (table_name, schema_name, description, owner, tags, 
                         created_at, updated_at, column_info, table_stats)
                        VALUES (:table_name, :schema_name, :description, :owner, :tags,
                                :created_at, :updated_at, :column_info, :table_stats)
                    """)
                    
                    conn.execute(insert_query, {
                        'table_name': table_name,
                        'schema_name': schema_name,
                        'description': description,
                        'owner': owner,
                        'tags': tags,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now(),
                        'column_info': json.dumps(column_info) if column_info else None,
                        'table_stats': json.dumps(table_stats) if table_stats else None
                    })
                
                conn.commit()
                logger.info(f"Successfully registered table: {schema_name}.{table_name}")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to register table {table_name}: {e}")
            return False
    
    def _analyze_table_structure(self, table_name: str, schema_name: str) -> Dict[str, Any]:
        """Analyze table structure and return column information.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name
            
        Returns:
            Dictionary containing column information
        """
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=schema_name)
            primary_keys = inspector.get_primary_keys(table_name, schema=schema_name)
            foreign_keys = inspector.get_foreign_keys(table_name, schema=schema_name)
            
            # Build foreign key mapping
            fk_columns = set()
            for fk in foreign_keys:
                fk_columns.update(fk['constrained_columns'])
            
            column_info = {}
            for col in columns:
                column_info[col['name']] = {
                    'data_type': str(col['type']),
                    'is_nullable': col['nullable'],
                    'default_value': str(col['default']) if col['default'] else None,
                    'is_primary_key': col['name'] in primary_keys,
                    'is_foreign_key': col['name'] in fk_columns
                }
            
            return column_info
            
        except Exception as e:
            logger.error(f"Failed to analyze table structure: {e}")
            return {}
    
    def _get_table_statistics(self, table_name: str, schema_name: str) -> Dict[str, Any]:
        """Get basic statistics about the table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name
            
        Returns:
            Dictionary containing table statistics
        """
        try:
            with self.engine.connect() as conn:
                # Get row count
                count_query = text(f"""
                    SELECT COUNT(*) as row_count 
                    FROM {schema_name}.{table_name}
                """)
                
                result = conn.execute(count_query)
                row_count = result.scalar()
                
                # Get table size (PostgreSQL specific)
                size_query = text(f"""
                    SELECT pg_total_relation_size('{schema_name}.{table_name}') as size_bytes
                """)
                
                try:
                    result = conn.execute(size_query)
                    size_bytes = result.scalar()
                except:
                    size_bytes = None  # Not PostgreSQL or permission issue
                
                return {
                    'row_count': row_count,
                    'size_bytes': size_bytes,
                    'last_analyzed': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Failed to get table statistics: {e}")
            return {}
    
    def get_table_metadata(self, table_name: str, schema_name: str = "public") -> Optional[TableMetadata]:
        """Get metadata for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name
            
        Returns:
            TableMetadata object or None if not found
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT table_name, schema_name, description, owner, tags,
                           column_info, table_stats, created_at, updated_at
                    FROM metadata.data_catalog
                    WHERE table_name = :table_name AND schema_name = :schema_name
                """)
                
                result = conn.execute(query, {
                    'table_name': table_name,
                    'schema_name': schema_name
                })
                
                row = result.fetchone()
                
                if row:
                    return TableMetadata(
                        table_name=row.table_name,
                        schema_name=row.schema_name,
                        description=row.description,
                        owner=row.owner,
                        tags=row.tags if row.tags else [],
                        column_info=json.loads(row.column_info) if row.column_info else None,
                        table_stats=json.loads(row.table_stats) if row.table_stats else None,
                        created_at=row.created_at,
                        updated_at=row.updated_at
                    )
                
                return None
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get table metadata: {e}")
            return None
    
    def search_tables(
        self, 
        search_term: Optional[str] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None
    ) -> List[TableMetadata]:
        """Search for tables in the catalog.
        
        Args:
            search_term: Search in table name or description
            tags: Filter by tags
            owner: Filter by owner
            
        Returns:
            List of matching TableMetadata objects
        """
        try:
            with self.engine.connect() as conn:
                conditions = []
                params = {}
                
                if search_term:
                    conditions.append("(table_name ILIKE :search OR description ILIKE :search)")
                    params['search'] = f"%{search_term}%"
                
                if owner:
                    conditions.append("owner = :owner")
                    params['owner'] = owner
                
                if tags:
                    # PostgreSQL array overlap operator
                    conditions.append("tags && :tags")
                    params['tags'] = tags
                
                where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
                
                query = text(f"""
                    SELECT table_name, schema_name, description, owner, tags,
                           column_info, table_stats, created_at, updated_at
                    FROM metadata.data_catalog
                    {where_clause}
                    ORDER BY table_name
                """)
                
                result = conn.execute(query, params)
                
                tables = []
                for row in result:
                    tables.append(TableMetadata(
                        table_name=row.table_name,
                        schema_name=row.schema_name,
                        description=row.description,
                        owner=row.owner,
                        tags=row.tags if row.tags else [],
                        column_info=json.loads(row.column_info) if row.column_info else None,
                        table_stats=json.loads(row.table_stats) if row.table_stats else None,
                        created_at=row.created_at,
                        updated_at=row.updated_at
                    ))
                
                return tables
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to search tables: {e}")
            return []
    
    def generate_catalog_report(self) -> str:
        """Generate a comprehensive data catalog report.
        
        Returns:
            String containing the catalog report
        """
        tables = self.search_tables()
        
        report = []
        report.append("Data Catalog Report")
        report.append("=" * 50)
        report.append(f"Total Tables: {len(tables)}")
        report.append("")
        
        # Group by schema
        schemas = {}
        for table in tables:
            schema = table.schema_name
            if schema not in schemas:
                schemas[schema] = []
            schemas[schema].append(table)
        
        for schema_name, schema_tables in schemas.items():
            report.append(f"Schema: {schema_name}")
            report.append("-" * 30)
            
            for table in schema_tables:
                report.append(f"  • {table.table_name}")
                if table.description:
                    report.append(f"    Description: {table.description}")
                if table.owner:
                    report.append(f"    Owner: {table.owner}")
                if table.tags:
                    report.append(f"    Tags: {', '.join(table.tags)}")
                if table.table_stats:
                    stats = table.table_stats
                    if 'row_count' in stats:
                        report.append(f"    Rows: {stats['row_count']:,}")
                report.append("")
            
            report.append("")
        
        return "\n".join(report)