"""PostgreSQL storage implementation."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
import logging
from ..core.base import DataStorage

logger = logging.getLogger(__name__)


class PostgreSQLStorage(DataStorage):
    """Storage backend for PostgreSQL database."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Database connection parameters
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5432)
        self.database = config.get('database', 'myapp_db')
        self.username = config.get('username', 'jacob')
        self.password = config.get('password', '')
        self.pool_size = config.get('pool_size', 10)
        self.max_overflow = config.get('max_overflow', 20)
        self.echo = config.get('echo', False)
        
        # Schema settings
        self.default_schema = config.get('schema', 'public')
        
        # Create database engine
        self.engine = self._create_engine()
        self.metadata = MetaData()
        
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine for PostgreSQL."""
        connection_string = (
            f"postgresql://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
        
        return create_engine(
            connection_string,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            echo=self.echo
        )
    
    def write(self, data: Union[pd.DataFrame, Any], destination: str, 
             if_exists: str = 'append', index: bool = False, 
             chunk_size: int = 10000, method: str = 'multi', **kwargs) -> bool:
        """Write data to PostgreSQL table."""
        try:
            # Handle non-pandas data
            if not isinstance(data, pd.DataFrame):
                if hasattr(data, 'toPandas'):  # Spark DataFrame
                    data = data.toPandas()
                else:
                    raise ValueError("Data must be a pandas DataFrame or Spark DataFrame")
            
            # Parse table name and schema
            schema, table = self._parse_destination(destination)
            
            # Write data using pandas to_sql
            data.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
                chunksize=chunk_size,
                method=method,
                **kwargs
            )
            
            logger.info(f"Successfully wrote {len(data)} rows to {schema}.{table}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing data to {destination}: {e}")
            raise
    
    def read(self, source: str, query: Optional[str] = None, 
            columns: Optional[List[str]] = None, limit: Optional[int] = None, 
            where_clause: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Read data from PostgreSQL table or execute custom query."""
        try:
            if query:
                # Execute custom SQL query
                return pd.read_sql(query, self.engine, **kwargs)
            else:
                # Build query from table name and parameters
                schema, table = self._parse_destination(source)
                full_table = f'"{schema}"."{table}"'
                
                # Build SELECT statement
                if columns:
                    cols = ', '.join([f'"{col}"' for col in columns])
                    sql_query = f"SELECT {cols} FROM {full_table}"
                else:
                    sql_query = f"SELECT * FROM {full_table}"
                
                # Add WHERE clause
                if where_clause:
                    sql_query += f" WHERE {where_clause}"
                
                # Add LIMIT
                if limit:
                    sql_query += f" LIMIT {limit}"
                
                return pd.read_sql(sql_query, self.engine, **kwargs)
                
        except Exception as e:
            logger.error(f"Error reading data from {source}: {e}")
            raise
    
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List all tables in the database or specific schema."""
        if schema is None:
            schema = self.default_schema
        
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :schema
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                return [row[0] for row in result]
                
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive information about a table."""
        schema, table = self._parse_destination(table_name)
        
        try:
            # Get basic table info
            table_info_query = """
                SELECT 
                    schemaname,
                    tablename,
                    tableowner,
                    hasindexes,
                    hasrules,
                    hastriggers,
                    rowsecurity
                FROM pg_tables 
                WHERE schemaname = :schema AND tablename = :table
            """
            
            # Get column information
            columns_query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """
            
            # Get table size
            size_query = """
                SELECT 
                    pg_size_pretty(pg_total_relation_size(:full_table_name1)) as total_size,
                    pg_size_pretty(pg_relation_size(:full_table_name2)) as table_size,
                    reltuples::BIGINT as estimated_rows
                FROM pg_class 
                WHERE relname = :table
            """
            
            with self.engine.connect() as conn:
                # Basic table info
                table_result = conn.execute(text(table_info_query), {"schema": schema, "table": table})
                table_row = table_result.fetchone()
                
                if not table_row:
                    raise ValueError(f"Table {schema}.{table} not found")
                
                # Column info
                columns_result = conn.execute(text(columns_query), {"schema": schema, "table": table})
                columns = []
                for row in columns_result:
                    columns.append({
                        'name': row[0],
                        'data_type': row[1],
                        'is_nullable': row[2] == 'YES',
                        'column_default': row[3],
                        'character_maximum_length': row[4],
                        'numeric_precision': row[5],
                        'numeric_scale': row[6]
                    })
                
                # Size info
                full_table_name = f'"{schema}"."{table}"'
                size_result = conn.execute(text(size_query), {"full_table_name1": full_table_name, "full_table_name2": full_table_name, "table": table})
                size_row = size_result.fetchone()
                
                return {
                    'schema': schema,
                    'table': table,
                    'owner': table_row[2],
                    'has_indexes': table_row[3],
                    'has_rules': table_row[4],
                    'has_triggers': table_row[5],
                    'row_security': table_row[6],
                    'columns': columns,
                    'size_info': {
                        'total_size': size_row[0] if size_row else 'Unknown',
                        'table_size': size_row[1] if size_row else 'Unknown',
                        'estimated_rows': size_row[2] if size_row else 0
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            raise
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str, 
                                   if_exists: str = 'fail', index: bool = False) -> bool:
        """Create a table from DataFrame schema."""
        try:
            schema, table = self._parse_destination(table_name)
            
            # Use pandas to create the table structure
            df.head(0).to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=index
            )
            
            logger.info(f"Successfully created table {schema}.{table}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """Execute a raw SQL query."""
        try:
            if params:
                return pd.read_sql(query, self.engine, params=params)
            else:
                return pd.read_sql(query, self.engine)
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def bulk_insert(self, data: pd.DataFrame, table_name: str, 
                   on_conflict: str = 'ignore', conflict_columns: List[str] = None) -> bool:
        """Perform bulk insert with conflict resolution."""
        try:
            schema, table = self._parse_destination(table_name)
            
            # Convert DataFrame to list of dictionaries
            records = data.to_dict('records')
            
            with self.engine.connect() as conn:
                # Create table object
                metadata = MetaData()
                table_obj = Table(table, metadata, autoload_with=self.engine, schema=schema)
                
                # Create insert statement
                stmt = insert(table_obj)
                
                # Handle conflicts
                if on_conflict == 'update' and conflict_columns:
                    # ON CONFLICT DO UPDATE
                    update_dict = {col.name: stmt.excluded[col.name] 
                                 for col in table_obj.columns if col.name not in conflict_columns}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_columns,
                        set_=update_dict
                    )
                elif on_conflict == 'ignore':
                    # ON CONFLICT DO NOTHING
                    stmt = stmt.on_conflict_do_nothing()
                
                # Execute bulk insert
                conn.execute(stmt, records)
                conn.commit()
                
            logger.info(f"Successfully bulk inserted {len(records)} records to {schema}.{table}")
            return True
            
        except Exception as e:
            logger.error(f"Error in bulk insert to {table_name}: {e}")
            raise
    
    def drop_table(self, table_name: str) -> bool:
        """Drop a table."""
        try:
            schema, table = self._parse_destination(table_name)
            
            with self.engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}"'))
                conn.commit()
                
            logger.info(f"Successfully dropped table {schema}.{table}")
            return True
            
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            raise
    
    def _parse_destination(self, destination: str) -> tuple[str, str]:
        """Parse destination into schema and table name."""
        if '.' in destination:
            schema, table = destination.split('.', 1)
        else:
            schema = self.default_schema
            table = destination
        
        return schema, table
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")