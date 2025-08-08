"""Pytest configuration and fixtures."""

import os
import tempfile
from pathlib import Path
import pytest
import pandas as pd
from sqlalchemy import create_engine
from unittest.mock import Mock

# Set test environment variables
os.environ.update({
    'DB_HOST': 'localhost',
    'DB_PORT': '5432',
    'DB_NAME': 'test_db',
    'DB_USER': 'postgres',
    'DB_PASSWORD': 'postgres',
    'PIPELINE_ENV': 'test',
    'LOG_LEVEL': 'ERROR'  # Reduce log noise during tests
})


@pytest.fixture(scope="session")
def test_database_url():
    """Database URL for testing."""
    return "postgresql://postgres:postgres@localhost:5432/test_db"


@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 
                  'diana@example.com', 'eve@example.com'],
        'salary': [50000, 60000, 75000, 55000, 68000],
        'department': ['Engineering', 'Sales', 'Engineering', 'Marketing', 'Sales']
    })


@pytest.fixture
def sample_json_data():
    """Sample JSON data for testing."""
    return [
        {'id': 1, 'name': 'Product A', 'price': 19.99, 'category': 'Electronics'},
        {'id': 2, 'name': 'Product B', 'price': 29.99, 'category': 'Books'},
        {'id': 3, 'name': 'Product C', 'price': 39.99, 'category': 'Clothing'},
    ]


@pytest.fixture
def temp_csv_file(sample_csv_data):
    """Create a temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_csv_data.to_csv(f.name, index=False)
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_json_file(sample_json_data):
    """Create a temporary JSON file."""
    import json
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(sample_json_data, f)
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_directory():
    """Create a temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'name': 'test_db',
            'user': 'postgres',
            'password': 'postgres'
        },
        'processing': {
            'batch_size': 100,
            'max_workers': 2
        },
        'validation': {
            'enabled': True
        }
    }


@pytest.fixture
def mock_db_engine(test_database_url):
    """Mock database engine."""
    try:
        engine = create_engine(test_database_url)
        # Test connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        yield engine
    except Exception:
        # If real database is not available, use mock
        mock_engine = Mock()
        yield mock_engine


@pytest.fixture
def sample_pipeline_config():
    """Sample pipeline configuration."""
    return {
        'name': 'test_pipeline',
        'description': 'Test pipeline configuration',
        'source': {
            'type': 'csv',
            'config': {
                'base_path': './test_data',
                'encoding': 'utf-8',
                'delimiter': ','
            }
        },
        'processing': {
            'engine': 'pandas',
            'operations': [
                {
                    'type': 'clean',
                    'params': {
                        'operations': ['remove_empty_rows', 'trim_strings']
                    }
                }
            ]
        },
        'storage': {
            'type': 'postgresql',
            'destination': 'test_table',
            'mode': 'replace'
        },
        'validation': {
            'enabled': True
        }
    }