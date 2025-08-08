"""Configuration management for the data pipeline."""

import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DatabaseConfig(BaseModel):
    """Database configuration."""
    type: str = "postgresql"
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="data_warehouse")
    username: str = Field(default="postgres")
    password: str = Field(default="")
    pool_size: int = Field(default=10)
    max_overflow: int = Field(default=20)
    echo: bool = Field(default=False)


class StorageConfig(BaseModel):
    """Storage configuration."""
    
    class LocalConfig(BaseModel):
        base_path: str = "./data"
        raw_data: str = "raw"
        processed_data: str = "processed"
        external_data: str = "external"
    
    class S3Config(BaseModel):
        bucket: Optional[str] = None
        region: str = "us-east-1"
        prefix: str = "data-pipeline"
        access_key_id: Optional[str] = None
        secret_access_key: Optional[str] = None
    
    local: LocalConfig = LocalConfig()
    s3: S3Config = S3Config()
    primary: str = "local"


class ProcessingConfig(BaseModel):
    """Processing engine configuration."""
    
    class SparkConfig(BaseModel):
        app_name: str = "data-pipeline-spark"
        master: str = "local[*]"
        config: Dict[str, str] = Field(default_factory=dict)
    
    default_engine: str = "pandas"
    batch_size: int = 10000
    memory_limit_gb: int = 8
    spark: SparkConfig = SparkConfig()


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers: Dict[str, Any] = Field(default_factory=dict)


class ValidationConfig(BaseModel):
    """Data validation configuration."""
    enabled: bool = True
    store_results: bool = True
    fail_on_error: bool = False


class MonitoringConfig(BaseModel):
    """Monitoring configuration."""
    enabled: bool = True
    metrics_backend: str = "prometheus"
    health_check_interval: int = 30


class Config(BaseModel):
    """Main configuration class."""
    name: str = "data-pipeline"
    database: DatabaseConfig = DatabaseConfig()
    storage: StorageConfig = StorageConfig()
    processing: ProcessingConfig = ProcessingConfig()
    logging: LoggingConfig = LoggingConfig()
    validation: ValidationConfig = ValidationConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    
    @classmethod
    def from_yaml(cls, config_path: str, environment: str = "development") -> "Config":
        """Load configuration from YAML file."""
        config_dir = Path(config_path)
        env_file = config_dir / f"{environment}.yaml"
        
        if not env_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {env_file}")
        
        # Load base config if it exists
        base_file = config_dir / "base.yaml"
        config_data = {}
        
        if base_file.exists():
            with open(base_file, 'r') as f:
                base_config = yaml.safe_load(f)
                if base_config:
                    config_data.update(base_config)
        
        # Load environment-specific config
        with open(env_file, 'r') as f:
            env_config = yaml.safe_load(f)
            if env_config:
                config_data.update(env_config)
        
        # Substitute environment variables
        config_data = cls._substitute_env_vars(config_data)
        
        return cls(**config_data)
    
    @staticmethod
    def _substitute_env_vars(data: Any) -> Any:
        """Recursively substitute environment variables in configuration."""
        if isinstance(data, dict):
            return {key: Config._substitute_env_vars(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [Config._substitute_env_vars(item) for item in data]
        elif isinstance(data, str):
            # Handle ${VAR_NAME:default_value} pattern
            if data.startswith('${') and data.endswith('}'):
                var_spec = data[2:-1]  # Remove ${ and }
                if ':' in var_spec:
                    var_name, default_value = var_spec.split(':', 1)
                    return os.getenv(var_name, default_value)
                else:
                    return os.getenv(var_spec, data)
        return data
    
    def get_database_url(self) -> str:
        """Get database connection URL."""
        return (
            f"{self.database.type}://{self.database.username}:{self.database.password}"
            f"@{self.database.host}:{self.database.port}/{self.database.database}"
        )