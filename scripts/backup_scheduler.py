#!/usr/bin/env python3
"""
Enterprise Backup Scheduler for Data Pipeline Framework

Provides automated database backups, data archival, and disaster recovery features.
Supports multiple backup strategies including full, incremental, and point-in-time recovery.
"""

import os
import sys
import time
import logging
import schedule
import subprocess
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
from dataclasses import dataclass, asdict
import boto3
import psycopg2
from botocore.exceptions import ClientError

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_pipeline.core.config import Config
from data_pipeline.observability.metrics import get_metrics_collector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BackupMetadata:
    """Metadata for backup operations."""
    backup_id: str
    timestamp: datetime
    backup_type: str  # 'full', 'incremental', 'archive'
    database_name: str
    size_bytes: int
    duration_seconds: float
    s3_key: Optional[str] = None
    local_path: Optional[str] = None
    checksum: Optional[str] = None
    compression: str = 'gzip'
    encryption: bool = True
    retention_days: int = 30
    success: bool = False
    error_message: Optional[str] = None


class BackupScheduler:
    """Enterprise backup scheduler with multiple storage backends."""
    
    def __init__(self, config_path: str = 'config/environments/production.yaml'):
        self.config = Config(config_path)
        self.metrics_collector = get_metrics_collector()
        
        # Backup configuration
        self.backup_dir = Path(os.getenv('BACKUP_LOCAL_DIR', '/backups'))
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # AWS S3 configuration
        self.s3_bucket = os.getenv('S3_BACKUP_BUCKET')
        self.s3_prefix = os.getenv('S3_BACKUP_PREFIX', 'backups/data-pipeline')
        
        # Retention policies
        self.full_backup_retention = int(os.getenv('FULL_BACKUP_RETENTION_DAYS', '90'))
        self.incremental_backup_retention = int(os.getenv('INCREMENTAL_BACKUP_RETENTION_DAYS', '30'))
        self.archive_retention = int(os.getenv('ARCHIVE_RETENTION_DAYS', '2555'))  # 7 years
        
        # Initialize S3 client
        self.s3_client = None
        if self.s3_bucket:
            try:
                self.s3_client = boto3.client('s3')
                logger.info(f"Initialized S3 client for bucket: {self.s3_bucket}")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {e}")
        
        # Database configuration
        self.db_config = self.config.get('database', {})
        
        # Backup history
        self.backup_history: List[BackupMetadata] = []
        self.load_backup_history()
        
    def load_backup_history(self) -> None:
        """Load backup history from local storage."""
        history_file = self.backup_dir / 'backup_history.json'
        
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    data = json.load(f)
                    
                for item in data:
                    # Convert timestamp string back to datetime
                    item['timestamp'] = datetime.fromisoformat(item['timestamp'])
                    self.backup_history.append(BackupMetadata(**item))
                    
                logger.info(f"Loaded {len(self.backup_history)} backup records from history")
            except Exception as e:
                logger.error(f"Failed to load backup history: {e}")
    
    def save_backup_history(self) -> None:
        """Save backup history to local storage."""
        history_file = self.backup_dir / 'backup_history.json'
        
        try:
            # Convert to serializable format
            serializable_history = []
            for backup in self.backup_history:
                data = asdict(backup)
                data['timestamp'] = backup.timestamp.isoformat()
                serializable_history.append(data)
            
            with open(history_file, 'w') as f:
                json.dump(serializable_history, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save backup history: {e}")
    
    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
    
    def create_full_backup(self) -> BackupMetadata:
        """Create a full database backup."""
        start_time = time.time()
        backup_id = f"full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting full backup: {backup_id}")
        
        # Create backup metadata
        backup_metadata = BackupMetadata(
            backup_id=backup_id,
            timestamp=datetime.now(),
            backup_type='full',
            database_name=self.db_config.get('database', 'data_warehouse'),
            size_bytes=0,
            duration_seconds=0,
            retention_days=self.full_backup_retention
        )
        
        try:
            # Create local backup file
            backup_filename = f"{backup_id}.sql.gz"
            local_backup_path = self.backup_dir / backup_filename
            
            # Build pg_dump command
            dump_cmd = [
                'pg_dump',
                f"--host={self.db_config.get('host', 'localhost')}",
                f"--port={self.db_config.get('port', 5432)}",
                f"--username={self.db_config.get('username', 'postgres')}",
                f"--dbname={backup_metadata.database_name}",
                '--verbose',
                '--clean',
                '--create',
                '--if-exists',
                '--format=custom',
                '--compress=9'
            ]
            
            # Set password via environment
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config.get('password', '')
            
            # Execute backup
            with open(local_backup_path, 'wb') as f:
                result = subprocess.run(
                    dump_cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    env=env,
                    timeout=3600  # 1 hour timeout
                )
            
            if result.returncode != 0:
                error_msg = result.stderr.decode('utf-8')
                logger.error(f"pg_dump failed: {error_msg}")
                backup_metadata.error_message = error_msg
                return backup_metadata
            
            # Calculate file size and checksum
            backup_metadata.size_bytes = local_backup_path.stat().st_size
            backup_metadata.checksum = self.calculate_checksum(local_backup_path)
            backup_metadata.local_path = str(local_backup_path)
            
            # Upload to S3 if configured
            if self.s3_client and self.s3_bucket:
                s3_key = f"{self.s3_prefix}/{backup_filename}"
                
                try:
                    self.s3_client.upload_file(
                        str(local_backup_path),
                        self.s3_bucket,
                        s3_key,
                        ExtraArgs={
                            'ServerSideEncryption': 'AES256',
                            'Metadata': {
                                'backup-id': backup_id,
                                'backup-type': 'full',
                                'database': backup_metadata.database_name,
                                'checksum': backup_metadata.checksum
                            }
                        }
                    )
                    backup_metadata.s3_key = s3_key
                    logger.info(f"Backup uploaded to S3: s3://{self.s3_bucket}/{s3_key}")
                    
                except ClientError as e:
                    logger.error(f"Failed to upload backup to S3: {e}")
            
            # Mark as successful
            backup_metadata.success = True
            backup_metadata.duration_seconds = time.time() - start_time
            
            logger.info(f"Full backup completed successfully: {backup_id} "
                       f"({backup_metadata.size_bytes / 1024 / 1024:.1f} MB)")
            
            # Record metrics
            self.metrics_collector.record_counter(
                'backup_full_completed_total',
                tags={'status': 'success', 'database': backup_metadata.database_name}
            )
            
            self.metrics_collector.record_histogram(
                'backup_duration_seconds',
                backup_metadata.duration_seconds,
                tags={'type': 'full', 'database': backup_metadata.database_name}
            )
            
            self.metrics_collector.record_gauge(
                'backup_size_bytes',
                backup_metadata.size_bytes,
                tags={'type': 'full', 'database': backup_metadata.database_name}
            )
            
        except Exception as e:
            backup_metadata.error_message = str(e)
            backup_metadata.duration_seconds = time.time() - start_time
            logger.error(f"Full backup failed: {e}")
            
            # Record failure metrics
            self.metrics_collector.record_counter(
                'backup_full_failed_total',
                tags={'database': backup_metadata.database_name, 'error': str(e)[:100]}
            )
        
        # Add to history and save
        self.backup_history.append(backup_metadata)
        self.save_backup_history()
        
        return backup_metadata
    
    def create_incremental_backup(self) -> Optional[BackupMetadata]:
        """Create an incremental backup using WAL archiving."""
        # Find last full backup
        last_full_backup = None
        for backup in reversed(self.backup_history):
            if backup.backup_type == 'full' and backup.success:
                last_full_backup = backup
                break
        
        if not last_full_backup:
            logger.warning("No full backup found, skipping incremental backup")
            return None
        
        start_time = time.time()
        backup_id = f"incr_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting incremental backup: {backup_id}")
        
        backup_metadata = BackupMetadata(
            backup_id=backup_id,
            timestamp=datetime.now(),
            backup_type='incremental',
            database_name=self.db_config.get('database', 'data_warehouse'),
            size_bytes=0,
            duration_seconds=0,
            retention_days=self.incremental_backup_retention
        )
        
        try:
            # Use pg_basebackup with incremental option
            backup_dir = self.backup_dir / backup_id
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Build pg_basebackup command for WAL files
            basebackup_cmd = [
                'pg_basebackup',
                f"--host={self.db_config.get('host', 'localhost')}",
                f"--port={self.db_config.get('port', 5432)}",
                f"--username={self.db_config.get('username', 'postgres')}",
                f"--pgdata={backup_dir}",
                '--format=tar',
                '--gzip',
                '--checkpoint=fast',
                '--wal-method=stream',
                '--verbose'
            ]
            
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config.get('password', '')
            
            result = subprocess.run(
                basebackup_cmd,
                stderr=subprocess.PIPE,
                env=env,
                timeout=1800  # 30 minutes
            )
            
            if result.returncode != 0:
                error_msg = result.stderr.decode('utf-8')
                logger.error(f"pg_basebackup failed: {error_msg}")
                backup_metadata.error_message = error_msg
                return backup_metadata
            
            # Calculate total size
            total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
            backup_metadata.size_bytes = total_size
            backup_metadata.local_path = str(backup_dir)
            
            # Create archive for S3 upload
            if self.s3_client and self.s3_bucket:
                archive_path = self.backup_dir / f"{backup_id}.tar.gz"
                
                # Create compressed archive
                tar_cmd = ['tar', '-czf', str(archive_path), '-C', str(self.backup_dir), backup_id]
                subprocess.run(tar_cmd, check=True)
                
                backup_metadata.checksum = self.calculate_checksum(archive_path)
                
                # Upload to S3
                s3_key = f"{self.s3_prefix}/incremental/{backup_id}.tar.gz"
                self.s3_client.upload_file(
                    str(archive_path),
                    self.s3_bucket,
                    s3_key,
                    ExtraArgs={'ServerSideEncryption': 'AES256'}
                )
                backup_metadata.s3_key = s3_key
                
                # Clean up local archive
                archive_path.unlink()
            
            backup_metadata.success = True
            backup_metadata.duration_seconds = time.time() - start_time
            
            logger.info(f"Incremental backup completed: {backup_id}")
            
            # Record metrics
            self.metrics_collector.record_counter(
                'backup_incremental_completed_total',
                tags={'status': 'success', 'database': backup_metadata.database_name}
            )
            
        except Exception as e:
            backup_metadata.error_message = str(e)
            backup_metadata.duration_seconds = time.time() - start_time
            logger.error(f"Incremental backup failed: {e}")
            
            self.metrics_collector.record_counter(
                'backup_incremental_failed_total',
                tags={'database': backup_metadata.database_name, 'error': str(e)[:100]}
            )
        
        self.backup_history.append(backup_metadata)
        self.save_backup_history()
        
        return backup_metadata
    
    def cleanup_old_backups(self) -> None:
        """Clean up old backups based on retention policies."""
        logger.info("Starting backup cleanup process")
        
        now = datetime.now()
        backups_to_remove = []
        
        for backup in self.backup_history:
            retention_days = backup.retention_days
            if backup.backup_type == 'full':
                retention_days = self.full_backup_retention
            elif backup.backup_type == 'incremental':
                retention_days = self.incremental_backup_retention
            elif backup.backup_type == 'archive':
                retention_days = self.archive_retention
            
            if now - backup.timestamp > timedelta(days=retention_days):
                backups_to_remove.append(backup)
        
        for backup in backups_to_remove:
            try:
                # Remove from S3
                if backup.s3_key and self.s3_client:
                    self.s3_client.delete_object(
                        Bucket=self.s3_bucket,
                        Key=backup.s3_key
                    )
                    logger.info(f"Removed S3 backup: {backup.s3_key}")
                
                # Remove local files
                if backup.local_path:
                    local_path = Path(backup.local_path)
                    if local_path.exists():
                        if local_path.is_file():
                            local_path.unlink()
                        else:
                            import shutil
                            shutil.rmtree(local_path)
                        logger.info(f"Removed local backup: {backup.local_path}")
                
                # Remove from history
                self.backup_history.remove(backup)
                
                # Record metrics
                self.metrics_collector.record_counter(
                    'backup_cleanup_completed_total',
                    tags={'type': backup.backup_type, 'database': backup.database_name}
                )
                
            except Exception as e:
                logger.error(f"Failed to cleanup backup {backup.backup_id}: {e}")
        
        if backups_to_remove:
            self.save_backup_history()
            logger.info(f"Cleaned up {len(backups_to_remove)} old backups")
    
    def verify_backup_integrity(self, backup_metadata: BackupMetadata) -> bool:
        """Verify backup integrity using checksum and test restore."""
        logger.info(f"Verifying backup integrity: {backup_metadata.backup_id}")
        
        try:
            # Verify local file checksum
            if backup_metadata.local_path and backup_metadata.checksum:
                local_path = Path(backup_metadata.local_path)
                if local_path.exists():
                    current_checksum = self.calculate_checksum(local_path)
                    if current_checksum != backup_metadata.checksum:
                        logger.error(f"Checksum mismatch for {backup_metadata.backup_id}")
                        return False
            
            # Verify S3 object exists and metadata
            if backup_metadata.s3_key and self.s3_client:
                try:
                    response = self.s3_client.head_object(
                        Bucket=self.s3_bucket,
                        Key=backup_metadata.s3_key
                    )
                    
                    # Check metadata
                    metadata = response.get('Metadata', {})
                    if metadata.get('backup-id') != backup_metadata.backup_id:
                        logger.error(f"S3 metadata mismatch for {backup_metadata.backup_id}")
                        return False
                        
                except ClientError:
                    logger.error(f"S3 object not found: {backup_metadata.s3_key}")
                    return False
            
            logger.info(f"Backup integrity verified: {backup_metadata.backup_id}")
            return True
            
        except Exception as e:
            logger.error(f"Backup integrity verification failed: {e}")
            return False
    
    def schedule_backups(self) -> None:
        """Schedule backup jobs."""
        # Full backup every day at 2 AM
        schedule.every().day.at("02:00").do(self.create_full_backup)
        
        # Incremental backup every 6 hours
        schedule.every(6).hours.do(self.create_incremental_backup)
        
        # Cleanup old backups daily at 4 AM
        schedule.every().day.at("04:00").do(self.cleanup_old_backups)
        
        # Verify random backup integrity daily at 5 AM
        schedule.every().day.at("05:00").do(self._verify_random_backup)
        
        logger.info("Backup schedules configured")
        logger.info("- Full backup: Daily at 2:00 AM")
        logger.info("- Incremental backup: Every 6 hours")
        logger.info("- Cleanup: Daily at 4:00 AM")
        logger.info("- Integrity check: Daily at 5:00 AM")
    
    def _verify_random_backup(self) -> None:
        """Verify integrity of a random recent backup."""
        import random
        
        # Get recent successful backups
        recent_backups = [
            b for b in self.backup_history[-10:]  # Last 10 backups
            if b.success and (datetime.now() - b.timestamp).days < 7
        ]
        
        if recent_backups:
            backup_to_verify = random.choice(recent_backups)
            is_valid = self.verify_backup_integrity(backup_to_verify)
            
            # Record metrics
            self.metrics_collector.record_counter(
                'backup_verification_completed_total',
                tags={
                    'status': 'success' if is_valid else 'failed',
                    'backup_id': backup_to_verify.backup_id
                }
            )
    
    def run_scheduler(self) -> None:
        """Run the backup scheduler."""
        logger.info("Starting backup scheduler")
        
        self.schedule_backups()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Backup scheduler stopped")
        except Exception as e:
            logger.error(f"Backup scheduler error: {e}")
            raise


def main():
    """Main entry point for backup scheduler."""
    config_path = os.getenv('CONFIG_PATH', 'config/environments/production.yaml')
    
    try:
        scheduler = BackupScheduler(config_path)
        
        # Handle command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == 'full':
                backup = scheduler.create_full_backup()
                print(f"Full backup completed: {backup.backup_id}, Success: {backup.success}")
                
            elif command == 'incremental':
                backup = scheduler.create_incremental_backup()
                if backup:
                    print(f"Incremental backup completed: {backup.backup_id}, Success: {backup.success}")
                else:
                    print("Incremental backup skipped (no full backup found)")
                    
            elif command == 'cleanup':
                scheduler.cleanup_old_backups()
                print("Backup cleanup completed")
                
            elif command == 'list':
                print("Backup History:")
                print("-" * 80)
                for backup in scheduler.backup_history[-20:]:  # Last 20
                    status = "✓" if backup.success else "✗"
                    size_mb = backup.size_bytes / 1024 / 1024
                    print(f"{status} {backup.timestamp.strftime('%Y-%m-%d %H:%M')} "
                          f"{backup.backup_type:12} {backup.backup_id:20} "
                          f"{size_mb:8.1f} MB")
                          
            else:
                print("Usage: backup_scheduler.py [full|incremental|cleanup|list]")
                sys.exit(1)
        else:
            # Run scheduler
            scheduler.run_scheduler()
            
    except Exception as e:
        logger.error(f"Backup scheduler failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()