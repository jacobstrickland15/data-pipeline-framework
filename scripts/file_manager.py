#!/usr/bin/env python3
"""File management utilities for the data pipeline."""

import os
import shutil
import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class LocalFileManager:
    """Manages local file operations for the data pipeline."""
    
    def __init__(self, base_path="./data"):
        self.base_path = Path(base_path)
        self.incoming_path = self.base_path / "incoming"
        self.archive_path = self.base_path / "archive"
        self.failed_path = self.base_path / "failed"
        self.temp_path = self.base_path / "temp"
        
        # Ensure directories exist
        for path in [self.incoming_path, self.archive_path, self.failed_path, self.temp_path]:
            path.mkdir(parents=True, exist_ok=True)
    
    def archive_file(self, file_path, success=True):
        """Archive a processed file."""
        file_path = Path(file_path)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if success:
            # Move to archive with timestamp
            archive_name = f"{timestamp}_{file_path.name}"
            destination = self.archive_path / archive_name
            logger.info(f"Archiving successful file: {file_path} -> {destination}")
        else:
            # Move to failed directory
            failed_name = f"{timestamp}_{file_path.name}"
            destination = self.failed_path / failed_name
            logger.info(f"Moving failed file: {file_path} -> {destination}")
        
        shutil.move(str(file_path), str(destination))
        return destination
    
    def list_incoming_files(self, pattern="*"):
        """List files in incoming directory."""
        return list(self.incoming_path.glob(pattern))
    
    def cleanup_temp_files(self, older_than_hours=24):
        """Clean up temporary files older than specified hours."""
        cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=older_than_hours)
        
        for file_path in self.temp_path.glob("*"):
            if file_path.stat().st_mtime < cutoff_time.timestamp():
                logger.info(f"Removing old temp file: {file_path}")
                file_path.unlink()
    
    def get_storage_stats(self):
        """Get storage statistics."""
        stats = {}
        
        for name, path in [
            ("incoming", self.incoming_path),
            ("archive", self.archive_path),
            ("failed", self.failed_path),
            ("temp", self.temp_path)
        ]:
            files = list(path.glob("*"))
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            stats[name] = {
                "file_count": len(files),
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            }
        
        return stats

def main():
    """Demo the file manager."""
    manager = LocalFileManager()
    
    # Show current status
    stats = manager.get_storage_stats()
    print("📊 Storage Statistics:")
    for folder, data in stats.items():
        print(f"  {folder}: {data['file_count']} files, {data['total_size_mb']} MB")
    
    # List incoming files
    incoming = manager.list_incoming_files("*.csv")
    print(f"\n📁 Incoming files: {len(incoming)}")
    for file in incoming:
        print(f"  - {file.name}")
    
    # Archive processed files (simulate)
    for file in incoming:
        print(f"\n✅ Archiving processed file: {file.name}")
        manager.archive_file(file, success=True)
    
    # Show updated stats
    stats = manager.get_storage_stats()
    print("\n📊 Updated Storage Statistics:")
    for folder, data in stats.items():
        print(f"  {folder}: {data['file_count']} files, {data['total_size_mb']} MB")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()