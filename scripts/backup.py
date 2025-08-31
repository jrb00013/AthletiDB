#!/usr/bin/env python3
"""
Sports Data Pipeline Backup Script
Comprehensive backup and restore functionality for the pipeline.
"""

import os
import sys
import shutil
import sqlite3
import json
import gzip
from pathlib import Path
from datetime import datetime, timedelta
import logging
import argparse
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineBackup:
    """Backup and restore functionality for the sports data pipeline."""
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.db_path = Path("sports_data.db")
        
    def create_backup(self, include_logs: bool = True, include_exports: bool = True, 
                      compress: bool = True) -> str:
        """Create a comprehensive backup of the pipeline."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"pipeline_backup_{timestamp}"
        backup_path = self.backup_dir / backup_name
        backup_path.mkdir(exist_ok=True)
        
        logger.info(f"Creating backup: {backup_name}")
        
        try:
            # 1. Backup database
            self._backup_database(backup_path)
            
            # 2. Backup configuration files
            self._backup_config_files(backup_path)
            
            # 3. Backup logs (optional)
            if include_logs:
                self._backup_logs(backup_path)
            
            # 4. Backup exports (optional)
            if include_exports:
                self._backup_exports(backup_path)
            
            # 5. Create backup manifest
            self._create_manifest(backup_path, include_logs, include_exports)
            
            # 6. Compress backup (optional)
            if compress:
                compressed_path = self._compress_backup(backup_path)
                shutil.rmtree(backup_path)
                backup_path = compressed_path
            
            logger.info(f"Backup completed successfully: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            if backup_path.exists():
                shutil.rmtree(backup_path)
            raise
    
    def _backup_database(self, backup_path: Path) -> None:
        """Backup the SQLite database."""
        if not self.db_path.exists():
            logger.warning("Database file not found, skipping database backup")
            return
        
        db_backup_path = backup_path / "database"
        db_backup_path.mkdir(exist_ok=True)
        
        # Copy database file
        shutil.copy2(self.db_path, db_backup_path / "sports_data.db")
        
        # Create database dump
        self._create_database_dump(db_backup_path / "sports_data.sql")
        
        logger.info("Database backup completed")
    
    def _create_database_dump(self, dump_path: Path) -> None:
        """Create a SQL dump of the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            with open(dump_path, 'w') as f:
                for line in conn.iterdump():
                    f.write(f'{line}\n')
            
            conn.close()
            logger.info("Database dump created")
            
        except Exception as e:
            logger.error(f"Failed to create database dump: {e}")
    
    def _backup_config_files(self, backup_path: Path) -> None:
        """Backup configuration files."""
        config_backup_path = backup_path / "config"
        config_backup_path.mkdir(exist_ok=True)
        
        config_files = [
            "config.yaml",
            ".env",
            "requirements.txt"
        ]
        
        for config_file in config_files:
            if Path(config_file).exists():
                shutil.copy2(config_file, config_backup_path / config_file)
        
        logger.info("Configuration files backed up")
    
    def _backup_logs(self, backup_path: Path) -> None:
        """Backup log files."""
        logs_backup_path = backup_path / "logs"
        logs_backup_path.mkdir(exist_ok=True)
        
        log_files = list(Path("logs").glob("*.log*")) if Path("logs").exists() else []
        
        for log_file in log_files:
            shutil.copy2(log_file, logs_backup_path / log_file.name)
        
        logger.info(f"Backed up {len(log_files)} log files")
    
    def _backup_exports(self, backup_path: Path) -> None:
        """Backup export files."""
        exports_backup_path = backup_path / "exports"
        
        if Path("exports").exists():
            shutil.copytree("exports", exports_backup_path, dirs_exist_ok=True)
            logger.info("Export files backed up")
        else:
            logger.info("No export files to backup")
    
    def _create_manifest(self, backup_path: Path, include_logs: bool, 
                        include_exports: bool) -> None:
        """Create a backup manifest file."""
        manifest = {
            "backup_info": {
                "timestamp": datetime.now().isoformat(),
                "backup_name": backup_path.name,
                "version": "1.0"
            },
            "contents": {
                "database": True,
                "config_files": True,
                "logs": include_logs,
                "exports": include_exports
            },
            "file_sizes": {},
            "checksums": {}
        }
        
        # Calculate file sizes
        for item in backup_path.rglob("*"):
            if item.is_file():
                relative_path = item.relative_to(backup_path)
                manifest["file_sizes"][str(relative_path)] = item.stat().st_size
        
        manifest_path = backup_path / "backup_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info("Backup manifest created")
    
    def _compress_backup(self, backup_path: Path) -> Path:
        """Compress the backup directory."""
        compressed_path = backup_path.with_suffix('.tar.gz')
        
        import tarfile
        with tarfile.open(compressed_path, 'w:gz') as tar:
            tar.add(backup_path, arcname=backup_path.name)
        
        logger.info(f"Backup compressed: {compressed_path}")
        return compressed_path
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List all available backups."""
        backups = []
        
        for backup_item in self.backup_dir.iterdir():
            if backup_item.is_file() and backup_item.suffix == '.tar.gz':
                # Compressed backup
                backup_info = self._get_backup_info(backup_item)
                backups.append(backup_info)
            elif backup_item.is_dir() and backup_item.name.startswith('pipeline_backup_'):
                # Uncompressed backup
                backup_info = self._get_backup_info(backup_item)
                backups.append(backup_info)
        
        # Sort by timestamp (newest first)
        backups.sort(key=lambda x: x['timestamp'], reverse=True)
        return backups
    
    def _get_backup_info(self, backup_path: Path) -> Dict[str, Any]:
        """Get information about a backup."""
        try:
            if backup_path.suffix == '.tar.gz':
                # Compressed backup
                size = backup_path.stat().st_size
                backup_type = 'compressed'
            else:
                # Uncompressed backup
                size = sum(f.stat().st_size for f in backup_path.rglob('*') if f.is_file())
                backup_type = 'uncompressed'
            
            # Extract timestamp from name
            name_parts = backup_path.stem.split('_')
            if len(name_parts) >= 3:
                timestamp_str = f"{name_parts[-2]}_{name_parts[-1]}"
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                except ValueError:
                    timestamp = datetime.fromtimestamp(backup_path.stat().st_mtime)
            else:
                timestamp = datetime.fromtimestamp(backup_path.stat().st_mtime)
            
            return {
                'name': backup_path.name,
                'path': str(backup_path),
                'type': backup_type,
                'size': size,
                'size_human': self._format_size(size),
                'timestamp': timestamp,
                'age': datetime.now() - timestamp
            }
            
        except Exception as e:
            logger.error(f"Error getting backup info for {backup_path}: {e}")
            return {
                'name': backup_path.name,
                'path': str(backup_path),
                'type': 'unknown',
                'size': 0,
                'size_human': '0 B',
                'timestamp': datetime.now(),
                'age': timedelta(0)
            }
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"
    
    def restore_backup(self, backup_name: str, target_dir: str = None, 
                      overwrite: bool = False) -> None:
        """Restore a backup."""
        backup_path = self.backup_dir / backup_name
        
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup not found: {backup_name}")
        
        target_dir = Path(target_dir) if target_dir else Path(".")
        
        logger.info(f"Restoring backup: {backup_name} to {target_dir}")
        
        try:
            if backup_path.suffix == '.tar.gz':
                # Extract compressed backup
                self._extract_backup(backup_path, target_dir)
            else:
                # Copy uncompressed backup
                self._copy_backup(backup_path, target_dir, overwrite)
            
            logger.info("Backup restored successfully")
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            raise
    
    def _extract_backup(self, backup_path: Path, target_dir: Path) -> None:
        """Extract a compressed backup."""
        import tarfile
        
        with tarfile.open(backup_path, 'r:gz') as tar:
            tar.extractall(target_dir)
    
    def _copy_backup(self, backup_path: Path, target_dir: Path, overwrite: bool) -> None:
        """Copy an uncompressed backup."""
        if not overwrite and target_dir.exists():
            raise FileExistsError(f"Target directory exists: {target_dir}")
        
        if target_dir.exists():
            shutil.rmtree(target_dir)
        
        shutil.copytree(backup_path, target_dir)
    
    def cleanup_old_backups(self, keep_days: int = 30, keep_count: int = 10) -> None:
        """Clean up old backups based on age and count."""
        backups = self.list_backups()
        
        # Remove backups older than keep_days
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        old_backups = [b for b in backups if b['timestamp'] < cutoff_date]
        
        # Keep at least keep_count backups
        if len(backups) - len(old_backups) < keep_count:
            old_backups = old_backups[keep_count:]
        
        # Remove old backups
        for backup in old_backups:
            try:
                backup_path = Path(backup['path'])
                if backup_path.exists():
                    if backup_path.is_file():
                        backup_path.unlink()
                    else:
                        shutil.rmtree(backup_path)
                    logger.info(f"Removed old backup: {backup['name']}")
            except Exception as e:
                logger.error(f"Failed to remove old backup {backup['name']}: {e}")
        
        logger.info(f"Cleanup completed. Removed {len(old_backups)} old backups")

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Sports Data Pipeline Backup Tool")
    parser.add_argument('action', choices=['create', 'list', 'restore', 'cleanup'],
                       help='Action to perform')
    parser.add_argument('--backup-dir', default='backups', help='Backup directory')
    parser.add_argument('--include-logs', action='store_true', default=True,
                       help='Include logs in backup')
    parser.add_argument('--include-exports', action='store_true', default=True,
                       help='Include exports in backup')
    parser.add_argument('--compress', action='store_true', default=True,
                       help='Compress backup')
    parser.add_argument('--backup-name', help='Backup name for restore')
    parser.add_argument('--target-dir', help='Target directory for restore')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
    parser.add_argument('--keep-days', type=int, default=30, help='Keep backups for N days')
    parser.add_argument('--keep-count', type=int, default=10, help='Keep at least N backups')
    
    args = parser.parse_args()
    
    backup_tool = PipelineBackup(args.backup_dir)
    
    try:
        if args.action == 'create':
            backup_path = backup_tool.create_backup(
                include_logs=args.include_logs,
                include_exports=args.include_exports,
                compress=args.compress
            )
            print(f"Backup created: {backup_path}")
            
        elif args.action == 'list':
            backups = backup_tool.list_backups()
            if not backups:
                print("No backups found")
            else:
                print(f"{'Name':<30} {'Type':<12} {'Size':<10} {'Age':<15}")
                print("-" * 70)
                for backup in backups:
                    age_str = str(backup['age']).split('.')[0]  # Remove microseconds
                    print(f"{backup['name']:<30} {backup['type']:<12} "
                          f"{backup['size_human']:<10} {age_str:<15}")
            
        elif args.action == 'restore':
            if not args.backup_name:
                print("Error: --backup-name is required for restore")
                sys.exit(1)
            
            backup_tool.restore_backup(
                args.backup_name,
                args.target_dir,
                args.overwrite
            )
            print("Backup restored successfully")
            
        elif args.action == 'cleanup':
            backup_tool.cleanup_old_backups(args.keep_days, args.keep_count)
            print("Cleanup completed")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
