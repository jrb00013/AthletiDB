#!/usr/bin/env python3
"""
Sports Data Pipeline - System Monitor
Comprehensive monitoring, health checks, and performance tracking.
"""

import os
import sys
import json
import logging
import argparse
import psutil
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import sqlite3
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.db import get_engine
from pipeline.utils import RateLimiter, Cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SystemMonitor:
    """Comprehensive system monitoring and health checking."""
    
    def __init__(self, db_url: str = "sqlite:///sports_data.db"):
        """Initialize the system monitor."""
        self.db_url = db_url
        self.engine = get_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.start_time = time.time()
        
    def check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage."""
        logger.info("Checking system resources")
        
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            cpu_freq = psutil.cpu_freq()
            
            # Memory usage
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_io = psutil.disk_io_counters()
            
            # Network usage
            network = psutil.net_io_counters()
            
            # Process info
            process = psutil.Process()
            process_memory = process.memory_info()
            process_cpu = process.cpu_percent()
            
            resources = {
                'timestamp': datetime.now().isoformat(),
                'cpu': {
                    'usage_percent': cpu_percent,
                    'count': cpu_count,
                    'frequency_mhz': cpu_freq.current if cpu_freq else None,
                    'process_cpu_percent': process_cpu
                },
                'memory': {
                    'total_gb': round(memory.total / (1024**3), 2),
                    'available_gb': round(memory.available / (1024**3), 2),
                    'used_gb': round(memory.used / (1024**3), 2),
                    'percent': memory.percent,
                    'swap_total_gb': round(swap.total / (1024**3), 2),
                    'swap_used_gb': round(swap.used / (1024**3), 2),
                    'process_memory_mb': round(process_memory.rss / (1024**2), 2)
                },
                'disk': {
                    'total_gb': round(disk.total / (1024**3), 2),
                    'used_gb': round(disk.used / (1024**3), 2),
                    'free_gb': round(disk.free / (1024**3), 2),
                    'percent': round((disk.used / disk.total) * 100, 2),
                    'read_mb': round(disk_io.read_bytes / (1024**2), 2) if disk_io else 0,
                    'write_mb': round(disk_io.write_bytes / (1024**2), 2) if disk_io else 0
                },
                'network': {
                    'bytes_sent_mb': round(network.bytes_sent / (1024**2), 2),
                    'bytes_recv_mb': round(network.bytes_recv / (1024**2), 2),
                    'packets_sent': network.packets_sent,
                    'packets_recv': network.packets_recv
                }
            }
            
            logger.info("System resources checked successfully")
            return resources
            
        except Exception as e:
            logger.error(f"Error checking system resources: {e}")
            return {}
    
    def check_database_health(self) -> Dict[str, Any]:
        """Check database health and performance."""
        logger.info("Checking database health")
        
        try:
            # Test connection
            start_time = time.time()
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                connection_time = time.time() - start_time
            
            # Check table sizes
            table_sizes = {}
            with self.engine.connect() as conn:
                tables_query = """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
                tables = [row[0] for row in conn.execute(text(tables_query))]
                
                for table in tables:
                    count_query = f"SELECT COUNT(*) FROM {table}"
                    count_result = conn.execute(text(count_query))
                    count = count_result.scalar()
                    table_sizes[table] = count
            
            # Check database file size
            db_path = self.db_url.replace('sqlite:///', '')
            if os.path.exists(db_path):
                db_size = os.path.getsize(db_path)
                db_size_mb = round(db_size / (1024**2), 2)
            else:
                db_size_mb = 0
            
            # Check for locks
            with self.engine.connect() as conn:
                lock_query = "SELECT * FROM sqlite_master WHERE type='table' AND name='sqlite_lock'"
                lock_result = conn.execute(text(lock_query))
                has_locks = lock_result.fetchone() is not None
            
            health = {
                'timestamp': datetime.now().isoformat(),
                'connection': {
                    'status': 'OK',
                    'response_time_ms': round(connection_time * 1000, 2)
                },
                'tables': table_sizes,
                'file_size_mb': db_size_mb,
                'locks_detected': has_locks,
                'total_records': sum(table_sizes.values())
            }
            
            logger.info("Database health checked successfully")
            return health
            
        except Exception as e:
            logger.error(f"Error checking database health: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'connection': {'status': 'ERROR', 'error': str(e)},
                'tables': {},
                'file_size_mb': 0,
                'locks_detected': False,
                'total_records': 0
            }
    
    def check_api_status(self) -> Dict[str, Any]:
        """Check API provider status and rate limits."""
        logger.info("Checking API status")
        
        try:
            # Check TheSportsDB API
            thesportsdb_status = self._check_thesportsdb_api()
            
            # Check rate limiter status
            rate_limiter = RateLimiter()
            rate_limit_status = {
                'requests_per_hour': rate_limiter.requests_per_hour,
                'current_requests': rate_limiter.current_requests,
                'burst_limit': rate_limiter.burst_limit,
                'available_requests': rate_limiter.requests_per_hour - rate_limiter.current_requests,
                'reset_time': rate_limiter.last_reset.isoformat() if rate_limiter.last_reset else None
            }
            
            # Check cache status
            cache = Cache()
            cache_status = {
                'cache_dir': str(cache.cache_dir),
                'cache_size_mb': self._get_cache_size(cache.cache_dir),
                'cache_files': len(list(cache.cache_dir.glob('*.cache')))
            }
            
            api_status = {
                'timestamp': datetime.now().isoformat(),
                'thesportsdb': thesportsdb_status,
                'rate_limiter': rate_limit_status,
                'cache': cache_status
            }
            
            logger.info("API status checked successfully")
            return api_status
            
        except Exception as e:
            logger.error(f"Error checking API status: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'thesportsdb': {'status': 'ERROR', 'error': str(e)},
                'rate_limiter': {},
                'cache': {}
            }
    
    def _check_thesportsdb_api(self) -> Dict[str, Any]:
        """Check TheSportsDB API connectivity."""
        try:
            # Test API endpoint
            api_key = os.getenv('THESPORTSDB_API_KEY', '1')
            test_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/search_all_teams.php?l=American%20football%20_nfl"
            
            start_time = time.time()
            response = requests.get(test_url, timeout=10)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                teams_count = len(data.get('teams', []))
                
                return {
                    'status': 'OK',
                    'response_time_ms': round(response_time * 1000, 2),
                    'status_code': response.status_code,
                    'teams_returned': teams_count,
                    'api_key_configured': bool(api_key and api_key != '1')
                }
            else:
                return {
                    'status': 'ERROR',
                    'response_time_ms': round(response_time * 1000, 2),
                    'status_code': response.status_code,
                    'error': f"HTTP {response.status_code}"
                }
                
        except requests.exceptions.RequestException as e:
            return {
                'status': 'ERROR',
                'error': f"Request failed: {str(e)}"
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': f"Unexpected error: {str(e)}"
            }
    
    def _get_cache_size(self, cache_dir: Path) -> float:
        """Calculate cache directory size in MB."""
        try:
            total_size = 0
            for file_path in cache_dir.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            return round(total_size / (1024**2), 2)
        except Exception:
            return 0.0
    
    def check_pipeline_status(self) -> Dict[str, Any]:
        """Check overall pipeline status and recent activity."""
        logger.info("Checking pipeline status")
        
        try:
            # Check recent data activity
            with self.engine.connect() as conn:
                # Recent players
                recent_players_query = """
                SELECT COUNT(*) as count, MAX(created_at) as last_update
                FROM players 
                WHERE created_at >= datetime('now', '-7 days')
                """
                recent_players = conn.execute(text(recent_players_query)).fetchone()
                
                # Recent upsets
                recent_upsets_query = """
                SELECT COUNT(*) as count, MAX(date) as last_upset
                FROM upsets 
                WHERE date >= datetime('now', '-7 days')
                """
                recent_upsets = conn.execute(text(recent_upsets_query)).fetchone()
                
                # Recent games
                recent_games_query = """
                SELECT COUNT(*) as count, MAX(date) as last_game
                FROM games 
                WHERE date >= datetime('now', '-7 days')
                """
                recent_games = conn.execute(text(recent_games_query)).fetchone()
                
                # Recent injuries
                recent_injuries_query = """
                SELECT COUNT(*) as count, MAX(date) as last_injury
                FROM injuries 
                WHERE date >= datetime('now', '-7 days')
                """
                recent_injuries = conn.execute(text(recent_injuries_query)).fetchone()
            
            # Check log files
            log_dir = Path('logs')
            recent_logs = []
            if log_dir.exists():
                for log_file in log_dir.glob('*.log*'):
                    stat = log_file.stat()
                    if time.time() - stat.st_mtime < 86400:  # Last 24 hours
                        recent_logs.append({
                            'name': log_file.name,
                            'size_mb': round(stat.st_size / (1024**2), 2),
                            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
            
            # Check export files
            export_dir = Path('exports')
            recent_exports = []
            if export_dir.exists():
                for export_file in export_dir.glob('*'):
                    stat = export_file.stat()
                    if time.time() - stat.st_mtime < 86400:  # Last 24 hours
                        recent_exports.append({
                            'name': export_file.name,
                            'size_mb': round(stat.st_size / (1024**2), 2),
                            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
            
            pipeline_status = {
                'timestamp': datetime.now().isoformat(),
                'uptime_seconds': round(time.time() - self.start_time, 2),
                'recent_activity': {
                    'players_last_7_days': recent_players.count if recent_players else 0,
                    'last_player_update': recent_players.last_update if recent_players else None,
                    'upsets_last_7_days': recent_upsets.count if recent_upsets else 0,
                    'last_upset': recent_upsets.last_upset if recent_upsets else None,
                    'games_last_7_days': recent_games.count if recent_games else 0,
                    'last_game': recent_games.last_game if recent_games else None,
                    'injuries_last_7_days': recent_injuries.count if recent_injuries else 0,
                    'last_injury': recent_injuries.last_injury if recent_injuries else None
                },
                'logs': {
                    'recent_logs': recent_logs,
                    'total_logs': len(recent_logs)
                },
                'exports': {
                    'recent_exports': recent_exports,
                    'total_exports': len(recent_exports)
                }
            }
            
            logger.info("Pipeline status checked successfully")
            return pipeline_status
            
        except Exception as e:
            logger.error(f"Error checking pipeline status: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'uptime_seconds': round(time.time() - self.start_time, 2),
                'error': str(e)
            }
    
    def generate_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report."""
        logger.info("Generating health report")
        
        try:
            health_report = {
                'timestamp': datetime.now().isoformat(),
                'system_resources': self.check_system_resources(),
                'database_health': self.check_database_health(),
                'api_status': self.check_api_status(),
                'pipeline_status': self.check_pipeline_status()
            }
            
            # Add overall health score
            health_score = self._calculate_health_score(health_report)
            health_report['overall_health_score'] = health_score
            
            # Add recommendations
            health_report['recommendations'] = self._generate_recommendations(health_report)
            
            logger.info("Health report generated successfully")
            return health_report
            
        except Exception as e:
            logger.error(f"Error generating health report: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'overall_health_score': 0
            }
    
    def _calculate_health_score(self, health_report: Dict[str, Any]) -> int:
        """Calculate overall health score (0-100)."""
        score = 100
        
        try:
            # System resources (30 points)
            if 'system_resources' in health_report:
                resources = health_report['system_resources']
                if resources.get('memory', {}).get('percent', 0) > 90:
                    score -= 15
                elif resources.get('memory', {}).get('percent', 0) > 80:
                    score -= 10
                
                if resources.get('disk', {}).get('percent', 0) > 90:
                    score -= 15
                elif resources.get('disk', {}).get('percent', 0) > 80:
                    score -= 10
            
            # Database health (25 points)
            if 'database_health' in health_report:
                db_health = health_report['database_health']
                if db_health.get('connection', {}).get('status') != 'OK':
                    score -= 25
                elif db_health.get('connection', {}).get('response_time_ms', 0) > 1000:
                    score -= 10
            
            # API status (25 points)
            if 'api_status' in health_report:
                api_status = health_report['api_status']
                if api_status.get('thesportsdb', {}).get('status') != 'OK':
                    score -= 15
                
                rate_limiter = api_status.get('rate_limiter', {})
                if rate_limiter.get('available_requests', 0) < 10:
                    score -= 10
            
            # Pipeline status (20 points)
            if 'pipeline_status' in health_report:
                pipeline_status = health_report['pipeline_status']
                if pipeline_status.get('recent_activity', {}).get('players_last_7_days', 0) == 0:
                    score -= 10
                if pipeline_status.get('recent_activity', {}).get('games_last_7_days', 0) == 0:
                    score -= 10
            
            return max(0, score)
            
        except Exception:
            return 50  # Default score if calculation fails
    
    def _generate_recommendations(self, health_report: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on health report."""
        recommendations = []
        
        try:
            # System resources
            resources = health_report.get('system_resources', {})
            if resources.get('memory', {}).get('percent', 0) > 80:
                recommendations.append("Consider increasing available memory or optimizing memory usage")
            
            if resources.get('disk', {}).get('percent', 0) > 80:
                recommendations.append("Disk space is running low - consider cleanup or expansion")
            
            # Database
            db_health = health_report.get('database_health', {})
            if db_health.get('connection', {}).get('response_time_ms', 0) > 1000:
                recommendations.append("Database response time is slow - consider optimization")
            
            # API
            api_status = health_report.get('api_status', {})
            if api_status.get('thesportsdb', {}).get('status') != 'OK':
                recommendations.append("TheSportsDB API is not responding - check connectivity and API key")
            
            rate_limiter = api_status.get('rate_limiter', {})
            if rate_limiter.get('available_requests', 0) < 10:
                recommendations.append("API rate limit nearly reached - consider reducing request frequency")
            
            # Pipeline
            pipeline_status = health_report.get('pipeline_status', {})
            if pipeline_status.get('recent_activity', {}).get('players_last_7_days', 0) == 0:
                recommendations.append("No recent player data - check data collection processes")
            
            if not recommendations:
                recommendations.append("System is healthy - no immediate action required")
            
            return recommendations
            
        except Exception:
            return ["Unable to generate recommendations - check system manually"]
    
    def export_health_report(self, output_dir: str = "reports", format: str = "json") -> str:
        """Export health report to file."""
        logger.info(f"Exporting health report to {output_dir}")
        
        try:
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format.lower() == "json":
                filename = f"health_report_{timestamp}.json"
                filepath = output_path / filename
                
                health_report = self.generate_health_report()
                
                with open(filepath, 'w') as f:
                    json.dump(health_report, f, indent=2, default=str)
                
                logger.info(f"Health report exported to {filepath}")
                return str(filepath)
                
            elif format.lower() == "txt":
                filename = f"health_report_{timestamp}.txt"
                filepath = output_path / filename
                
                health_report = self.generate_health_report()
                
                with open(filepath, 'w') as f:
                    f.write("Sports Data Pipeline - Health Report\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(f"Generated: {health_report['timestamp']}\n")
                    f.write(f"Overall Health Score: {health_report['overall_health_score']}/100\n\n")
                    
                    # System Resources
                    f.write("SYSTEM RESOURCES\n")
                    f.write("-" * 20 + "\n")
                    resources = health_report.get('system_resources', {})
                    if resources:
                        f.write(f"CPU Usage: {resources.get('cpu', {}).get('usage_percent', 'N/A')}%\n")
                        f.write(f"Memory Usage: {resources.get('memory', {}).get('percent', 'N/A')}%\n")
                        f.write(f"Disk Usage: {resources.get('disk', {}).get('percent', 'N/A')}%\n")
                    f.write("\n")
                    
                    # Database Health
                    f.write("DATABASE HEALTH\n")
                    f.write("-" * 20 + "\n")
                    db_health = health_report.get('database_health', {})
                    if db_health:
                        f.write(f"Status: {db_health.get('connection', {}).get('status', 'N/A')}\n")
                        f.write(f"Response Time: {db_health.get('connection', {}).get('response_time_ms', 'N/A')}ms\n")
                        f.write(f"Total Records: {db_health.get('total_records', 'N/A')}\n")
                    f.write("\n")
                    
                    # Recommendations
                    f.write("RECOMMENDATIONS\n")
                    f.write("-" * 20 + "\n")
                    recommendations = health_report.get('recommendations', [])
                    for i, rec in enumerate(recommendations, 1):
                        f.write(f"{i}. {rec}\n")
                
                logger.info(f"Health report exported to {filepath}")
                return str(filepath)
            
            else:
                logger.error(f"Unsupported format: {format}")
                return ""
                
        except Exception as e:
            logger.error(f"Error exporting health report: {e}")
            return ""

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Sports Data Pipeline System Monitor")
    parser.add_argument('action', choices=[
        'resources', 'database', 'api', 'pipeline', 'health', 'export'
    ], help='Action to perform')
    parser.add_argument('--output-dir', '-o', default='reports', 
                       help='Output directory for reports')
    parser.add_argument('--format', '-f', choices=['json', 'txt'], default='json',
                       help='Output format')
    parser.add_argument('--continuous', '-c', action='store_true',
                       help='Run continuous monitoring')
    parser.add_argument('--interval', '-i', type=int, default=60,
                       help='Monitoring interval in seconds (for continuous mode)')
    
    args = parser.parse_args()
    
    monitor = SystemMonitor()
    
    if args.continuous:
        print(f"Starting continuous monitoring (interval: {args.interval}s)")
        try:
            while True:
                print(f"\n--- Health Check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                
                if args.action == 'health':
                    health_report = monitor.generate_health_report()
                    print(f"Health Score: {health_report['overall_health_score']}/100")
                    
                    # Show key metrics
                    resources = health_report.get('system_resources', {})
                    if resources:
                        print(f"CPU: {resources.get('cpu', {}).get('usage_percent', 'N/A')}% | "
                              f"Memory: {resources.get('memory', {}).get('percent', 'N/A')}% | "
                              f"Disk: {resources.get('disk', {}).get('percent', 'N/A')}%")
                    
                    # Show recommendations
                    recommendations = health_report.get('recommendations', [])
                    if recommendations:
                        print("Recommendations:")
                        for rec in recommendations[:3]:  # Show top 3
                            print(f"  - {rec}")
                
                time.sleep(args.interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            return 0
    
    elif args.action == 'resources':
        resources = monitor.check_system_resources()
        print(json.dumps(resources, indent=2, default=str))
    
    elif args.action == 'database':
        db_health = monitor.check_database_health()
        print(json.dumps(db_health, indent=2, default=str))
    
    elif args.action == 'api':
        api_status = monitor.check_api_status()
        print(json.dumps(api_status, indent=2, default=str))
    
    elif args.action == 'pipeline':
        pipeline_status = monitor.check_pipeline_status()
        print(json.dumps(pipeline_status, indent=2, default=str))
    
    elif args.action == 'health':
        health_report = monitor.generate_health_report()
        print(json.dumps(health_report, indent=2, default=str))
    
    elif args.action == 'export':
        filepath = monitor.export_health_report(args.output_dir, args.format)
        if filepath:
            print(f"Health report exported to: {filepath}")
        else:
            print("Failed to export health report")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
