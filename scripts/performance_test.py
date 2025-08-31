#!/usr/bin/env python3
"""
Sports Data Pipeline - Performance Testing
Comprehensive performance testing and benchmarking utilities.
"""

import os
import sys
import json
import logging
import argparse
import time
import cProfile
import pstats
import io
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.db import get_engine
from pipeline.normalize import Player, Upset, Injury, TeamRecord, Game, PlayerStats, TeamAnalysis
from pipeline.utils import RateLimiter, Cache
from pipeline.providers import thesportsdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/performance_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PerformanceTester:
    """Comprehensive performance testing and benchmarking engine."""
    
    def __init__(self, db_url: str = "sqlite:///sports_data.db"):
        """Initialize the performance tester."""
        self.db_url = db_url
        self.engine = get_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.results = {}
        
    def benchmark_database_operations(self, iterations: int = 100) -> Dict[str, Any]:
        """Benchmark database read/write operations."""
        logger.info(f"Benchmarking database operations with {iterations} iterations")
        
        try:
            results = {
                'timestamp': datetime.now().isoformat(),
                'iterations': iterations,
                'operations': {}
            }
            
            # Test table creation
            start_time = time.time()
            for i in range(iterations):
                with self.engine.begin() as conn:
                    conn.execute(text(f"CREATE TABLE IF NOT EXISTS test_table_{i} (id INTEGER, name TEXT)"))
            table_creation_time = time.time() - start_time
            
            results['operations']['table_creation'] = {
                'total_time': round(table_creation_time, 4),
                'avg_time_per_table': round(table_creation_time / iterations, 6),
                'tables_per_second': round(iterations / table_creation_time, 2)
            }
            
            # Test data insertion
            start_time = time.time()
            for i in range(iterations):
                with self.engine.begin() as conn:
                    conn.execute(text(f"INSERT INTO test_table_{i} (id, name) VALUES (?, ?)"), (i, f"test_{i}"))
            data_insertion_time = time.time() - start_time
            
            results['operations']['data_insertion'] = {
                'total_time': round(data_insertion_time, 4),
                'avg_time_per_insert': round(data_insertion_time / iterations, 6),
                'inserts_per_second': round(iterations / data_insertion_time, 2)
            }
            
            # Test data retrieval
            start_time = time.time()
            for i in range(iterations):
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT * FROM test_table_{i}"))
                    data = result.fetchall()
            data_retrieval_time = time.time() - start_time
            
            results['operations']['data_retrieval'] = {
                'total_time': round(data_retrieval_time, 4),
                'avg_time_per_select': round(data_retrieval_time / iterations, 6),
                'selects_per_second': round(iterations / data_retrieval_time, 2)
            }
            
            # Test complex queries
            start_time = time.time()
            for i in range(iterations // 10):  # Fewer iterations for complex queries
                with self.engine.connect() as conn:
                    # Simulate complex join query
                    query = """
                    SELECT p.name, p.team, COUNT(ps.id) as games
                    FROM players p
                    LEFT JOIN player_stats ps ON p.id = ps.player_id
                    WHERE p.league = 'nfl'
                    GROUP BY p.id, p.name, p.team
                    ORDER BY games DESC
                    LIMIT 10
                    """
                    result = conn.execute(text(query))
                    data = result.fetchall()
            complex_query_time = time.time() - start_time
            
            results['operations']['complex_queries'] = {
                'total_time': round(complex_query_time, 4),
                'avg_time_per_query': round(complex_query_time / (iterations // 10), 6),
                'queries_per_second': round((iterations // 10) / complex_query_time, 2)
            }
            
            # Cleanup test tables
            start_time = time.time()
            for i in range(iterations):
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS test_table_{i}"))
            cleanup_time = time.time() - start_time
            
            results['operations']['cleanup'] = {
                'total_time': round(cleanup_time, 4),
                'avg_time_per_drop': round(cleanup_time / iterations, 6),
                'drops_per_second': round(iterations / cleanup_time, 2)
            }
            
            # Calculate overall database performance score
            total_time = sum([
                results['operations']['table_creation']['total_time'],
                results['operations']['data_insertion']['total_time'],
                results['operations']['data_retrieval']['total_time'],
                results['operations']['complex_queries']['total_time'],
                results['operations']['cleanup']['total_time']
            ])
            
            results['overall_score'] = {
                'total_time': round(total_time, 4),
                'operations_per_second': round(iterations * 4 / total_time, 2),
                'performance_rating': self._calculate_performance_rating(total_time, iterations)
            }
            
            logger.info("Database operations benchmark completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error benchmarking database operations: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'iterations': iterations
            }
    
    def benchmark_api_operations(self, iterations: int = 50) -> Dict[str, Any]:
        """Benchmark API operations and rate limiting."""
        logger.info(f"Benchmarking API operations with {iterations} iterations")
        
        try:
            results = {
                'timestamp': datetime.now().isoformat(),
                'iterations': iterations,
                'operations': {}
            }
            
            # Test rate limiter performance
            rate_limiter = RateLimiter()
            
            start_time = time.time()
            for i in range(iterations):
                with rate_limiter:
                    time.sleep(0.01)  # Simulate API call
            rate_limiter_time = time.time() - start_time
            
            results['operations']['rate_limiting'] = {
                'total_time': round(rate_limiter_time, 4),
                'avg_time_per_request': round(rate_limiter_time / iterations, 6),
                'requests_per_second': round(iterations / rate_limiter_time, 2)
            }
            
            # Test cache performance
            cache = Cache()
            
            # Test cache write performance
            start_time = time.time()
            for i in range(iterations):
                cache.set(f"test_key_{i}", f"test_value_{i}", ttl=3600)
            cache_write_time = time.time() - start_time
            
            results['operations']['cache_write'] = {
                'total_time': round(cache_write_time, 4),
                'avg_time_per_write': round(cache_write_time / iterations, 6),
                'writes_per_second': round(iterations / cache_write_time, 2)
            }
            
            # Test cache read performance
            start_time = time.time()
            for i in range(iterations):
                value = cache.get(f"test_key_{i}")
            cache_read_time = time.time() - start_time
            
            results['operations']['cache_read'] = {
                'total_time': round(cache_read_time, 4),
                'avg_time_per_read': round(cache_read_time / iterations, 6),
                'reads_per_second': round(iterations / cache_read_time, 2)
            }
            
            # Test cache miss performance
            start_time = time.time()
            for i in range(iterations):
                value = cache.get(f"nonexistent_key_{i}")
            cache_miss_time = time.time() - start_time
            
            results['operations']['cache_miss'] = {
                'total_time': round(cache_miss_time, 4),
                'avg_time_per_miss': round(cache_miss_time / iterations, 6),
                'misses_per_second': round(iterations / cache_miss_time, 2)
            }
            
            # Cleanup cache
            for i in range(iterations):
                cache.delete(f"test_key_{i}")
            
            # Calculate overall API performance score
            total_time = sum([
                results['operations']['rate_limiting']['total_time'],
                results['operations']['cache_write']['total_time'],
                results['operations']['cache_read']['total_time'],
                results['operations']['cache_miss']['total_time']
            ])
            
            results['overall_score'] = {
                'total_time': round(total_time, 4),
                'operations_per_second': round(iterations * 4 / total_time, 2),
                'performance_rating': self._calculate_performance_rating(total_time, iterations)
            }
            
            logger.info("API operations benchmark completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error benchmarking API operations: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'iterations': iterations
            }
    
    def benchmark_data_processing(self, data_size: int = 1000) -> Dict[str, Any]:
        """Benchmark data processing operations."""
        logger.info(f"Benchmarking data processing with {data_size} records")
        
        try:
            results = {
                'timestamp': datetime.now().isoformat(),
                'data_size': data_size,
                'operations': {}
            }
            
            # Generate test data
            test_data = []
            for i in range(data_size):
                test_data.append({
                    'id': i,
                    'name': f'Player_{i}',
                    'team': f'Team_{i % 10}',
                    'position': f'Pos_{i % 5}',
                    'points': np.random.randint(0, 100),
                    'rebounds': np.random.randint(0, 20),
                    'assists': np.random.randint(0, 15)
                })
            
            # Test pandas DataFrame creation
            start_time = time.time()
            df = pd.DataFrame(test_data)
            df_creation_time = time.time() - start_time
            
            results['operations']['dataframe_creation'] = {
                'total_time': round(df_creation_time, 6),
                'records_per_second': round(data_size / df_creation_time, 2)
            }
            
            # Test data filtering
            start_time = time.time()
            filtered_df = df[df['points'] > 50]
            filtering_time = time.time() - start_time
            
            results['operations']['data_filtering'] = {
                'total_time': round(filtering_time, 6),
                'filters_per_second': round(1 / filtering_time, 2),
                'filtered_records': len(filtered_df)
            }
            
            # Test data grouping
            start_time = time.time()
            grouped_df = df.groupby('team').agg({
                'points': ['mean', 'sum', 'count'],
                'rebounds': ['mean', 'sum'],
                'assists': ['mean', 'sum']
            })
            grouping_time = time.time() - start_time
            
            results['operations']['data_grouping'] = {
                'total_time': round(grouping_time, 6),
                'groups_per_second': round(1 / grouping_time, 2),
                'groups_created': len(grouped_df)
            }
            
            # Test data sorting
            start_time = time.time()
            sorted_df = df.sort_values(['points', 'rebounds', 'assists'], ascending=[False, False, False])
            sorting_time = time.time() - start_time
            
            results['operations']['data_sorting'] = {
                'total_time': round(sorting_time, 6),
                'sorts_per_second': round(1 / sorting_time, 2)
            }
            
            # Test data export to CSV
            start_time = time.time()
            csv_buffer = df.to_csv(index=False)
            csv_export_time = time.time() - start_time
            
            results['operations']['csv_export'] = {
                'total_time': round(csv_export_time, 6),
                'exports_per_second': round(1 / csv_export_time, 2),
                'csv_size_bytes': len(csv_buffer)
            }
            
            # Test data export to JSON
            start_time = time.time()
            json_buffer = df.to_json(orient='records')
            json_export_time = time.time() - start_time
            
            results['operations']['json_export'] = {
                'total_time': round(json_export_time, 6),
                'exports_per_second': round(1 / json_export_time, 2),
                'json_size_bytes': len(json_buffer)
            }
            
            # Calculate overall data processing performance score
            total_time = sum([
                results['operations']['dataframe_creation']['total_time'],
                results['operations']['data_filtering']['total_time'],
                results['operations']['data_grouping']['total_time'],
                results['operations']['data_sorting']['total_time'],
                results['operations']['csv_export']['total_time'],
                results['operations']['json_export']['total_time']
            ])
            
            results['overall_score'] = {
                'total_time': round(total_time, 6),
                'operations_per_second': round(6 / total_time, 2),
                'performance_rating': self._calculate_performance_rating(total_time, data_size)
            }
            
            logger.info("Data processing benchmark completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error benchmarking data processing: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'data_size': data_size
            }
    
    def benchmark_memory_usage(self, data_size: int = 10000) -> Dict[str, Any]:
        """Benchmark memory usage patterns."""
        logger.info(f"Benchmarking memory usage with {data_size} records")
        
        try:
            import psutil
            import gc
            
            results = {
                'timestamp': datetime.now().isoformat(),
                'data_size': data_size,
                'memory_tests': {}
            }
            
            process = psutil.Process()
            
            # Test memory usage for different data structures
            gc.collect()  # Force garbage collection
            initial_memory = process.memory_info().rss
            
            # Test list memory usage
            start_time = time.time()
            test_list = [i for i in range(data_size)]
            list_memory = process.memory_info().rss
            list_time = time.time() - start_time
            
            results['memory_tests']['list'] = {
                'memory_usage_mb': round((list_memory - initial_memory) / (1024**2), 2),
                'creation_time': round(list_time, 6),
                'memory_per_item_bytes': round((list_memory - initial_memory) / data_size, 2)
            }
            
            # Test dictionary memory usage
            start_time = time.time()
            test_dict = {i: f"value_{i}" for i in range(data_size)}
            dict_memory = process.memory_info().rss
            dict_time = time.time() - start_time
            
            results['memory_tests']['dictionary'] = {
                'memory_usage_mb': round((dict_memory - list_memory) / (1024**2), 2),
                'creation_time': round(dict_time, 6),
                'memory_per_item_bytes': round((dict_memory - list_memory) / data_size, 2)
            }
            
            # Test pandas DataFrame memory usage
            start_time = time.time()
            test_df = pd.DataFrame({
                'id': range(data_size),
                'value': [f"value_{i}" for i in range(data_size)],
                'numeric': np.random.randn(data_size)
            })
            df_memory = process.memory_info().rss
            df_time = time.time() - start_time
            
            results['memory_tests']['pandas_dataframe'] = {
                'memory_usage_mb': round((df_memory - dict_memory) / (1024**2), 2),
                'creation_time': round(df_time, 6),
                'memory_per_item_bytes': round((df_memory - dict_memory) / data_size, 2)
            }
            
            # Test numpy array memory usage
            start_time = time.time()
            test_array = np.random.randn(data_size, 10)
            array_memory = process.memory_info().rss
            array_time = time.time() - start_time
            
            results['memory_tests']['numpy_array'] = {
                'memory_usage_mb': round((array_memory - df_memory) / (1024**2), 2),
                'creation_time': round(array_time, 6),
                'memory_per_item_bytes': round((array_memory - df_memory) / data_size, 2)
            }
            
            # Cleanup and measure final memory
            del test_list, test_dict, test_df, test_array
            gc.collect()
            final_memory = process.memory_info().rss
            
            results['memory_cleanup'] = {
                'peak_memory_mb': round((array_memory - initial_memory) / (1024**2), 2),
                'final_memory_mb': round((final_memory - initial_memory) / (1024**2), 2),
                'memory_leak_mb': round((final_memory - initial_memory) / (1024**2), 2)
            }
            
            # Calculate overall memory efficiency score
            total_memory_used = sum([
                results['memory_tests']['list']['memory_usage_mb'],
                results['memory_tests']['dictionary']['memory_usage_mb'],
                results['memory_tests']['pandas_dataframe']['memory_usage_mb'],
                results['memory_tests']['numpy_array']['memory_usage_mb']
            ])
            
            results['overall_score'] = {
                'total_memory_used_mb': round(total_memory_used, 2),
                'memory_efficiency': round(data_size / total_memory_used, 2),
                'performance_rating': self._calculate_memory_rating(total_memory_used, data_size)
            }
            
            logger.info("Memory usage benchmark completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error benchmarking memory usage: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'data_size': data_size
            }
    
    def profile_function(self, func, *args, **kwargs) -> Dict[str, Any]:
        """Profile a specific function using cProfile."""
        logger.info(f"Profiling function: {func.__name__}")
        
        try:
            # Create profiler
            pr = cProfile.Profile()
            pr.enable()
            
            # Run function
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            pr.disable()
            
            # Get stats
            s = io.StringIO()
            ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
            ps.print_stats(20)  # Top 20 functions
            
            profile_output = s.getvalue()
            
            # Parse profile output for key metrics
            lines = profile_output.split('\n')
            total_calls = 0
            total_time = 0
            
            for line in lines:
                if 'function calls' in line and 'in' in line:
                    parts = line.split()
                    if len(parts) >= 4:
                        total_calls = int(parts[0])
                        total_time = float(parts[3])
                        break
            
            profile_results = {
                'timestamp': datetime.now().isoformat(),
                'function_name': func.__name__,
                'execution_time': round(execution_time, 6),
                'profile_stats': {
                    'total_calls': total_calls,
                    'total_time': total_time,
                    'calls_per_second': round(total_calls / total_time, 2) if total_time > 0 else 0
                },
                'detailed_profile': profile_output
            }
            
            logger.info(f"Function profiling completed for {func.__name__}")
            return profile_results
            
        except Exception as e:
            logger.error(f"Error profiling function {func.__name__}: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'function_name': func.__name__,
                'error': str(e)
            }
    
    def run_comprehensive_benchmark(self, iterations: int = 100, data_size: int = 1000) -> Dict[str, Any]:
        """Run comprehensive performance benchmark."""
        logger.info("Starting comprehensive performance benchmark")
        
        try:
            comprehensive_results = {
                'timestamp': datetime.now().isoformat(),
                'benchmark_parameters': {
                    'iterations': iterations,
                    'data_size': data_size
                },
                'benchmarks': {}
            }
            
            # Run all benchmarks
            comprehensive_results['benchmarks']['database'] = self.benchmark_database_operations(iterations)
            comprehensive_results['benchmarks']['api'] = self.benchmark_api_operations(iterations)
            comprehensive_results['benchmarks']['data_processing'] = self.benchmark_data_processing(data_size)
            comprehensive_results['benchmarks']['memory'] = self.benchmark_memory_usage(data_size)
            
            # Calculate overall performance score
            total_time = 0
            total_operations = 0
            
            for benchmark_name, benchmark_results in comprehensive_results['benchmarks'].items():
                if 'overall_score' in benchmark_results:
                    total_time += benchmark_results['overall_score'].get('total_time', 0)
                    total_operations += benchmark_results['overall_score'].get('operations_per_second', 0)
            
            comprehensive_results['overall_performance'] = {
                'total_benchmark_time': round(total_time, 4),
                'total_operations_per_second': round(total_operations, 2),
                'overall_rating': self._calculate_comprehensive_rating(comprehensive_results)
            }
            
            logger.info("Comprehensive performance benchmark completed successfully")
            return comprehensive_results
            
        except Exception as e:
            logger.error(f"Error running comprehensive benchmark: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _calculate_performance_rating(self, total_time: float, operations: int) -> str:
        """Calculate performance rating based on time and operations."""
        if total_time == 0:
            return "EXCELLENT"
        
        ops_per_second = operations / total_time
        
        if ops_per_second >= 1000:
            return "EXCELLENT"
        elif ops_per_second >= 500:
            return "GOOD"
        elif ops_per_second >= 100:
            return "AVERAGE"
        elif ops_per_second >= 10:
            return "POOR"
        else:
            return "VERY_POOR"
    
    def _calculate_memory_rating(self, memory_mb: float, records: int) -> str:
        """Calculate memory efficiency rating."""
        if records == 0:
            return "UNKNOWN"
        
        bytes_per_record = (memory_mb * 1024 * 1024) / records
        
        if bytes_per_record <= 100:
            return "EXCELLENT"
        elif bytes_per_record <= 500:
            return "GOOD"
        elif bytes_per_record <= 1000:
            return "AVERAGE"
        elif bytes_per_record <= 5000:
            return "POOR"
        else:
            return "VERY_POOR"
    
    def _calculate_comprehensive_rating(self, benchmark_results: Dict[str, Any]) -> str:
        """Calculate comprehensive performance rating."""
        ratings = []
        
        for benchmark_name, benchmark_result in benchmark_results['benchmarks'].items():
            if 'overall_score' in benchmark_result:
                rating = benchmark_result['overall_score'].get('performance_rating', 'UNKNOWN')
                ratings.append(rating)
        
        if not ratings:
            return "UNKNOWN"
        
        # Count ratings
        rating_counts = {}
        for rating in ratings:
            rating_counts[rating] = rating_counts.get(rating, 0) + 1
        
        # Determine overall rating
        if rating_counts.get('EXCELLENT', 0) >= len(ratings) * 0.7:
            return "EXCELLENT"
        elif rating_counts.get('GOOD', 0) >= len(ratings) * 0.7:
            return "GOOD"
        elif rating_counts.get('AVERAGE', 0) >= len(ratings) * 0.7:
            return "AVERAGE"
        elif rating_counts.get('POOR', 0) >= len(ratings) * 0.7:
            return "POOR"
        else:
            return "MIXED"
    
    def export_benchmark_results(self, results: Dict[str, Any], output_dir: str = "reports", format: str = "json") -> str:
        """Export benchmark results to file."""
        logger.info(f"Exporting benchmark results to {output_dir}")
        
        try:
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format.lower() == "json":
                filename = f"performance_benchmark_{timestamp}.json"
                filepath = output_path / filename
                
                with open(filepath, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                
                logger.info(f"Benchmark results exported to {filepath}")
                return str(filepath)
                
            elif format.lower() == "txt":
                filename = f"performance_benchmark_{timestamp}.txt"
                filepath = output_path / filename
                
                with open(filepath, 'w') as f:
                    f.write("Sports Data Pipeline - Performance Benchmark Results\n")
                    f.write("=" * 60 + "\n\n")
                    f.write(f"Generated: {results['timestamp']}\n\n")
                    
                    if 'overall_performance' in results:
                        overall = results['overall_performance']
                        f.write("OVERALL PERFORMANCE\n")
                        f.write("-" * 30 + "\n")
                        f.write(f"Total Benchmark Time: {overall['total_benchmark_time']}s\n")
                        f.write(f"Total Operations/Second: {overall['total_operations_per_second']}\n")
                        f.write(f"Overall Rating: {overall['overall_rating']}\n\n")
                    
                    for benchmark_name, benchmark_result in results.get('benchmarks', {}).items():
                        f.write(f"{benchmark_name.upper()} BENCHMARK\n")
                        f.write("-" * 30 + "\n")
                        
                        if 'overall_score' in benchmark_result:
                            score = benchmark_result['overall_score']
                            f.write(f"Performance Rating: {score.get('performance_rating', 'N/A')}\n")
                            f.write(f"Total Time: {score.get('total_time', 'N/A')}s\n")
                            f.write(f"Operations/Second: {score.get('operations_per_second', 'N/A')}\n")
                        
                        f.write("\n")
                
                logger.info(f"Benchmark results exported to {filepath}")
                return str(filepath)
            
            else:
                logger.error(f"Unsupported format: {format}")
                return ""
                
        except Exception as e:
            logger.error(f"Error exporting benchmark results: {e}")
            return ""

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Sports Data Pipeline Performance Tester")
    parser.add_argument('action', choices=[
        'database', 'api', 'data', 'memory', 'profile', 'comprehensive', 'export'
    ], help='Action to perform')
    parser.add_argument('--iterations', '-i', type=int, default=100,
                       help='Number of iterations for benchmarks')
    parser.add_argument('--data-size', '-d', type=int, default=1000,
                       help='Data size for data processing benchmarks')
    parser.add_argument('--output-dir', '-o', default='reports', 
                       help='Output directory for results')
    parser.add_argument('--format', '-f', choices=['json', 'txt'], default='json',
                       help='Output format')
    parser.add_argument('--function', type=str,
                       help='Function name to profile (for profile action)')
    
    args = parser.parse_args()
    
    tester = PerformanceTester()
    
    if args.action == 'database':
        results = tester.benchmark_database_operations(args.iterations)
        print(json.dumps(results, indent=2, default=str))
    
    elif args.action == 'api':
        results = tester.benchmark_api_operations(args.iterations)
        print(json.dumps(results, indent=2, default=str))
    
    elif args.action == 'data':
        results = tester.benchmark_data_processing(args.data_size)
        print(json.dumps(results, indent=2, default=str))
    
    elif args.action == 'memory':
        results = tester.benchmark_memory_usage(args.data_size)
        print(json.dumps(results, indent=2, default=str))
    
    elif args.action == 'profile':
        if not args.function:
            print("Error: Function name is required for profile action")
            return 1
        
        # Import and profile the specified function
        try:
            if args.function == 'get_engine':
                from pipeline.db import get_engine
                results = tester.profile_function(get_engine, "sqlite:///test.db")
            elif args.function == 'rate_limiter':
                from pipeline.utils import RateLimiter
                rate_limiter = RateLimiter()
                results = tester.profile_function(rate_limiter.__enter__, rate_limiter)
            else:
                print(f"Unknown function: {args.function}")
                return 1
            
            print(json.dumps(results, indent=2, default=str))
            
        except ImportError as e:
            print(f"Error importing function: {e}")
            return 1
    
    elif args.action == 'comprehensive':
        results = tester.run_comprehensive_benchmark(args.iterations, args.data_size)
        print(json.dumps(results, indent=2, default=str))
    
    elif args.action == 'export':
        # Run comprehensive benchmark first
        results = tester.run_comprehensive_benchmark(args.iterations, args.data_size)
        filepath = tester.export_benchmark_results(results, args.output_dir, args.format)
        if filepath:
            print(f"Benchmark results exported to: {filepath}")
        else:
            print("Failed to export benchmark results")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
