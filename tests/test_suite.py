#!/usr/bin/env python3
"""
Sports Data Pipeline Test Suite
Comprehensive testing for all pipeline components.
"""

import unittest
import sys
import os
from pathlib import Path
import tempfile
import shutil
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.db import get_engine, upsert_players, insert_upset, insert_injury
from pipeline.utils import RateLimiter, Cache, export_csv, export_json, export_excel
from pipeline.normalize import detect_upset, Player, Upset, Injury
from pipeline.providers.thesportsdb import TheSportsDBProvider

# Configure logging for tests
logging.basicConfig(level=logging.WARNING)

class TestDatabase(unittest.TestCase):
    """Test database operations."""
    
    def setUp(self):
        """Set up test database."""
        self.test_db_url = "sqlite:///:memory:"
        self.engine = get_engine(self.test_db_url)
    
    def test_database_initialization(self):
        """Test database initialization."""
        self.assertIsNotNone(self.engine)
        
        # Check if tables were created
        with self.engine.connect() as conn:
            result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in result]
            
            expected_tables = ['players', 'upsets', 'injuries', 'team_records', 
                             'games', 'player_stats', 'team_analysis']
            
            for table in expected_tables:
                self.assertIn(table, tables)
    
    def test_player_upsert(self):
        """Test player upsert operations."""
        test_players = [
            {
                "id": "test_1",
                "full_name": "John Doe",
                "league": "NFL",
                "team": "Test Team",
                "position": "QB",
                "active": 1,
                "updated_at": "2024-01-01T00:00:00Z",
                "created_at": "2024-01-01T00:00:00Z"
            }
        ]
        
        upsert_players(self.engine, test_players)
        
        # Verify player was inserted
        with self.engine.connect() as conn:
            result = conn.execute("SELECT * FROM players WHERE id = 'test_1'")
            player = result.fetchone()
            self.assertIsNotNone(player)
            self.assertEqual(player.full_name, "John Doe")
    
    def test_upset_insertion(self):
        """Test upset insertion."""
        test_upset = {
            "league": "NFL",
            "game_date": "2024-01-01",
            "home_team": "Team A",
            "away_team": "Team B",
            "winner": "Team B",
            "loser": "Team A",
            "upset_type": "point_spread",
            "created_at": "2024-01-01T00:00:00Z"
        }
        
        insert_upset(self.engine, test_upset)
        
        # Verify upset was inserted
        with self.engine.connect() as conn:
            result = conn.execute("SELECT * FROM upsets WHERE winner = 'Team B'")
            upset = result.fetchone()
            self.assertIsNotNone(upset)
            self.assertEqual(upset.upset_type, "point_spread")

class TestRateLimiter(unittest.TestCase):
    """Test rate limiting functionality."""
    
    def setUp(self):
        """Set up rate limiter for testing."""
        self.rate_limiter = RateLimiter(requests_per_hour=10, burst_limit=2)
    
    def test_rate_limiter_initialization(self):
        """Test rate limiter initialization."""
        self.assertEqual(self.rate_limiter.requests_per_hour, 10)
        self.assertEqual(self.rate_limiter.burst_limit, 2)
    
    def test_rate_limiter_basic_operation(self):
        """Test basic rate limiter operation."""
        # Should allow first few requests
        self.assertTrue(self.rate_limiter.can_make_request())
        self.rate_limiter.record_request()
        
        self.assertTrue(self.rate_limiter.can_make_request())
        self.rate_limiter.record_request()
        
        # Should hit burst limit
        self.assertFalse(self.rate_limiter.can_make_request())
    
    def test_rate_limiter_reset(self):
        """Test rate limiter reset functionality."""
        # Make some requests
        for _ in range(3):
            self.rate_limiter.record_request()
        
        # Force reset
        self.rate_limiter._reset_if_needed()
        
        # Should be able to make requests again
        self.assertTrue(self.rate_limiter.can_make_request())

class TestCache(unittest.TestCase):
    """Test caching functionality."""
    
    def setUp(self):
        """Set up cache for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache = Cache(self.temp_dir, duration=3600)
    
    def tearDown(self):
        """Clean up test cache."""
        shutil.rmtree(self.temp_dir)
    
    def test_cache_set_get(self):
        """Test cache set and get operations."""
        test_data = {"test": "data", "number": 42}
        
        # Set data in cache
        self.cache.set("test_key", test_data)
        
        # Get data from cache
        retrieved_data = self.cache.get("test_key")
        
        self.assertEqual(retrieved_data, test_data)
    
    def test_cache_expiration(self):
        """Test cache expiration."""
        # Create cache with very short duration
        short_cache = Cache(self.temp_dir, duration=1)
        
        test_data = {"test": "expires"}
        short_cache.set("expire_key", test_data)
        
        # Wait for expiration
        import time
        time.sleep(2)
        
        # Data should be expired
        retrieved_data = short_cache.get("expire_key")
        self.assertIsNone(retrieved_data)

class TestDataNormalization(unittest.TestCase):
    """Test data normalization functions."""
    
    def test_detect_upset(self):
        """Test upset detection logic."""
        # Test point spread upset
        game_data = {
            "home_team": "Favorites",
            "away_team": "Underdogs",
            "home_score": 20,
            "away_score": 28,
            "point_spread": -10.5,  # Favorites favored by 10.5
            "odds_before_game": 1.5
        }
        
        upset = detect_upset(game_data)
        self.assertIsNotNone(upset)
        self.assertEqual(upset["winner"], "Underdogs")
        self.assertEqual(upset["upset_type"], "point_spread")
    
    def test_player_model_validation(self):
        """Test Player model validation."""
        valid_player_data = {
            "id": "test_player",
            "full_name": "Test Player",
            "league": "NFL",
            "team": "Test Team",
            "active": True,
            "updated_at": "2024-01-01T00:00:00Z",
            "created_at": "2024-01-01T00:00:00Z"
        }
        
        player = Player(**valid_player_data)
        self.assertEqual(player.full_name, "Test Player")
        self.assertEqual(player.league, "NFL")

class TestExportFunctions(unittest.TestCase):
    """Test export functionality."""
    
    def setUp(self):
        """Set up test data and directory."""
        self.test_dir = tempfile.mkdtemp()
        self.test_data = [
            {"name": "John", "age": 30, "city": "New York"},
            {"name": "Jane", "age": 25, "city": "Los Angeles"}
        ]
    
    def tearDown(self):
        """Clean up test directory."""
        shutil.rmtree(self.test_dir)
    
    def test_csv_export(self):
        """Test CSV export functionality."""
        output_path = export_csv(self.test_data, self.test_dir, "test", True)
        
        self.assertIsNotNone(output_path)
        self.assertTrue(Path(output_path).exists())
        
        # Check if metadata was created
        metadata_path = Path(self.test_dir) / "test_metadata.json"
        self.assertTrue(metadata_path.exists())
    
    def test_json_export(self):
        """Test JSON export functionality."""
        output_path = export_json(self.test_data, self.test_dir, "test_data.json", True)
        
        self.assertIsNotNone(output_path)
        self.assertTrue(Path(output_path).exists())
    
    def test_excel_export(self):
        """Test Excel export functionality."""
        output_path = export_excel(self.test_data, self.test_dir, "test_data.xlsx", True)
        
        self.assertIsNotNone(output_path)
        self.assertTrue(Path(output_path).exists())

class TestTheSportsDBProvider(unittest.TestCase):
    """Test TheSportsDB provider functionality."""
    
    def setUp(self):
        """Set up provider for testing."""
        # Use test API key
        os.environ["THESPORTSDB_API_KEY"] = "1"
        self.provider = TheSportsDBProvider()
    
    def test_provider_initialization(self):
        """Test provider initialization."""
        self.assertIsNotNone(self.provider)
        self.assertEqual(self.provider.api_key, "1")
    
    def test_rate_limit_status(self):
        """Test rate limit status reporting."""
        status = self.provider.get_rate_limit_status()
        
        self.assertIn("api_name", status)
        self.assertIn("requests_per_hour", status)
        self.assertIn("current_requests", status)
        self.assertEqual(status["api_name"], "thesportsdb")

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline."""
    
    def setUp(self):
        """Set up integration test environment."""
        self.test_db_url = "sqlite:///:memory:"
        self.engine = get_engine(self.test_db_url)
    
    def test_complete_pipeline_flow(self):
        """Test complete pipeline flow."""
        # 1. Insert test data
        test_players = [
            {
                "id": "int_test_1",
                "full_name": "Integration Test Player",
                "league": "NFL",
                "team": "Test Team",
                "position": "QB",
                "active": 1,
                "updated_at": "2024-01-01T00:00:00Z",
                "created_at": "2024-01-01T00:00:00Z"
            }
        ]
        
        upsert_players(self.engine, test_players)
        
        # 2. Insert test upset
        test_upset = {
            "league": "NFL",
            "game_date": "2024-01-01",
            "home_team": "Favorites",
            "away_team": "Underdogs",
            "winner": "Underdogs",
            "loser": "Favorites",
            "upset_type": "point_spread",
            "created_at": "2024-01-01T00:00:00Z"
        }
        
        insert_upset(self.engine, test_upset)
        
        # 3. Verify data integrity
        with self.engine.connect() as conn:
            # Check players
            player_result = conn.execute("SELECT COUNT(*) FROM players")
            player_count = player_result.fetchone()[0]
            self.assertEqual(player_count, 1)
            
            # Check upsets
            upset_result = conn.execute("SELECT COUNT(*) FROM upsets")
            upset_count = upset_result.fetchone()[0]
            self.assertEqual(upset_count, 1)
            
            # Check relationships
            player_upset_result = conn.execute("""
                SELECT p.full_name, u.upset_type 
                FROM players p 
                JOIN upsets u ON p.league = u.league
                WHERE p.id = 'int_test_1'
            """)
            relationship = player_upset_result.fetchone()
            self.assertIsNotNone(relationship)

def run_performance_tests():
    """Run performance tests."""
    print("\n" + "="*50)
    print("PERFORMANCE TESTS")
    print("="*50)
    
    import time
    
    # Test database performance
    start_time = time.time()
    engine = get_engine("sqlite:///:memory:")
    db_init_time = time.time() - start_time
    print(f"Database initialization: {db_init_time:.4f}s")
    
    # Test rate limiter performance
    start_time = time.time()
    rate_limiter = RateLimiter(requests_per_hour=1000, burst_limit=100)
    for _ in range(100):
        rate_limiter.can_make_request()
        rate_limiter.record_request()
    rate_limiter_time = time.time() - start_time
    print(f"Rate limiter operations: {rate_limiter_time:.4f}s")
    
    # Test cache performance
    start_time = time.time()
    cache = Cache(tempfile.mkdtemp(), duration=3600)
    for i in range(100):
        cache.set(f"key_{i}", {"data": f"value_{i}"})
    for i in range(100):
        cache.get(f"key_{i}")
    cache_time = time.time() - start_time
    print(f"Cache operations: {cache_time:.4f}s")

def main():
    """Run all tests."""
    print("Sports Data Pipeline Test Suite")
    print("="*50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestDatabase,
        TestRateLimiter,
        TestCache,
        TestDataNormalization,
        TestExportFunctions,
        TestTheSportsDBProvider,
        TestIntegration
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")
    
    # Run performance tests
    if result.wasSuccessful():
        run_performance_tests()
    
    # Return appropriate exit code
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    sys.exit(main())
