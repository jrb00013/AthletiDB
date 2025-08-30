#!/usr/bin/env python3
"""
Test script to verify the Players Pipeline setup.
Run this to check that all components are working correctly.
"""

import sys
import os
from pathlib import Path

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    
    try:
        import requests
        print("  ✓ requests")
    except ImportError:
        print("  ✗ requests - install with: pip install requests")
        return False
    
    try:
        import pandas
        print("  ✓ pandas")
    except ImportError:
        print("  ✗ pandas - install with: pip install pandas")
        return False
    
    try:
        import pydantic
        print("  ✓ pydantic")
    except ImportError:
        print("  ✗ pydantic - install with: pip install pydantic")
        return False
    
    try:
        import sqlalchemy
        print("  ✓ sqlalchemy")
    except ImportError:
        print("  ✗ sqlalchemy - install with: pip install sqlalchemy")
        return False
    
    try:
        import yaml
        print("  ✓ PyYAML")
    except ImportError:
        print("  ✗ PyYAML - install with: pip install PyYAML")
        return False
    
    try:
        import click
        print("  ✓ click")
    except ImportError:
        print("  ✗ click - install with: pip install click")
        return False
    
    try:
        import dotenv
        print("  ✓ python-dotenv")
    except ImportError:
        print("  ✗ python-dotenv - install with: pip install python-dotenv")
        return False
    
    return True

def test_pipeline_modules():
    """Test that pipeline modules can be imported."""
    print("\nTesting pipeline modules...")
    
    try:
        from pipeline import db
        print("  ✓ pipeline.db")
    except ImportError as e:
        print(f"  ✗ pipeline.db: {e}")
        return False
    
    try:
        from pipeline import normalize
        print("  ✓ pipeline.normalize")
    except ImportError as e:
        print(f"  ✗ pipeline.normalize: {e}")
        return False
    
    try:
        from pipeline import utils
        print("  ✓ pipeline.utils")
    except ImportError as e:
        print(f"  ✗ pipeline.utils: {e}")
        return False
    
    try:
        from pipeline.providers import nba_balldontlie
        print("  ✓ pipeline.providers.nba_balldontlie")
    except ImportError as e:
        print(f"  ✗ pipeline.providers.nba_balldontlie: {e}")
        return False
    
    try:
        from pipeline.providers import nfl_sleeper
        print("  ✓ pipeline.providers.nfl_sleeper")
    except ImportError as e:
        print(f"  ✗ pipeline.providers.nfl_sleeper: {e}")
        return False
    
    try:
        from pipeline.providers import mlb_statsapi
        print("  ✓ pipeline.providers.mlb_statsapi")
    except ImportError as e:
        print(f"  ✗ pipeline.providers.mlb_statsapi: {e}")
        return False
    
    try:
        from pipeline.providers import nhl_api
        print("  ✓ pipeline.providers.nhl_api")
    except ImportError as e:
        print(f"  ✗ pipeline.providers.nhl_api: {e}")
        return False
    
    return True

def test_configuration():
    """Test configuration file and settings."""
    print("\nTesting configuration...")
    
    config_file = Path("config.yaml")
    if not config_file.exists():
        print("  ✗ config.yaml not found")
        return False
    
    try:
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        required_keys = ['database_url', 'csv_dir', 'leagues', 'seasons', 'upsets']
        for key in required_keys:
            if key not in config:
                print(f"  ✗ Missing config key: {key}")
                return False
        
        print("  ✓ config.yaml loaded successfully")
        print(f"  ✓ Database: {config['database_url']}")
        print(f"  ✓ CSV Directory: {config['csv_dir']}")
        print(f"  ✓ Leagues: {', '.join(config['leagues'])}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error loading config: {e}")
        return False

def test_database_connection():
    """Test database connection and schema creation."""
    print("\nTesting database connection...")
    
    try:
        from pipeline.db import get_engine
        
        # Test with in-memory database
        engine = get_engine("sqlite:///:memory:")
        
        # Test basic operations
        from pipeline.db import upsert_players, insert_upset
        
        # Test player upsert
        test_player = {
            "id": "test_1",
            "full_name": "Test Player",
            "league": "TEST",
            "active": 1,
            "updated_at": "2024-01-01T00:00:00Z"
        }
        
        upsert_players(engine, [test_player])
        print("  ✓ Database schema created")
        print("  ✓ Player upsert test passed")
        
        # Test upset insert
        test_upset = {
            "league": "TEST",
            "game_date": "2024-01-01",
            "home_team": "Home Team",
            "away_team": "Away Team",
            "winner": "Away Team",
            "loser": "Home Team",
            "upset_type": "test",
            "upset_magnitude": 5.0,
            "created_at": "2024-01-01T00:00:00Z"
        }
        
        insert_upset(engine, test_upset)
        print("  ✓ Upset insert test passed")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Database test failed: {e}")
        return False

def test_provider_connectivity():
    """Test basic connectivity to data providers."""
    print("\nTesting provider connectivity...")
    
    try:
        import requests
        
        # Test NBA provider (balldontlie.io)
        try:
            response = requests.get("https://api.balldontlie.io/v1/players?per_page=1", timeout=10)
            if response.status_code == 200:
                print("  ✓ NBA API (balldontlie.io) - accessible")
            else:
                print(f"  ⚠ NBA API returned status {response.status_code}")
        except Exception as e:
            print(f"  ⚠ NBA API test failed: {e}")
        
        # Test NFL provider (Sleeper)
        try:
            response = requests.get("https://api.sleeper.app/v1/players/nfl", timeout=30)
            if response.status_code == 200:
                print("  ✓ NFL API (Sleeper) - accessible")
            else:
                print(f"  ⚠ NFL API returned status {response.status_code}")
        except Exception as e:
            print(f"  ⚠ NFL API test failed: {e}")
        
        # Test MLB provider
        try:
            response = requests.get("https://statsapi.mlb.com/api/v1/teams?sportId=1", timeout=10)
            if response.status_code == 200:
                print("  ✓ MLB API - accessible")
            else:
                print(f"  ⚠ MLB API returned status {response.status_code}")
        except Exception as e:
            print(f"  ⚠ MLB API test failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Provider connectivity test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Players Pipeline - Setup Test")
    print("=" * 40)
    
    tests = [
        ("Dependencies", test_imports),
        ("Pipeline Modules", test_pipeline_modules),
        ("Configuration", test_configuration),
        ("Database", test_database_connection),
        ("Provider Connectivity", test_provider_connectivity)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"  {test_name} test failed")
        except Exception as e:
            print(f"  {test_name} test error: {e}")
    
    print("\n" + "=" * 40)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your setup is ready.")
        print("\nNext steps:")
        print("1. Run: python main.py --help")
        print("2. Try: python main.py --league nba")
        print("3. Test upsets: python example_upsets.py")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        print("\nCommon solutions:")
        print("1. Install missing dependencies: pip install -r requirements.txt")
        print("2. Check file permissions and paths")
        print("3. Verify internet connectivity for API tests")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
