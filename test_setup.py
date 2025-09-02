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
        print("   [PASSED]  requests")
    except ImportError:
        print("  [FAILED] requests - install with: pip install requests")
        return False
    
    try:
        import pandas
        print("   [PASSED]  pandas")
    except ImportError:
        print("  [FAILED] pandas - install with: pip install pandas")
        return False
    
    try:
        import pydantic
        print("   [PASSED]  pydantic")
    except ImportError:
        print("  [FAILED] pydantic - install with: pip install pydantic")
        return False
    
    try:
        import sqlalchemy
        print("   [PASSED]  sqlalchemy")
    except ImportError:
        print("  [FAILED] sqlalchemy - install with: pip install sqlalchemy")
        return False
    
    try:
        import yaml
        print("   [PASSED]  PyYAML")
    except ImportError:
        print("  [FAILED] PyYAML - install with: pip install PyYAML")
        return False
    
    try:
        import click
        print("   [PASSED]  click")
    except ImportError:
        print("  [FAILED] click - install with: pip install click")
        return False
    
    try:
        import dotenv
        print("   [PASSED]  python-dotenv")
    except ImportError:
        print("  [FAILED] python-dotenv - install with: pip install python-dotenv")
        return False
    
    return True

def test_pipeline_modules():
    """Test that pipeline modules can be imported."""
    print("\nTesting pipeline modules...")
    
    try:
        from pipeline import db
        print("   [PASSED]  pipeline.db")
    except ImportError as e:
        print(f"  [FAILED] pipeline.db: {e}")
        return False
    
    try:
        from pipeline import normalize
        print("   [PASSED]  pipeline.normalize")
    except ImportError as e:
        print(f"  [FAILED] pipeline.normalize: {e}")
        return False
    
    try:
        from pipeline import utils
        print("   [PASSED]  pipeline.utils")
    except ImportError as e:
        print(f"  [FAILED] pipeline.utils: {e}")
        return False
    
    try:
        from pipeline.providers import nba_balldontlie
        print("   [PASSED]  pipeline.providers.nba_balldontlie")
    except ImportError as e:
        print(f"  [FAILED] pipeline.providers.nba_balldontlie: {e}")
        return False
    
    try:
        from pipeline.providers import nfl_sleeper
        print("   [PASSED]  pipeline.providers.nfl_sleeper")
    except ImportError as e:
        print(f"  [FAILED] pipeline.providers.nfl_sleeper: {e}")
        return False
    
    try:
        from pipeline.providers import mlb_statsapi
        print("  [PASSED]  pipeline.providers.mlb_statsapi")
    except ImportError as e:
        print(f"  [FAILED] pipeline.providers.mlb_statsapi: {e}")
        return False
    
    try:
        from pipeline.providers import nhl_api
        print("   [PASSED]  pipeline.providers.nhl_api")
    except ImportError as e:
        print(f"  [FAILED] pipeline.providers.nhl_api: {e}")
        return False
    
    return True

def test_configuration():
    """Test configuration file and settings."""
    print("\nTesting configuration...")
    
    config_file = Path("config.yaml")
    if not config_file.exists():
        print("  [FAILED] config.yaml not found")
        return False
    
    try:
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        required_keys = ['database_url', 'csv_dir', 'leagues', 'seasons', 'upsets']
        for key in required_keys:
            if key not in config:
                print(f"  [FAILED] Missing config key: {key}")
                return False
        
        print("   [PASSED]  config.yaml loaded successfully")
        print(f"   [PASSED]  Database: {config['database_url']}")
        print(f"   [PASSED]  CSV Directory: {config['csv_dir']}")
        print(f"   [PASSED] Leagues: {', '.join(config['leagues'])}")
        
        return True
        
    except Exception as e:
        print(f"  [FAILED] Error loading config: {e}")
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
            "first_name": "Test",
            "last_name": "Player",
            "league": "TEST",
            "team": "Test Team",
            "team_id": "TEST001",
            "position": "QB",
            "jersey": "12",
            "nationality": "US",
            "birthdate": "1990-01-01",
            "height_cm": 180.0,
            "weight_kg": 85.0,
            "active": 1,
            "rookie_year": 2010,
            "experience_years": 5,
            "college": "Test University",
            "draft_round": 1,
            "draft_pick": 10,
            "updated_at": "2024-01-01T00:00:00Z",
            "created_at": "2024-01-01T00:00:00Z"
        }
        
        upsert_players(engine, [test_player])
        print("   [PASSED]  Database schema created")
        print("   [PASSED]  Player upsert test passed")
        
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
        print("  [PASSED] Upset insert test passed")
        
        return True
        
    except Exception as e:
        print(f"  [FAILED] Database test failed: {e}")
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
                print("  [PASSED]NBA API (balldontlie.io) - accessible")
            else:
                print(f"  [WARNING]   NBA API returned status {response.status_code}")
        except Exception as e:
            print(f"   [FAILURE]  NBA API test failed: {e}")
        
        # Test NFL provider (Sleeper)
        try:
            response = requests.get("https://api.sleeper.app/v1/players/nfl", timeout=30)
            if response.status_code == 200:
                print("  [PASSED] NFL API (Sleeper) - accessible")
            else:
                print(f"  [WARNING] NFL API returned status {response.status_code}")
        except Exception as e:
            print(f"   [FAILURE] NFL API test failed: {e}")
        
        # Test MLB provider
        try:
            response = requests.get("https://statsapi.mlb.com/api/v1/teams?sportId=1", timeout=10)
            if response.status_code == 200:
                print("  [PASSED] MLB API - accessible")
            else:
                print(f"   [WARNING]  MLB API returned status {response.status_code}")
        except Exception as e:
            print(f"   [FAILURE]  MLB API test failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"  [FAILED] Provider connectivity test failed: {e}")
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
        print("[PASSED] All tests passed! Your setup is ready.")
        print("\nNext steps:")
        print("1. Run: python main.py --help")
        print("2. Try: python main.py --league nba")
        print("3. Test upsets: python example_upsets.py")
    else:
        print(" [ERRORSome tests failed. Please check the errors above.")
        print("\nCommon solutions:")
        print("1. Install missing dependencies: pip install -r requirements.txt")
        print("2. Check file permissions and paths")
        print("3. Verify internet connectivity for API tests")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
