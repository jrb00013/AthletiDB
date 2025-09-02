#!/usr/bin/env python3
"""
Test Player Fetching Script
Tests player fetching functionality for NFL, NBA, and MLB from all available providers.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_thesportsdb_provider():
    """Test TheSportsDB provider for all leagues."""
    print("\n=== Testing TheSportsDB Provider ===")
    
    try:
        from pipeline.providers.thesportsdb import TheSportsDBProvider, test_api_connectivity
        
        # Test API connectivity first
        print("Testing API connectivity...")
        connectivity = test_api_connectivity()
        print(f"  Status: {connectivity['success']}")
        print(f"  Message: {connectivity['message']}")
        print(f"  API Key Present: {connectivity['api_key_present']}")
        
        if not connectivity['success']:
            print("  ❌ API connectivity test failed")
            return False
        
        # Test player fetching for each league
        leagues = ['nfl', 'nba', 'mlb']
        provider = TheSportsDBProvider()
        
        for league in leagues:
            print(f"\nTesting {league.upper()} player fetching...")
            try:
                players = provider.fetch_players(league)
                print(f"  ✅ Successfully fetched {len(players)} {league.upper()} players")
                
                if players:
                    # Show sample player data
                    sample_player = players[0]
                    print(f"  Sample player: {sample_player.get('full_name', 'Unknown')} - {sample_player.get('position', 'Unknown')} - {sample_player.get('team', 'Unknown')}")
                
            except Exception as e:
                print(f"  ❌ Failed to fetch {league.upper()} players: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ TheSportsDB provider test failed: {e}")
        return False

def test_nba_balldontlie_provider():
    """Test NBA balldontlie.io provider."""
    print("\n=== Testing NBA balldontlie.io Provider ===")
    
    try:
        from pipeline.providers.nba_balldontlie import fetch
        
        print("Testing NBA player fetching...")
        players = fetch()
        print(f"  ✅ Successfully fetched {len(players)} NBA players")
        
        if players:
            # Show sample player data
            sample_player = players[0]
            print(f"  Sample player: {sample_player.get('full_name', 'Unknown')} - {sample_player.get('position', 'Unknown')} - {sample_player.get('team', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ NBA balldontlie.io provider test failed: {e}")
        return False

def test_nfl_sleeper_provider():
    """Test NFL Sleeper provider."""
    print("\n=== Testing NFL Sleeper Provider ===")
    
    try:
        from pipeline.providers.nfl_sleeper import fetch
        
        print("Testing NFL player fetching...")
        players = fetch()
        print(f"  ✅ Successfully fetched {len(players)} NFL players")
        
        if players:
            # Show sample player data
            sample_player = players[0]
            print(f"  Sample player: {sample_player.get('full_name', 'Unknown')} - {sample_player.get('position', 'Unknown')} - {sample_player.get('team', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ NFL Sleeper provider test failed: {e}")
        return False

def test_mlb_statsapi_provider():
    """Test MLB Stats API provider."""
    print("\n=== Testing MLB Stats API Provider ===")
    
    try:
        from pipeline.providers.mlb_statsapi import fetch
        
        print("Testing MLB player fetching...")
        players = fetch()
        print(f"  ✅ Successfully fetched {len(players)} MLB players")
        
        if players:
            # Show sample player data
            sample_player = players[0]
            print(f"  Sample player: {sample_player.get('full_name', 'Unknown')} - {sample_player.get('position', 'Unknown')} - {sample_player.get('team', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ MLB Stats API provider test failed: {e}")
        return False

def test_database_integration():
    """Test database integration with fetched players."""
    print("\n=== Testing Database Integration ===")
    
    try:
        from pipeline.db import get_engine, upsert_players
        from pipeline.providers.thesportsdb import fetch_players
        
        # Create test database
        engine = get_engine("sqlite:///:memory:")
        
        # Fetch a small sample of players
        print("Fetching sample players for database test...")
        nba_players = fetch_players('nba')
        nfl_players = fetch_players('nfl')
        
        total_players = len(nba_players) + len(nfl_players)
        print(f"  Total players fetched: {total_players}")
        
        if total_players > 0:
            # Test database insertion
            all_players = nba_players + nfl_players
            upsert_players(engine, all_players)
            print(f"  ✅ Successfully inserted {total_players} players into database")
            
            # Verify data in database
            from sqlalchemy import text
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM players"))
                count = result.fetchone()[0]
                print(f"  ✅ Database contains {count} players")
                
                # Check league distribution
                result = conn.execute(text("SELECT league, COUNT(*) FROM players GROUP BY league"))
                for league, count in result:
                    print(f"    {league}: {count} players")
            
            return True
        else:
            print("  ⚠ No players fetched, skipping database test")
            return False
        
    except Exception as e:
        print(f"❌ Database integration test failed: {e}")
        return False

def main():
    """Run all player fetching tests."""
    print("Player Fetching Test Suite")
    print("=" * 50)
    
    # Set environment variable for testing
    os.environ["THESPORTSDB_API_KEY"] = "1"
    
    tests = [
        ("TheSportsDB Provider", test_thesportsdb_provider),
        ("NBA balldontlie.io Provider", test_nba_balldontlie_provider),
        ("NFL Sleeper Provider", test_nfl_sleeper_provider),
        ("MLB Stats API Provider", test_mlb_statsapi_provider),
        ("Database Integration", test_database_integration)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} test passed")
            else:
                print(f"❌ {test_name} test failed")
        except Exception as e:
            print(f"❌ {test_name} test error: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All player fetching tests passed!")
        print("\nNext steps:")
        print("1. Set proper API keys in environment variables")
        print("2. Run: python main.py --league nba --source live")
        print("3. Run: python main.py --league nfl --source live")
        print("4. Run: python main.py --league mlb --source live")
    else:
        print("❌ Some tests failed. Check the errors above.")
        print("\nCommon solutions:")
        print("1. Check API key configuration")
        print("2. Verify internet connectivity")
        print("3. Check provider implementations")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
