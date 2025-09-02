#!/usr/bin/env python3
"""
Working Player Fetching Script
Uses the providers that are currently working to fetch players for NFL, NBA, and MLB.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def fetch_nfl_players():
    """Fetch NFL players using the working Sleeper provider."""
    print("\n=== Fetching NFL Players ===")
    
    try:
        from pipeline.providers.nfl_sleeper import fetch
        
        print("Fetching NFL players from Sleeper API...")
        players = fetch()
        print(f"✅ Successfully fetched {len(players)} NFL players")
        
        if players:
            # Show sample players
            print("\nSample NFL Players:")
            for i, player in enumerate(players[:5]):
                print(f"  {i+1}. {player.get('full_name', 'Unknown')} - {player.get('position', 'Unknown')} - {player.get('team', 'Unknown')}")
        
        return players
        
    except Exception as e:
        print(f"❌ Failed to fetch NFL players: {e}")
        return []

def fetch_nba_players():
    """Fetch NBA players using balldontlie.io."""
    print("\n=== Fetching NBA Players ===")
    
    try:
        from pipeline.providers.nba_balldontlie import fetch
        
        print("Fetching NBA players from balldontlie.io...")
        players = fetch()
        print(f"✅ Successfully fetched {len(players)} NBA players")
        
        if players:
            # Show sample players
            print("\nSample NBA Players:")
            for i, player in enumerate(players[:5]):
                print(f"  {i+1}. {player.get('full_name', 'Unknown')} - {player.get('position', 'Unknown')} - {player.get('team', 'Unknown')}")
        
        return players
        
    except Exception as e:
        print(f"❌ Failed to fetch NBA players: {e}")
        return []

def fetch_mlb_players():
    """Fetch MLB players using MLB Stats API."""
    print("\n=== Fetching MLB Players ===")
    
    try:
        from pipeline.providers.mlb_statsapi import fetch
        
        print("Fetching MLB players from MLB Stats API...")
        players = fetch()
        print(f"✅ Successfully fetched {len(players)} MLB players")
        
        if players:
            # Show sample players
            print("\nSample MLB Players:")
            for i, player in enumerate(players[:5]):
                print(f"  {i+1}. {player.get('full_name', 'Unknown')} - {player.get('position', 'Unknown')} - {player.get('team', 'Unknown')}")
        
        return players
        
    except Exception as e:
        print(f"❌ Failed to fetch MLB players: {e}")
        return []

def test_thesportsdb_fallback():
    """Test TheSportsDB as a fallback option."""
    print("\n=== Testing TheSportsDB Fallback ===")
    
    try:
        from pipeline.providers.thesportsdb import TheSportsDBProvider
        
        provider = TheSportsDBProvider()
        
        # Test with different endpoints
        test_endpoints = [
            "searchplayers.php",
            "search_all_players.php", 
            "players_all.php"
        ]
        
        for endpoint in test_endpoints:
            try:
                print(f"Testing endpoint: {endpoint}")
                response = provider._make_request(endpoint, {"l": "Basketball_nba"})
                if response:
                    print(f"  ✅ {endpoint} returned data")
                    if "player" in response:
                        print(f"  📊 Found {len(response['player'])} players")
                        return True
                else:
                    print(f"  ❌ {endpoint} failed")
            except Exception as e:
                print(f"  ❌ {endpoint} error: {e}")
        
        return False
        
    except Exception as e:
        print(f"❌ TheSportsDB test failed: {e}")
        return False

def save_players_to_database(players, league):
    """Save players to database."""
    print(f"\n=== Saving {league.upper()} Players to Database ===")
    
    try:
        from pipeline.db import get_engine, upsert_players
        
        # Create database
        engine = get_engine("sqlite:///working_players.db")
        
        if players:
            # Ensure all required fields are present
            normalized_players = []
            for player in players:
                normalized_player = {
                    "id": player.get("id", f"{league}_{hash(player.get('full_name', ''))}"),
                    "full_name": player.get("full_name", "Unknown"),
                    "first_name": player.get("first_name", player.get("full_name", "Unknown").split()[0] if player.get("full_name") else "Unknown"),
                    "last_name": player.get("last_name", " ".join(player.get("full_name", "Unknown").split()[1:]) if player.get("full_name") and len(player.get("full_name", "").split()) > 1 else "Unknown"),
                    "league": league.upper(),
                    "team": player.get("team", "Unknown"),
                    "team_id": player.get("team_id", f"{league}_team_{hash(player.get('team', ''))}"),
                    "position": player.get("position", "Unknown"),
                    "jersey": player.get("jersey", "0"),
                    "nationality": player.get("nationality", "Unknown"),
                    "birthdate": player.get("birthdate", "1900-01-01"),
                    "height_cm": player.get("height_cm", 180.0),
                    "weight_kg": player.get("weight_kg", 80.0),
                    "active": player.get("active", True),
                    "rookie_year": player.get("rookie_year", 2020),
                    "experience_years": player.get("experience_years", 1),
                    "college": player.get("college", "Unknown"),
                    "draft_round": player.get("draft_round", 1),
                    "draft_pick": player.get("draft_pick", 1),
                    "updated_at": "2024-01-01T00:00:00Z",
                    "created_at": "2024-01-01T00:00:00Z"
                }
                normalized_players.append(normalized_player)
            
            # Save to database
            upsert_players(engine, normalized_players)
            print(f"✅ Successfully saved {len(normalized_players)} {league.upper()} players to database")
            
            # Verify
            from sqlalchemy import text
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM players WHERE league = '{league.upper()}'"))
                count = result.fetchone()[0]
                print(f"📊 Database now contains {count} {league.upper()} players")
            
            return True
        else:
            print(f"⚠ No {league.upper()} players to save")
            return False
            
    except Exception as e:
        print(f"❌ Failed to save {league.upper()} players: {e}")
        return False

def main():
    """Main function to fetch players from all working providers."""
    print("Working Player Fetching System")
    print("=" * 50)
    
    # Set environment variables
    os.environ["THESPORTSDB_API_KEY"] = "1"
    
    all_players = {}
    
    # Fetch from working providers
    nfl_players = fetch_nfl_players()
    if nfl_players:
        all_players['nfl'] = nfl_players
        save_players_to_database(nfl_players, 'nfl')
    
    nba_players = fetch_nba_players()
    if nba_players:
        all_players['nba'] = nba_players
        save_players_to_database(nba_players, 'nba')
    
    mlb_players = fetch_mlb_players()
    if mlb_players:
        all_players['mlb'] = mlb_players
        save_players_to_database(mlb_players, 'mlb')
    
    # Test TheSportsDB fallback
    thesportsdb_working = test_thesportsdb_fallback()
    
    # Summary
    print("\n" + "=" * 50)
    print("FETCHING SUMMARY")
    print("=" * 50)
    
    total_players = sum(len(players) for players in all_players.values())
    print(f"Total players fetched: {total_players}")
    
    for league, players in all_players.items():
        print(f"{league.upper()}: {len(players)} players")
    
    print(f"TheSportsDB fallback: {'✅ Working' if thesportsdb_working else '❌ Not working'}")
    
    if all_players:
        print("\n🎉 Player fetching completed successfully!")
        print("\nNext steps:")
        print("1. Check the database: working_players.db")
        print("2. Run: python main.py --league nfl --source legacy")
        print("3. Run: python main.py --league nba --source legacy")
        print("4. Run: python main.py --league mlb --source legacy")
    else:
        print("\n❌ No players were fetched successfully")
    
    return len(all_players) > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
