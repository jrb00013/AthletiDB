#!/usr/bin/env python3
"""
Example script demonstrating upset detection and tracking.
This shows how to manually create and track upsets for analysis.
"""

from pipeline.db import get_engine, insert_upset, get_recent_upsets, get_upset_stats
from pipeline.normalize import detect_upset, Upset
from pipeline.utils import format_upset_summary, export_upsets_csv
from datetime import datetime

def create_sample_upsets():
    """Create some sample upset data for demonstration."""
    
    # Initialize database
    engine = get_engine("sqlite:///players.db")
    
    # Sample game results that would constitute upsets
    sample_games = [
        {
            "league": "NBA",
            "home_team": "Lakers",
            "away_team": "Warriors", 
            "home_score": 98,
            "away_score": 112,
            "point_spread": 8.5,  # Lakers were favored by 8.5
            "odds_before_game": 2.8  # Warriors had 2.8 odds
        },
        {
            "league": "NFL",
            "home_team": "Patriots",
            "away_team": "Jets",
            "home_score": 17,
            "away_score": 24,
            "point_spread": 10.0,  # Patriots were favored by 10
            "odds_before_game": 3.2  # Jets had 3.2 odds
        },
        {
            "league": "MLB",
            "home_team": "Yankees",
            "away_team": "Orioles",
            "home_score": 2,
            "away_score": 8,
            "point_spread": None,
            "odds_before_game": 4.1  # Orioles had 4.1 odds
        }
    ]
    
    print("Creating sample upsets...")
    
    for game in sample_games:
        # Detect if this is an upset
        upset = detect_upset(
            league=game["league"],
            home_team=game["home_team"],
            away_team=game["away_team"],
            home_score=game["home_score"],
            away_score=game["away_score"],
            point_spread=game["point_spread"],
            odds_before_game=game["odds_before_game"]
        )
        
        if upset:
            # Insert into database
            upset_data = upset.model_dump()
            insert_upset(engine, upset_data)
            print(f"  ✓ Upset detected: {format_upset_summary(upset_data)}")
        else:
            print(f"  ✗ No upset detected for {game['away_team']} @ {game['home_team']}")
    
    print(f"\nSample upsets created successfully!")

def analyze_upsets():
    """Analyze the created upset data."""
    
    engine = get_engine("sqlite:///players.db")
    
    print("\n=== Upset Analysis ===")
    
    # Get overall stats
    stats = get_upset_stats(engine)
    if stats:
        print(f"Total Upsets: {stats.get('total_upsets', 0)}")
        print(f"Unique Upset Teams: {stats.get('unique_upset_teams', 0)}")
        print(f"Average Magnitude: {stats.get('avg_magnitude', 0):.2f}")
        print(f"Max Magnitude: {stats.get('max_magnitude', 0):.2f}")
    
    # Get recent upsets
    recent = get_recent_upsets(engine, limit=10)
    if recent:
        print(f"\nRecent Upsets:")
        for upset in recent:
            print(f"  {format_upset_summary(upset)}")
    
    # Export to CSV
    if recent:
        csv_path = export_upsets_csv(recent, "exports")
        print(f"\nUpsets exported to: {csv_path}")

def demonstrate_upset_detection():
    """Demonstrate how upset detection works with different scenarios."""
    
    print("\n=== Upset Detection Examples ===")
    
    # Example 1: Point spread upset
    upset1 = detect_upset(
        league="NBA",
        home_team="Celtics",
        away_team="Hornets", 
        home_score=105,
        away_score=118,
        point_spread=12.0  # Celtics favored by 12
    )
    
    if upset1:
        print(f"Point Spread Upset: {format_upset_summary(upset1.model_dump())}")
    
    # Example 2: Odds upset
    upset2 = detect_upset(
        league="NFL",
        home_team="Cowboys",
        away_team="Giants",
        home_score=14,
        away_score=31,
        odds_before_game=5.2  # Giants had 5.2 odds
    )
    
    if upset2:
        print(f"Odds Upset: {format_upset_summary(upset2.model_dump())}")
    
    # Example 3: Close game upset
    upset3 = detect_upset(
        league="MLB",
        home_team="Red Sox",
        away_team="Rays",
        home_score=3,
        away_score=4,
        point_spread=None
    )
    
    if upset3:
        print(f"Performance Upset: {format_upset_summary(upset3.model_dump())}")

if __name__ == "__main__":
    print("Players Pipeline - Upset Tracking Demo")
    print("=" * 50)
    
    try:
        # Create sample data
        create_sample_upsets()
        
        # Analyze the data
        analyze_upsets()
        
        # Demonstrate detection
        demonstrate_upset_detection()
        
        print("\nDemo completed successfully!")
        print("Check the 'exports' directory for CSV files and 'players.db' for the database.")
        
    except Exception as e:
        print(f"Error during demo: {e}")
        import traceback
        traceback.print_exc()
