#!/usr/bin/env python3
"""
NBA Fallback Provider
Provides NBA player data when primary providers are unavailable.
"""

import requests
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def fetch_nba_players_fallback() -> List[Dict[str, Any]]:
    """Fetch NBA players using fallback methods."""
    logger.info("Fetching NBA players using fallback methods...")
    
    # Skip external API calls that might hang and go straight to sample data
    try:
        players = create_sample_nba_players()
        if players:
            logger.info(f"Successfully created {len(players)} sample NBA players")
            return players
    except Exception as e:
        logger.warning(f"Sample player creation failed: {e}")
    
    logger.error("NBA fallback failed")
    return []

def fetch_from_espn_fallback() -> List[Dict[str, Any]]:
    """Try to fetch from ESPN's public data."""
    try:
        # ESPN has some public endpoints that might work
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        players = []
        
        # Extract team and player information
        for team in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team_info = team.get("team", {})
            team_name = team_info.get("name", "Unknown")
            team_id = team_info.get("id", "unknown")
            
            # Create sample players for each team (since detailed player data might not be available)
            sample_positions = ["PG", "SG", "SF", "PF", "C"]
            for i, pos in enumerate(sample_positions):
                player = {
                    "id": f"nba_fallback_{team_id}_{i}",
                    "full_name": f"Sample {pos} Player",
                    "first_name": "Sample",
                    "last_name": f"{pos} Player",
                    "league": "NBA",
                    "team": team_name,
                    "team_id": str(team_id),
                    "position": pos,
                    "jersey": str(i + 1),
                    "nationality": "US",
                    "birthdate": "1990-01-01",
                    "height_cm": 190.0,
                    "weight_kg": 85.0,
                    "active": True,
                    "rookie_year": 2020,
                    "experience_years": 3,
                    "college": "Sample University",
                    "draft_round": 1,
                    "draft_pick": 1,
                    "updated_at": datetime.utcnow().isoformat() + "Z",
                    "created_at": datetime.utcnow().isoformat() + "Z"
                }
                players.append(player)
        
        return players[:100]  # Limit to 100 players for testing
        
    except Exception as e:
        logger.warning(f"ESPN fallback failed: {e}")
        return []

def fetch_from_basketball_reference_fallback() -> List[Dict[str, Any]]:
    """Try to fetch from Basketball Reference public data."""
    try:
        # Basketball Reference has some public data
        url = "https://www.basketball-reference.com/leaders/pts_career.html"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # This is a basic approach - in practice you'd parse the HTML
        # For now, return sample data
        return create_sample_nba_players()
        
    except Exception as e:
        logger.warning(f"Basketball Reference fallback failed: {e}")
        return []

def create_sample_nba_players() -> List[Dict[str, Any]]:
    """Create sample NBA players for testing when all else fails."""
    logger.info("Creating sample NBA players for testing")
    
    # Sample NBA teams
    nba_teams = [
        "Los Angeles Lakers", "Boston Celtics", "Golden State Warriors", 
        "Chicago Bulls", "Miami Heat", "Dallas Mavericks", "New York Knicks",
        "Philadelphia 76ers", "Toronto Raptors", "Milwaukee Bucks"
    ]
    
    # Sample positions
    positions = ["PG", "SG", "SF", "PF", "C"]
    
    # Sample player names (real NBA players for testing)
    sample_names = [
        "LeBron James", "Stephen Curry", "Kevin Durant", "Giannis Antetokounmpo",
        "Nikola Jokic", "Joel Embiid", "Luka Doncic", "Jayson Tatum",
        "Devin Booker", "Damian Lillard", "Jimmy Butler", "Bam Adebayo",
        "Anthony Davis", "Russell Westbrook", "Chris Paul", "Kawhi Leonard",
        "Paul George", "Bradley Beal", "Donovan Mitchell", "Rudy Gobert"
    ]
    
    players = []
    for i, name in enumerate(sample_names):
        team = nba_teams[i % len(nba_teams)]
        position = positions[i % len(positions)]
        
        # Split name into first and last
        name_parts = name.split()
        first_name = name_parts[0] if name_parts else "Unknown"
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else "Unknown"
        
        player = {
            "id": f"nba_sample_{i+1}",
            "full_name": name,
            "first_name": first_name,
            "last_name": last_name,
            "league": "NBA",
            "team": team,
            "team_id": f"nba_team_{hash(team) % 1000}",
            "position": position,
            "jersey": str((i % 99) + 1),
            "nationality": "US",
            "birthdate": "1990-01-01",
            "height_cm": 190.0 + (i % 20),
            "weight_kg": 80.0 + (i % 30),
            "active": True,
            "rookie_year": 2015 + (i % 10),
            "experience_years": 1 + (i % 15),
            "college": "Sample University",
            "draft_round": 1,
            "draft_pick": 1 + (i % 30),
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        players.append(player)
    
    return players

def fetch(season: int | None = None) -> List[Dict[str, Any]]:
    """Main fetch function for NBA players."""
    return fetch_nba_players_fallback()

def fetch_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    """Fetch recent NBA games for upset detection."""
    logger.info("Fetching recent NBA games using fallback method")
    
    # Create sample recent games
    sample_games = []
    for i in range(limit):
        game = {
            "game_date": datetime.now().strftime("%Y-%m-%d"),
            "home_team": "Sample Home Team",
            "away_team": "Sample Away Team",
            "home_score": 100 + (i % 20),
            "away_score": 95 + (i % 25),
            "status": "Final"
        }
        sample_games.append(game)
    
    return sample_games
