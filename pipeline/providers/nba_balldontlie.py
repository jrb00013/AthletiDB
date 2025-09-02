import requests
import logging
from datetime import datetime
from typing import List, Dict, Any
from ..normalize import Player, to_row
from ..utils import paginate, polite_delay
import os

logger = logging.getLogger(__name__)

BASE = "https://api.balldontlie.io/v1"  # public; for higher limits, use API key header

def fetch(season: int | None = None) -> List[Dict[str, Any]]:
    """Fetch NBA players from balldontlie.io API."""
    out = []
    page = 1
    per_page = 100
    
    while True:
        params = {"page": page, "per_page": per_page}
        
        # Add API key if available
        headers = {}
        api_key = os.getenv("BALDONTLIE_API_KEY")
        if api_key and api_key != "your_balldontlie_api_key_here":
            headers["Authorization"] = api_key
        else:
            # Try without API key (public access)
            logger.warning("No valid BALDONTLIE_API_KEY found, trying public access")
        
        r = requests.get(f"{BASE}/players", params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        for pl in data.get("data", []):
            team = pl.get("team") or {}
            p = Player(
                id=f"nba_{pl['id']}",
                full_name=f"{pl.get('first_name','').strip()} {pl.get('last_name','').strip()}".strip(),
                first_name=pl.get("first_name"),
                last_name=pl.get("last_name"),
                league="NBA",
                team=team.get("full_name"),
                team_id=str(team.get("id")) if team.get("id") is not None else None,
                position=pl.get("position") or None,
                jersey=None,
                nationality=None,
                birthdate=None,
                height_cm=None,
                weight_kg=None,
                active=True
            )
            out.append(to_row(p))
        
        total_pages = data.get("meta", {}).get("total_pages", page)
        if page >= total_pages:
            break
        
        page += 1
        polite_delay()
    
    return out

def fetch_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    """Fetch recent NBA games for upset detection."""
    try:
        # Get recent games
        params = {"per_page": limit}
        headers = {}
        api_key = os.getenv("BALDONTLIE_API_KEY")
        if api_key:
            headers["Authorization"] = api_key
        
        r = requests.get(f"{BASE}/games", params=params, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        games = []
        for game in data.get("data", []):
            if game.get("status") == "Final":
                games.append({
                    "game_date": game.get("date"),
                    "home_team": game.get("home_team", {}).get("name"),
                    "away_team": game.get("visitor_team", {}).get("name"),
                    "home_score": game.get("home_team_score"),
                    "away_score": game.get("visitor_team_score"),
                    "status": game.get("status")
                })
        
        return games
    except Exception as e:
        print(f"Error fetching NBA games: {e}")
        return []
