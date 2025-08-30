import requests
from typing import List, Dict, Any
from ..normalize import Player, to_row
from ..utils import polite_delay

BASE = "https://statsapi.mlb.com/api/v1"

def _teams() -> List[Dict[str, Any]]:
    """Fetch active MLB teams."""
    r = requests.get(f"{BASE}/teams?sportId=1", timeout=30)
    r.raise_for_status()
    return [t for t in r.json().get("teams", []) if t.get("active")]

def _roster(team_id: int) -> List[Dict[str, Any]]:
    """Fetch team roster."""
    r = requests.get(f"{BASE}/teams/{team_id}/roster", timeout=30)
    r.raise_for_status()
    return r.json().get("roster", [])

def _person(person_id: int) -> Dict[str, Any]:
    """Fetch person details."""
    r = requests.get(f"{BASE}/people/{person_id}", timeout=30)
    r.raise_for_status()
    people = r.json().get("people", [])
    return people[0] if people else {}

def fetch(season: int | None = None) -> List[Dict[str, Any]]:
    """Fetch MLB players from MLB Stats API."""
    out = []
    
    for t in _teams():
        tid = t["id"]
        tname = t["name"]
        
        for r in _roster(tid):
            per = r.get("person", {})
            pid = per.get("id")
            
            # enrich minimal bio
            bio = _person(pid) if pid else {}
            
            p = Player(
                id=f"mlb_{pid}",
                full_name=bio.get("fullName") or per.get("fullName"),
                first_name=bio.get("firstName"),
                last_name=bio.get("lastName"),
                league="MLB",
                team=tname,
                team_id=str(tid),
                position=(r.get("position") or {}).get("abbreviation"),
                jersey=str(bio.get("primaryNumber")) if bio.get("primaryNumber") else None,
                nationality=bio.get("birthCountry"),
                birthdate=bio.get("birthDate"),
                height_cm=None,
                weight_kg=None,
                active=True
            )
            out.append(to_row(p))
        
        polite_delay(0.15)
    
    return out

def fetch_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    """Fetch recent MLB games for upset detection."""
    try:
        # Get recent games
        params = {"sportId": 1, "limit": limit}
        r = requests.get(f"{BASE}/schedule", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        games = []
        for date in data.get("dates", []):
            for game in date.get("games", []):
                if game.get("status", {}).get("statusCode") == "F":
                    games.append({
                        "game_date": date.get("date"),
                        "home_team": game.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                        "away_team": game.get("teams", {}).get("away", {}).get("team", {}).get("name"),
                        "home_score": game.get("teams", {}).get("home", {}).get("score"),
                        "away_score": game.get("teams", {}).get("away", {}).get("score"),
                        "status": game.get("status", {}).get("statusCode")
                    })
        
        return games[:limit]
    except Exception as e:
        print(f"Error fetching MLB games: {e}")
        return []
