import requests
from typing import List, Dict, Any
from ..normalize import Player, to_row

URL = "https://api.sleeper.app/v1/players/nfl"  # large JSON (tens of MB)

def fetch(season: int | None = None) -> List[Dict[str, Any]]:
    """Fetch NFL players from Sleeper API."""
    r = requests.get(URL, timeout=120)
    r.raise_for_status()
    data = r.json()
    
    out = []
    for pid, pl in data.items():
        if pl.get("position") is None and not pl.get("full_name"):
            continue
        
        full = pl.get("full_name") or " ".join(filter(None, [pl.get("first_name"), pl.get("last_name")])).strip()
        
        p = Player(
            id=f"nfl_{pid}",
            full_name=full,
            first_name=pl.get("first_name"),
            last_name=pl.get("last_name"),
            league="NFL",
            team=pl.get("team"),
            team_id=pl.get("team"),
            position=pl.get("position"),
            jersey=str(pl.get("number")) if pl.get("number") is not None else None,
            nationality=None,
            birthdate=pl.get("birth_date"),
            height_cm=None,
            weight_kg=None,
            active=bool(pl.get("active", True))
        )
        out.append(to_row(p))
    
    return out

def fetch_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    """Fetch recent NFL games for upset detection."""
    try:
        # NFL games are typically fetched from different sources
        # For now, return empty list - can be extended with ESPN API or similar
        return []
    except Exception as e:
        print(f"Error fetching NFL games: {e}")
        return []
