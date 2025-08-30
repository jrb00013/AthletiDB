import os
import time
import math
import csv
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any

def envget(key: str, default: str | None = None) -> str | None:
    """Get environment variable with fallback to default."""
    val = os.getenv(key)
    return val if val not in ("", None) else default

def ensure_dir(path: str | Path) -> None:
    """Ensure directory exists, create if it doesn't."""
    Path(path).mkdir(parents=True, exist_ok=True)

def export_csv(rows: List[Dict[str, Any]], out_dir: str, league: str) -> str | None:
    """Export rows to CSV file in specified directory."""
    if not rows:
        return None
    
    ensure_dir(out_dir)
    df = pd.DataFrame(rows)
    p = Path(out_dir) / f"{league}_players.csv"
    df.to_csv(p, index=False)
    return str(p)

def export_upsets_csv(upsets: List[Dict[str, Any]], out_dir: str, league: str = None) -> str | None:
    """Export upsets to CSV file."""
    if not upsets:
        return None
    
    ensure_dir(out_dir)
    df = pd.DataFrame(upsets)
    
    if league:
        filename = f"{league}_upsets.csv"
    else:
        filename = "all_upsets.csv"
    
    p = Path(out_dir) / filename
    df.to_csv(p, index=False)
    return str(p)

def paginate(total: int, per_page: int) -> range:
    """Calculate pagination range."""
    pages = math.ceil(total / per_page) if per_page else 1
    return range(1, pages + 1)

def polite_delay(seconds: float = 0.2) -> None:
    """Delay execution to be polite to APIs."""
    time.sleep(seconds)

def format_upset_summary(upset: Dict[str, Any]) -> str:
    """Format upset data into a readable summary."""
    league = upset.get('league', 'Unknown')
    winner = upset.get('winner', 'Unknown')
    loser = upset.get('loser', 'Unknown')
    upset_type = upset.get('upset_type', 'Unknown')
    reason = upset.get('upset_reason', 'No reason provided')
    
    return f"[{league}] {winner} upset {loser} ({upset_type}): {reason}"

def calculate_upset_magnitude(
    point_spread: float = None,
    odds: float = None,
    score_differential: int = None
) -> float:
    """Calculate the magnitude of an upset based on various factors."""
    magnitude = 0.0
    
    if point_spread is not None:
        magnitude += abs(point_spread)
    
    if odds is not None and odds > 1.0:
        magnitude += (odds - 1.0) * 10  # Scale odds impact
    
    if score_differential is not None:
        # Lower differential = higher upset (closer game than expected)
        magnitude += max(0, 10 - score_differential)
    
    return magnitude

def validate_team_names(team1: str, team2: str) -> bool:
    """Basic validation that team names are different and not empty."""
    return (
        team1 and team2 and 
        team1.strip() != team2.strip() and
        len(team1.strip()) > 1 and 
        len(team2.strip()) > 1
    )
