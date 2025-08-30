from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any

class Player(BaseModel):
    """Normalized player model across all leagues."""
    id: str
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    league: str
    team: Optional[str] = None
    team_id: Optional[str] = None
    position: Optional[str] = None
    jersey: Optional[str] = None
    nationality: Optional[str] = None
    birthdate: Optional[str] = None  # ISO date
    height_cm: Optional[float] = None
    weight_kg: Optional[float] = None
    active: Optional[bool] = True
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

class Upset(BaseModel):
    """Model for tracking upsets and surprising game outcomes."""
    league: str
    game_date: str
    home_team: str
    away_team: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    winner: str
    loser: str
    upset_type: str  # 'point_spread', 'odds', 'performance', 'historical'
    upset_reason: Optional[str] = None
    point_spread: Optional[float] = None
    odds_before_game: Optional[float] = None
    upset_magnitude: float
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

def to_row(p: Player) -> Dict[str, Any]:
    """Convert Player model to database row format."""
    d = p.model_dump()
    d["active"] = int(bool(d["active"]))
    return d

def to_upset_row(u: Upset) -> Dict[str, Any]:
    """Convert Upset model to database row format."""
    return u.model_dump()

def detect_upset(
    league: str,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    point_spread: Optional[float] = None,
    odds_before_game: Optional[float] = None,
    league_config: Optional[Dict[str, Any]] = None
) -> Optional[Upset]:
    """
    Detect if a game result constitutes an upset based on league-specific criteria.
    
    Args:
        league: The sports league
        home_team: Home team name
        away_team: Away team name
        home_score: Home team final score
        away_score: Away team final score
        point_spread: Point spread (positive = home favored, negative = away favored)
        odds_before_game: Pre-game odds for the winner
        league_config: League-specific upset thresholds
        
    Returns:
        Upset object if upset detected, None otherwise
    """
    winner = home_team if home_score > away_score else away_team
    loser = away_team if home_score > away_score else home_team
    winner_score = max(home_score, away_score)
    loser_score = min(home_score, away_score)
    
    upset_detected = False
    upset_type = None
    upset_reason = None
    upset_magnitude = 0.0
    
    # Check point spread upset
    if point_spread is not None:
        if point_spread > 0:  # Home team favored
            if home_score < away_score:  # Away team won
                upset_detected = True
                upset_type = "point_spread"
                upset_reason = f"Away team {away_team} beat favored home team {home_team} by {point_spread + (away_score - home_score)} points"
                upset_magnitude = abs(point_spread) + (away_score - home_score)
        else:  # Away team favored
            if home_score > away_score:  # Home team won
                upset_detected = True
                upset_type = "point_spread"
                upset_reason = f"Home team {home_team} beat favored away team {away_team} by {abs(point_spread) + (home_score - away_score)} points"
                upset_magnitude = abs(point_spread) + (home_score - away_score)
    
    # Check odds upset
    if odds_before_game is not None and odds_before_game > 2.0:
        upset_detected = True
        if upset_type is None:
            upset_type = "odds"
        upset_reason = f"{winner} won with {odds_before_game:.1f} odds"
        upset_magnitude = max(upset_magnitude, odds_before_game)
    
    # Check score differential upset (for close games that shouldn't have been close)
    if league_config:
        score_diff = abs(winner_score - loser_score)
        if score_diff <= 3:  # Close game
            upset_detected = True
            if upset_type is None:
                upset_type = "performance"
            upset_reason = f"Close game with {score_diff} point differential"
            upset_magnitude = max(upset_magnitude, 5.0 - score_diff)
    
    if not upset_detected:
        return None
    
    return Upset(
        league=league,
        game_date=datetime.now().strftime("%Y-%m-%d"),
        home_team=home_team,
        away_team=away_team,
        home_score=home_score,
        away_score=away_score,
        winner=winner,
        loser=loser,
        upset_type=upset_type,
        upset_reason=upset_reason,
        point_spread=point_spread,
        odds_before_game=odds_before_game,
        upset_magnitude=upset_magnitude
    )
