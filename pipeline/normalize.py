from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional, Dict, Any, List
import json

class Player(BaseModel):
    """Enhanced player model with comprehensive data."""
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
    rookie_year: Optional[int] = None
    experience_years: Optional[int] = None
    college: Optional[str] = None
    draft_round: Optional[int] = None
    draft_pick: Optional[int] = None
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('height_cm', 'weight_kg')
    def validate_physical_attributes(cls, v):
        if v is not None:
            if v <= 0:
                raise ValueError('Physical attributes must be positive')
        return v

    @validator('rookie_year', 'experience_years', 'draft_round', 'draft_pick')
    def validate_integer_fields(cls, v):
        if v is not None:
            if v < 0:
                raise ValueError('Integer fields must be non-negative')
        return v

class Upset(BaseModel):
    """Enhanced upset model with additional context."""
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
    game_quarter: Optional[str] = None
    time_remaining: Optional[str] = None
    weather_conditions: Optional[str] = None
    attendance: Optional[int] = None
    tv_network: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('upset_type')
    def validate_upset_type(cls, v):
        valid_types = ['point_spread', 'odds', 'performance', 'historical']
        if v not in valid_types:
            raise ValueError(f'Upset type must be one of: {valid_types}')
        return v

    @validator('upset_magnitude')
    def validate_magnitude(cls, v):
        if v < 0:
            raise ValueError('Upset magnitude must be non-negative')
        return v

class Injury(BaseModel):
    """Model for tracking player injuries."""
    player_id: str
    league: str
    team: str
    injury_type: Optional[str] = None
    body_part: Optional[str] = None
    severity: str  # 'questionable', 'doubtful', 'out', 'ir'
    status: str  # 'active', 'questionable', 'doubtful', 'out', 'ir'
    injury_date: str
    expected_return: Optional[str] = None
    games_missed: int = 0
    notes: Optional[str] = None
    source: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('severity', 'status')
    def validate_injury_fields(cls, v):
        valid_values = ['questionable', 'doubtful', 'out', 'ir', 'active']
        if v not in valid_values:
            raise ValueError(f'Value must be one of: {valid_values}')
        return v

    @validator('games_missed')
    def validate_games_missed(cls, v):
        if v < 0:
            raise ValueError('Games missed must be non-negative')
        return v

class TeamRecord(BaseModel):
    """Model for team records and statistics."""
    team: str
    league: str
    season: int
    wins: int = 0
    losses: int = 0
    ties: int = 0
    win_percentage: Optional[float] = None
    points_for: int = 0
    points_against: int = 0
    point_differential: int = 0
    home_wins: int = 0
    home_losses: int = 0
    away_wins: int = 0
    away_losses: int = 0
    division_wins: int = 0
    division_losses: int = 0
    conference_wins: int = 0
    conference_losses: int = 0
    playoff_appearance: bool = False
    playoff_seed: Optional[int] = None
    playoff_wins: int = 0
    playoff_losses: int = 0
    championships: int = 0
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('season')
    def validate_season(cls, v):
        if v < 1900 or v > 2100:
            raise ValueError('Season must be between 1900 and 2100')
        return v

    @validator('wins', 'losses', 'ties', 'points_for', 'points_against')
    def validate_non_negative(cls, v):
        if v < 0:
            raise ValueError('Value must be non-negative')
        return v

class Game(BaseModel):
    """Model for game results and statistics."""
    league: str
    season: int
    game_date: str
    home_team: str
    away_team: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_team_stats: Optional[str] = None  # JSON string
    away_team_stats: Optional[str] = None  # JSON string
    venue: Optional[str] = None
    attendance: Optional[int] = None
    weather: Optional[str] = None
    tv_network: Optional[str] = None
    game_status: str = 'scheduled'  # 'scheduled', 'live', 'final', 'postponed'
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('game_status')
    def validate_game_status(cls, v):
        valid_statuses = ['scheduled', 'live', 'final', 'postponed', 'cancelled']
        if v not in valid_statuses:
            raise ValueError(f'Game status must be one of: {valid_statuses}')
        return v

    @validator('home_team_stats', 'away_team_stats')
    def validate_stats_json(cls, v):
        if v is not None:
            try:
                json.loads(v)
            except json.JSONDecodeError:
                raise ValueError('Stats must be valid JSON string')
        return v

class PlayerStats(BaseModel):
    """Model for player statistics by game/season."""
    player_id: str
    league: str
    season: int
    game_id: Optional[int] = None
    team: str
    opponent: Optional[str] = None
    game_date: Optional[str] = None
    stats_data: str  # JSON string with detailed statistics
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('stats_data')
    def validate_stats_json(cls, v):
        try:
            json.loads(v)
        except json.JSONDecodeError:
            raise ValueError('Stats data must be valid JSON string')
        return v

class TeamAnalysis(BaseModel):
    """Model for team strengths and weaknesses analysis."""
    team: str
    league: str
    season: int
    analysis_type: str  # 'strength', 'weakness', 'trend'
    category: str  # 'offense', 'defense', 'special_teams', etc.
    description: str
    confidence_score: float  # 0.0 to 1.0
    supporting_data: Optional[str] = None  # JSON string with evidence
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")+"Z")

    @validator('analysis_type')
    def validate_analysis_type(cls, v):
        valid_types = ['strength', 'weakness', 'trend']
        if v not in valid_types:
            raise ValueError(f'Analysis type must be one of: {valid_types}')
        return v

    @validator('confidence_score')
    def validate_confidence(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError('Confidence score must be between 0.0 and 1.0')
        return v

    @validator('supporting_data')
    def validate_supporting_data(cls, v):
        if v is not None:
            try:
                json.loads(v)
            except json.JSONDecodeError:
                raise ValueError('Supporting data must be valid JSON string')
        return v

# Conversion functions
def to_row(p: Player) -> Dict[str, Any]:
    """Convert Player model to database row format."""
    d = p.model_dump()
    d["active"] = int(bool(d["active"]))
    return d

def to_upset_row(u: Upset) -> Dict[str, Any]:
    """Convert Upset model to database row format."""
    return u.model_dump()

def to_injury_row(i: Injury) -> Dict[str, Any]:
    """Convert Injury model to database row format."""
    return i.model_dump()

def to_team_record_row(t: TeamRecord) -> Dict[str, Any]:
    """Convert TeamRecord model to database row format."""
    return t.model_dump()

def to_game_row(g: Game) -> Dict[str, Any]:
    """Convert Game model to database row format."""
    return g.model_dump()

def to_player_stats_row(p: PlayerStats) -> Dict[str, Any]:
    """Convert PlayerStats model to database row format."""
    return p.model_dump()

def to_team_analysis_row(t: TeamAnalysis) -> Dict[str, Any]:
    """Convert TeamAnalysis model to database row format."""
    return t.model_dump()

# Enhanced upset detection
def detect_upset(
    league: str,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    point_spread: Optional[float] = None,
    odds_before_game: Optional[float] = None,
    league_config: Optional[Dict[str, Any]] = None,
    additional_context: Optional[Dict[str, Any]] = None
) -> Optional[Upset]:
    """
    Enhanced upset detection with additional context and league-specific logic.
    
    Args:
        league: The sports league
        home_team: Home team name
        away_team: Away team name
        home_score: Home team final score
        away_score: Away team final score
        point_spread: Point spread (positive = home favored, negative = away favored)
        odds_before_game: Pre-game odds for the winner
        league_config: League-specific upset thresholds
        additional_context: Additional game context (weather, venue, etc.)
        
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
    
    # Check performance upset (close games that shouldn't have been close)
    if league_config:
        score_diff = abs(winner_score - loser_score)
        if score_diff <= 3:  # Close game
            upset_detected = True
            if upset_type is None:
                upset_type = "performance"
            upset_reason = f"Close game with {score_diff} point differential"
            upset_magnitude = max(upset_magnitude, 5.0 - score_diff)
    
    # Check historical upset (based on team performance trends)
    if additional_context and additional_context.get('historical_context'):
        # This could include recent form, head-to-head records, etc.
        upset_detected = True
        if upset_type is None:
            upset_type = "historical"
        upset_reason = "Historical context suggests this was an upset"
        upset_magnitude = max(upset_magnitude, 3.0)
    
    if not upset_detected:
        return None
    
    # Build additional context for the upset
    upset_data = {
        "league": league,
        "game_date": datetime.now().strftime("%Y-%m-%d"),
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "winner": winner,
        "loser": loser,
        "upset_type": upset_type,
        "upset_reason": upset_reason,
        "upset_magnitude": upset_magnitude
    }
    
    # Add optional context
    if point_spread is not None:
        upset_data["point_spread"] = point_spread
    if odds_before_game is not None:
        upset_data["odds_before_game"] = odds_before_game
    if additional_context:
        upset_data.update({
            "game_quarter": additional_context.get("game_quarter"),
            "time_remaining": additional_context.get("time_remaining"),
            "weather_conditions": additional_context.get("weather"),
            "attendance": additional_context.get("attendance"),
            "tv_network": additional_context.get("tv_network")
        })
    
    return Upset(**upset_data)
