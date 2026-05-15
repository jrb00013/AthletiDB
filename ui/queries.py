"""
UI Database Query Functions
Optimized queries for the web and TUI interfaces
"""

from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from pipeline.db import get_engine
from datetime import datetime, timedelta


def get_league_overview(engine: Engine, league: str) -> Dict[str, Any]:
    """Get a complete overview of a league for the UI."""
    try:
        with engine.connect() as con:
            team_count = con.execute(text(
                "SELECT COUNT(DISTINCT team) as cnt FROM team_records WHERE league = :league"
            ), {"league": league}).fetchone()
            
            upset_count = con.execute(text(
                "SELECT COUNT(*) as cnt FROM upsets WHERE league = :league"
            ), {"league": league}).fetchone()
            
            injury_count = con.execute(text(
                "SELECT COUNT(*) as cnt FROM injuries WHERE status IN ('out', 'doubtful', 'questionable', 'ir') AND league = :league"
            ), {"league": league}).fetchone()
            
            return {
                "total_teams": team_count[0] if team_count else 0,
                "total_upsets": upset_count[0] if upset_count else 0,
                "active_injuries": injury_count[0] if injury_count else 0
            }
    except Exception as e:
        return {"total_teams": 0, "total_upsets": 0, "active_injuries": 0}


def get_hot_teams(engine: Engine, league: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Get teams on winning streaks (hot teams)."""
    try:
        with engine.connect() as con:
            result = con.execute(text("""
                SELECT team, wins, losses, win_percentage
                FROM team_records 
                WHERE league = :league
                ORDER BY win_percentage DESC, wins DESC
                LIMIT :limit
            """), {"league": league, "limit": limit})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_struggling_teams(engine: Engine, league: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Get teams on losing streaks (struggling teams)."""
    try:
        with engine.connect() as con:
            result = con.execute(text("""
                SELECT team, wins, losses, win_percentage
                FROM team_records 
                WHERE league = :league
                ORDER BY win_percentage ASC, losses DESC
                LIMIT :limit
            """), {"league": league, "limit": limit})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_biggest_upset(engine: Engine, league: str = None) -> Optional[Dict[str, Any]]:
    """Get the biggest upset in recent memory."""
    try:
        with engine.connect() as con:
            if league:
                result = con.execute(text("""
                    SELECT * FROM upsets 
                    WHERE league = :league
                    ORDER BY upset_magnitude DESC
                    LIMIT 1
                """), {"league": league})
            else:
                result = con.execute(text("""
                    SELECT * FROM upsets 
                    ORDER BY upset_magnitude DESC
                    LIMIT 1
                """))
            row = result.fetchone()
            return dict(row._mapping) if row else None
    except:
        return None


def get_players_by_team(engine: Engine, league: str, team: str) -> List[Dict[str, Any]]:
    """Get all players for a specific team."""
    try:
        with engine.connect() as con:
            result = con.execute(text("""
                SELECT full_name, position, jersey, nationality, active
                FROM players 
                WHERE league = :league AND team = :team
                ORDER BY position, full_name
            """), {"league": league, "team": team})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_teams_with_most_injuries(engine: Engine, league: str = None) -> List[Dict[str, Any]]:
    """Get teams with the most injured players."""
    try:
        with engine.connect() as con:
            if league:
                result = con.execute(text("""
                    SELECT team, COUNT(*) as injury_count
                    FROM injuries 
                    WHERE status IN ('out', 'doubtful', 'questionable', 'ir') AND league = :league
                    GROUP BY team
                    ORDER BY injury_count DESC
                    LIMIT 10
                """), {"league": league})
            else:
                result = con.execute(text("""
                    SELECT team, league, COUNT(*) as injury_count
                    FROM injuries 
                    WHERE status IN ('out', 'doubtful', 'questionable', 'ir')
                    GROUP BY team, league
                    ORDER BY injury_count DESC
                    LIMIT 10
                """))
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_league_leaders(engine: Engine, league: str, stat: str = "wins") -> List[Dict[str, Any]]:
    """Get league leaders in various categories."""
    valid_stats = ["wins", "win_percentage", "points_for", "point_differential"]
    if stat not in valid_stats:
        stat = "win_percentage"
    
    try:
        with engine.connect() as con:
            result = con.execute(text(f"""
                SELECT team, wins, losses, win_percentage, points_for, points_against, point_differential
                FROM team_records 
                WHERE league = :league
                ORDER BY {stat} DESC
                LIMIT 10
            """), {"league": league})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_recent_games_summary(engine: Engine, league: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get summary of recent games."""
    try:
        with engine.connect() as con:
            result = con.execute(text("""
                SELECT league, home_team, away_team, home_score, away_score, game_date
                FROM games
                WHERE league = :league AND home_score IS NOT NULL
                ORDER BY game_date DESC
                LIMIT :limit
            """), {"league": league, "limit": limit})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_conversation_insights(engine: Engine, league: str = None) -> List[Dict[str, Any]]:
    """Get insights formatted for casual conversation."""
    insights = []
    
    try:
        with engine.connect() as con:
            leagues = [league] if league else ['nfl', 'nba', 'mlb', 'nhl']
            
            for lg in leagues:
                biggest = get_biggest_upset(engine, lg)
                if biggest:
                    insights.append({
                        "type": "upset",
                        "league": lg.upper(),
                        "message": f"{biggest['winner']} beating {biggest['loser']} was a huge upset!",
                        "say_this": f"Did you see {biggest['winner']} upset {biggest['loser']}? Nobody saw that coming!"
                    })
                
                hot = get_hot_teams(engine, lg, 1)
                if hot:
                    insights.append({
                        "type": "hot_team",
                        "league": lg.upper(),
                        "message": f"{hot[0]['team']} is on fire right now!",
                        "say_this": f"{hot[0]['team']} has been playing incredible lately!"
                    })
                
                struggling = get_struggling_teams(engine, lg, 1)
                if struggling:
                    insights.append({
                        "type": "struggling",
                        "league": lg.upper(),
                        "message": f"{struggling[0]['team']} is really struggling this season",
                        "say_this": f"It's tough to watch {struggling[0]['team']} this season, they can't catch a break"
                    })
    except:
        pass
    
    return insights[:10]


def get_upset_summary_text(upset: Dict[str, Any]) -> str:
    """Format an upset for conversation."""
    winner = upset.get('winner', 'Unknown')
    loser = upset.get('loser', 'Unknown')
    score = f"{upset.get('home_score', 0)}-{upset.get('away_score', 0)}"
    upset_type = upset.get('upset_type', 'unknown')
    
    if upset_type == 'point_spread':
        return f"{winner} upset {loser}! They were supposed to lose but pulled off the win!"
    elif upset_type == 'odds':
        return f"Huge upset! {winner} had huge odds against them but still beat {loser}!"
    elif upset_type == 'performance':
        return f"Shocker! {winner} beat {loser} in a close game {score}."
    else:
        return f"{winner} surprised everyone by beating {loser} {score}."


def get_injury_summary_text(injury: Dict[str, Any]) -> str:
    """Format an injury for conversation."""
    player = injury.get('full_name', 'Unknown')
    team = injury.get('team', 'Unknown')
    status = injury.get('status', 'unknown')
    injury_type = injury.get('injury_type', 'Unknown')
    
    if status in ['out', 'ir']:
        return f"Oh man, {player} is out for {team}. That's a huge loss!"
    elif status == 'doubtful':
        return f"{player} from {team} is doubtful - might not play this week."
    else:
        return f"{player} ({team}) is {status} - we'll have to wait and see."


def get_team_injuries(engine: Engine, team: str, league: str = None) -> List[Dict[str, Any]]:
    """Get all injuries for a specific team."""
    try:
        with engine.connect() as con:
            if league:
                result = con.execute(text("""
                    SELECT i.*, p.full_name, p.position
                    FROM injuries i
                    JOIN players p ON i.player_id = p.id
                    WHERE i.team = :team AND i.league = :league
                    AND i.status IN ('out', 'doubtful', 'questionable', 'ir')
                    ORDER BY i.injury_date DESC
                """), {"team": team, "league": league})
            else:
                result = con.execute(text("""
                    SELECT i.*, p.full_name, p.position
                    FROM injuries i
                    JOIN players p ON i.player_id = p.id
                    WHERE i.team = :team
                    AND i.status IN ('out', 'doubtful', 'questionable', 'ir')
                    ORDER BY i.injury_date DESC
                """), {"team": team})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_upset_by_team(engine: Engine, team: str) -> List[Dict[str, Any]]:
    """Get all upsets involving a specific team."""
    try:
        with engine.connect() as con:
            result = con.execute(text("""
                SELECT * FROM upsets 
                WHERE winner = :team OR loser = :team
                ORDER BY game_date DESC
                LIMIT 20
            """), {"team": team})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_most_upset_teams(engine: Engine, league: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """Get teams with the most upset victories."""
    try:
        with engine.connect() as con:
            if league:
                result = con.execute(text("""
                    SELECT winner as team, COUNT(*) as upset_count
                    FROM upsets
                    WHERE league = :league
                    GROUP BY winner
                    ORDER BY upset_count DESC
                    LIMIT :limit
                """), {"league": league, "limit": limit})
            else:
                result = con.execute(text("""
                    SELECT winner as team, league, COUNT(*) as upset_count
                    FROM upsets
                    GROUP BY winner, league
                    ORDER BY upset_count DESC
                    LIMIT :limit
                """), {"limit": limit})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_recent_games_for_team(engine: Engine, team: str, league: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent games for a specific team."""
    try:
        with engine.connect() as con:
            if league:
                result = con.execute(text("""
                    SELECT * FROM games
                    WHERE (home_team = :team OR away_team = :team)
                    AND league = :league
                    ORDER BY game_date DESC
                    LIMIT :limit
                """), {"team": team, "league": league, "limit": limit})
            else:
                result = con.execute(text("""
                    SELECT * FROM games
                    WHERE home_team = :team OR away_team = :team
                    ORDER BY game_date DESC
                    LIMIT :limit
                """), {"team": team, "limit": limit})
            return [dict(row._mapping) for row in result]
    except:
        return []


def get_league_stat_leaders(engine: Engine, league: str, category: str = "win_pct") -> Dict[str, Any]:
    """Get stat leaders for a league."""
    valid_categories = {
        "win_pct": "win_percentage",
        "wins": "wins", 
        "points_for": "points_for",
        "point_differential": "point_differential"
    }
    
    col = valid_categories.get(category, "win_percentage")
    
    try:
        with engine.connect() as con:
            result = con.execute(text(f"""
                SELECT team, wins, losses, win_percentage, points_for, points_against, point_differential
                FROM team_records
                WHERE league = :league
                ORDER BY {col} DESC
                LIMIT 5
            """), {"league": league})
            
            leaders = [dict(row._mapping) for row in result]
            
            if not leaders:
                return {"category": category, "leaders": []}
            
            top = leaders[0]
            
            return {
                "category": category,
                "leaders": leaders,
                "leader": {
                    "team": top.get('team'),
                    "value": top.get(col, 0),
                    "say_this": f"{top.get('team')} leads the {league.upper()} in {category.replace('_', ' ')} with {top.get(col, 0)}!"
                }
            }
    except Exception as e:
        return {"category": category, "leaders": [], "error": str(e)}