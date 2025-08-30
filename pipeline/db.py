from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List, Dict, Any

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS players (
    id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    league TEXT NOT NULL,
    team TEXT,
    team_id TEXT,
    position TEXT,
    jersey TEXT,
    nationality TEXT,
    birthdate TEXT,
    height_cm REAL,
    weight_kg REAL,
    active INTEGER,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS upsets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT NOT NULL,
    game_date TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    winner TEXT NOT NULL,
    loser TEXT NOT NULL,
    upset_type TEXT NOT NULL,
    upset_reason TEXT,
    point_spread REAL,
    odds_before_game REAL,
    upset_magnitude REAL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_upsets_league ON upsets(league);
CREATE INDEX IF NOT EXISTS idx_upsets_date ON upsets(game_date);
CREATE INDEX IF NOT EXISTS idx_upsets_winner ON upsets(winner);
"""

def get_engine(db_url: str) -> Engine:
    """Create and configure database engine with schema."""
    eng = create_engine(db_url, future=True)
    with eng.begin() as con:
        con.exec_driver_sql(SCHEMA_SQL)
    return eng

def upsert_players(engine: Engine, rows: List[Dict[str, Any]]) -> None:
    """Insert or replace player records."""
    if not rows:
        return
    
    cols = [
        "id", "full_name", "first_name", "last_name", "league", "team", "team_id",
        "position", "jersey", "nationality", "birthdate", "height_cm", "weight_kg",
        "active", "updated_at"
    ]
    placeholders = ",".join([f":{c}" for c in cols])
    sql = text(f"""
        INSERT OR REPLACE INTO players ({",".join(cols)})
        VALUES ({placeholders})
    """)
    
    with engine.begin() as con:
        con.execute(sql, rows)

def insert_upset(engine: Engine, upset_data: Dict[str, Any]) -> None:
    """Insert a new upset record."""
    cols = [
        "league", "game_date", "home_team", "away_team", "home_score", "away_score",
        "winner", "loser", "upset_type", "upset_reason", "point_spread", 
        "odds_before_game", "upset_magnitude", "created_at"
    ]
    placeholders = ",".join([f":{c}" for c in cols])
    sql = text(f"""
        INSERT INTO upsets ({",".join(cols)})
        VALUES ({placeholders})
    """)
    
    with engine.begin() as con:
        con.execute(sql, upset_data)

def get_recent_upsets(engine: Engine, league: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent upsets, optionally filtered by league."""
    sql = """
        SELECT * FROM upsets 
        WHERE 1=1
    """
    params = {}
    
    if league:
        sql += " AND league = :league"
        params['league'] = league
    
    sql += " ORDER BY game_date DESC, created_at DESC LIMIT :limit"
    params['limit'] = limit
    
    with engine.connect() as con:
        result = con.execute(text(sql), params)
        return [dict(row._mapping) for row in result]

def get_upset_stats(engine: Engine, league: str = None) -> Dict[str, Any]:
    """Get upset statistics, optionally filtered by league."""
    where_clause = "WHERE 1=1"
    params = {}
    
    if league:
        where_clause += " AND league = :league"
        params['league'] = league
    
    sql = f"""
        SELECT 
            COUNT(*) as total_upsets,
            COUNT(DISTINCT winner) as unique_upset_teams,
            AVG(upset_magnitude) as avg_magnitude,
            MAX(upset_magnitude) as max_magnitude,
            COUNT(CASE WHEN upset_type = 'point_spread' THEN 1 END) as spread_upsets,
            COUNT(CASE WHEN upset_type = 'odds' THEN 1 END) as odds_upsets
        FROM upsets 
        {where_clause}
    """
    
    with engine.connect() as con:
        result = con.execute(text(sql), params)
        row = result.fetchone()
        return dict(row._mapping) if row else {}
