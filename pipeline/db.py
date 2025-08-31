from sqlalchemy import create_engine, text, Index
from sqlalchemy.engine import Engine
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
-- Enhanced Players table with more comprehensive data
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
    active INTEGER DEFAULT 1,
    rookie_year INTEGER,
    experience_years INTEGER,
    college TEXT,
    draft_round INTEGER,
    draft_pick INTEGER,
    updated_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Enhanced Upsets table with more context
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
    game_quarter TEXT,
    time_remaining TEXT,
    weather_conditions TEXT,
    attendance INTEGER,
    tv_network TEXT,
    created_at TEXT NOT NULL
);

-- New table for tracking injuries
CREATE TABLE IF NOT EXISTS injuries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id TEXT NOT NULL,
    league TEXT NOT NULL,
    team TEXT NOT NULL,
    injury_type TEXT,
    body_part TEXT,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    injury_date TEXT NOT NULL,
    expected_return TEXT,
    games_missed INTEGER DEFAULT 0,
    notes TEXT,
    source TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (player_id) REFERENCES players(id)
);

-- New table for team records and statistics
CREATE TABLE IF NOT EXISTS team_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team TEXT NOT NULL,
    league TEXT NOT NULL,
    season INTEGER NOT NULL,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    ties INTEGER DEFAULT 0,
    win_percentage REAL,
    points_for INTEGER DEFAULT 0,
    points_against INTEGER DEFAULT 0,
    point_differential INTEGER DEFAULT 0,
    home_wins INTEGER DEFAULT 0,
    home_losses INTEGER DEFAULT 0,
    away_wins INTEGER DEFAULT 0,
    away_losses INTEGER DEFAULT 0,
    division_wins INTEGER DEFAULT 0,
    division_losses INTEGER DEFAULT 0,
    conference_wins INTEGER DEFAULT 0,
    conference_losses INTEGER DEFAULT 0,
    playoff_appearance BOOLEAN DEFAULT FALSE,
    playoff_seed INTEGER,
    playoff_wins INTEGER DEFAULT 0,
    playoff_losses INTEGER DEFAULT 0,
    championships INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(team, league, season)
);

-- New table for game results and statistics
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT NOT NULL,
    season INTEGER NOT NULL,
    game_date TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    home_team_stats TEXT,  -- JSON string for detailed stats
    away_team_stats TEXT,  -- JSON string for detailed stats
    venue TEXT,
    attendance INTEGER,
    weather TEXT,
    tv_network TEXT,
    game_status TEXT DEFAULT 'scheduled',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- New table for player statistics by game/season
CREATE TABLE IF NOT EXISTS player_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id TEXT NOT NULL,
    league TEXT NOT NULL,
    season INTEGER NOT NULL,
    game_id INTEGER,
    team TEXT NOT NULL,
    opponent TEXT,
    game_date TEXT,
    stats_data TEXT NOT NULL,  -- JSON string for detailed stats
    created_at TEXT NOT NULL,
    FOREIGN KEY (player_id) REFERENCES players(id),
    FOREIGN KEY (game_id) REFERENCES games(id)
);

-- New table for team strengths and weaknesses analysis
CREATE TABLE IF NOT EXISTS team_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team TEXT NOT NULL,
    league TEXT NOT NULL,
    season INTEGER NOT NULL,
    analysis_type TEXT NOT NULL,  -- 'strength', 'weakness', 'trend'
    category TEXT NOT NULL,       -- 'offense', 'defense', 'special_teams', etc.
    description TEXT NOT NULL,
    confidence_score REAL,        -- 0.0 to 1.0
    supporting_data TEXT,         -- JSON string with evidence
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_players_league_team ON players(league, team);
CREATE INDEX IF NOT EXISTS idx_players_active ON players(active);
CREATE INDEX IF NOT EXISTS idx_upsets_league_date ON upsets(league, game_date);
CREATE INDEX IF NOT EXISTS idx_upsets_winner ON upsets(winner);
CREATE INDEX IF NOT EXISTS idx_injuries_player_status ON injuries(player_id, status);
CREATE INDEX IF NOT EXISTS idx_injuries_team_date ON injuries(team, injury_date);
CREATE INDEX IF NOT EXISTS idx_team_records_season ON team_records(season);
CREATE INDEX IF NOT EXISTS idx_games_league_season ON games(league, season);
CREATE INDEX IF NOT EXISTS idx_player_stats_player_season ON player_stats(player_id, season);
"""

def get_engine(db_url: str) -> Engine:
    """Create and configure database engine with enhanced schema."""
    try:
        eng = create_engine(db_url, future=True)

        # Parse SQL statements properly, separating comments from SQL
        raw_statements = SCHEMA_SQL.split(';')
        statements = []
        for raw_stmt in raw_statements:
            # Split by newlines and filter out comment lines
            lines = raw_stmt.strip().split('\n')
            sql_lines = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('--'):
                    # Remove inline comments (everything after --)
                    if '--' in line:
                        line = line.split('--')[0].strip()
                    if line:
                        sql_lines.append(line)

            if sql_lines:
                # Reconstruct the SQL statement
                sql_statement = ' '.join(sql_lines)
                if sql_statement:
                    statements.append(sql_statement)

        logger.info(f"Total SQL statements found: {len(statements)}")

        # Separate CREATE TABLE and CREATE INDEX statements
        table_statements = []
        index_statements = []
        
        for statement in statements:
            if statement.upper().startswith('CREATE TABLE'):
                table_statements.append(statement)
            elif statement.upper().startswith('CREATE INDEX'):
                index_statements.append(statement)

        logger.info(f"Table statements: {len(table_statements)}, Index statements: {len(index_statements)}")

        # Create tables first with explicit commits
        with eng.begin() as con:
            table_count = 0
            for i, statement in enumerate(table_statements):
                logger.info(f"Creating table {i+1}/{len(table_statements)}: {statement[:100]}...")
                try:
                    con.exec_driver_sql(statement)
                    table_count += 1
                    table_name = statement.split('(')[0].split()[-1]
                    logger.info(f"Created table #{table_count}: {table_name}")
                except Exception as e:
                    logger.error(f"Table creation failed: {e}")
                    logger.error(f"Failed statement: {statement}")
                    raise

        logger.info(f"Successfully created {table_count} tables")

        # Force engine disposal to ensure tables are committed
        eng.dispose()
        
        # Wait a moment for file system to sync
        import time
        time.sleep(0.1)

        # Now create indexes with a fresh connection
        with eng.begin() as con:
            index_count = 0
            for i, statement in enumerate(index_statements):
                logger.info(f"Creating index {i+1}/{len(index_statements)}: {statement[:100]}...")
                try:
                    con.exec_driver_sql(statement)
                    index_count += 1
                    index_name = statement.split('ON')[0].split()[-1]
                    logger.info(f"Created index #{index_count}: {index_name}")
                except Exception as e:
                    logger.error(f"Index creation failed: {e}")
                    logger.error(f"Failed statement: {statement}")
                    # Log warning but don't fail - indexes are optional
                    logger.warning(f"Index creation failed, continuing: {e}")

        logger.info(f"Successfully created {index_count} indexes")
        logger.info("Database schema initialized successfully")
        return eng
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {e}")
        raise

def upsert_players(engine: Engine, rows: List[Dict[str, Any]]) -> None:
    """Insert or replace player records with enhanced data."""
    if not rows:
        return
    
    cols = [
        "id", "full_name", "first_name", "last_name", "league", "team", "team_id",
        "position", "jersey", "nationality", "birthdate", "height_cm", "weight_kg",
        "active", "rookie_year", "experience_years", "college", "draft_round", 
        "draft_pick", "updated_at", "created_at"
    ]
    
    # Filter rows to only include valid columns
    filtered_rows = []
    for row in rows:
        filtered_row = {k: v for k, v in row.items() if k in cols}
        filtered_rows.append(filtered_row)
    
    placeholders = ",".join([f":{c}" for c in cols])
    sql = text(f"""
        INSERT OR REPLACE INTO players ({",".join(cols)})
        VALUES ({placeholders})
    """)
    
    try:
        with engine.begin() as con:
            con.execute(sql, filtered_rows)
        logger.info(f"Upserted {len(filtered_rows)} players")
    except Exception as e:
        logger.error(f"Failed to upsert players: {e}")
        raise

def insert_upset(engine: Engine, upset_data: Dict[str, Any]) -> None:
    """Insert a new upset record with enhanced data."""
    cols = [
        "league", "game_date", "home_team", "away_team", "home_score", "away_score",
        "winner", "loser", "upset_type", "upset_reason", "point_spread", 
        "odds_before_game", "upset_magnitude", "game_quarter", "time_remaining",
        "weather_conditions", "attendance", "tv_network", "created_at"
    ]
    
    filtered_data = {k: v for k, v in upset_data.items() if k in cols}
    placeholders = ",".join([f":{c}" for c in filtered_data.keys()])
    sql = text(f"""
        INSERT INTO upsets ({",".join(filtered_data.keys())})
        VALUES ({placeholders})
    """)
    
    try:
        with engine.begin() as con:
            con.execute(sql, filtered_data)
        logger.info("Upset record inserted successfully")
    except Exception as e:
        logger.error(f"Failed to insert upset: {e}")
        raise

def insert_injury(engine: Engine, injury_data: Dict[str, Any]) -> None:
    """Insert a new injury record."""
    cols = [
        "player_id", "league", "team", "injury_type", "body_part", "severity",
        "status", "injury_date", "expected_return", "games_missed", "notes",
        "source", "created_at", "updated_at"
    ]
    
    filtered_data = {k: v for k, v in injury_data.items() if k in cols}
    placeholders = ",".join([f":{c}" for c in filtered_data.keys()])
    sql = text(f"""
        INSERT INTO injuries ({",".join(filtered_data.keys())})
        VALUES ({placeholders})
    """)
    
    try:
        with engine.begin() as con:
            con.execute(sql, filtered_data)
        logger.info("Injury record inserted successfully")
    except Exception as e:
        logger.error(f"Failed to insert injury: {e}")
        raise

def upsert_team_record(engine: Engine, record_data: Dict[str, Any]) -> None:
    """Insert or update team record."""
    cols = [
        "team", "league", "season", "wins", "losses", "ties", "win_percentage",
        "points_for", "points_against", "point_differential", "home_wins",
        "home_losses", "away_wins", "away_losses", "division_wins",
        "division_losses", "conference_wins", "conference_losses",
        "playoff_appearance", "playoff_seed", "playoff_wins", "playoff_losses",
        "championships", "created_at", "updated_at"
    ]
    
    filtered_data = {k: v for k, v in record_data.items() if k in cols}
    placeholders = ",".join([f":{c}" for c in filtered_data.keys()])
    
    # Use UPSERT syntax for SQLite
    sql = text(f"""
        INSERT INTO team_records ({",".join(filtered_data.keys())})
        VALUES ({placeholders})
        ON CONFLICT(team, league, season) DO UPDATE SET
        updated_at = excluded.updated_at,
        wins = excluded.wins,
        losses = excluded.losses,
        ties = excluded.ties,
        win_percentage = excluded.win_percentage,
        points_for = excluded.points_for,
        points_against = excluded.points_against,
        point_differential = excluded.point_differential
    """)
    
    try:
        with engine.begin() as con:
            con.execute(sql, filtered_data)
        logger.info("Team record upserted successfully")
    except Exception as e:
        logger.error(f"Failed to upsert team record: {e}")
        raise

def get_recent_upsets(engine: Engine, league: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent upsets with enhanced filtering."""
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
    
    try:
        with engine.connect() as con:
            result = con.execute(text(sql), params)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Failed to fetch recent upsets: {e}")
        return []

def get_upset_stats(engine: Engine, league: str = None) -> Dict[str, Any]:
    """Get comprehensive upset statistics."""
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
            COUNT(CASE WHEN upset_type = 'odds' THEN 1 END) as odds_upsets,
            COUNT(CASE WHEN upset_type = 'performance' THEN 1 END) as performance_upsets,
            COUNT(CASE WHEN upset_type = 'historical' THEN 1 END) as historical_upsets
        FROM upsets 
        {where_clause}
    """
    
    try:
        with engine.connect() as con:
            result = con.execute(text(sql), params)
            row = result.fetchone()
            return dict(row._mapping) if row else {}
    except Exception as e:
        logger.error(f"Failed to fetch upset stats: {e}")
        return {}

def get_active_injuries(engine: Engine, league: str = None, team: str = None) -> List[Dict[str, Any]]:
    """Get active injuries with optional filtering."""
    sql = """
        SELECT i.*, p.full_name, p.position 
        FROM injuries i
        JOIN players p ON i.player_id = p.id
        WHERE i.status IN ('questionable', 'doubtful', 'out', 'ir')
    """
    params = {}
    
    if league:
        sql += " AND i.league = :league"
        params['league'] = league
    
    if team:
        sql += " AND i.team = :team"
        params['team'] = team
    
    sql += " ORDER BY i.injury_date DESC"
    
    try:
        with engine.connect() as con:
            result = con.execute(text(sql), params)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Failed to fetch active injuries: {e}")
        return []

def get_team_records(engine: Engine, league: str, season: int = None) -> List[Dict[str, Any]]:
    """Get team records for a specific league and season."""
    sql = """
        SELECT * FROM team_records 
        WHERE league = :league
    """
    params = {'league': league}
    
    if season:
        sql += " AND season = :season"
        params['season'] = season
    
    sql += " ORDER BY win_percentage DESC, wins DESC"
    
    try:
        with engine.connect() as con:
            result = con.execute(text(sql), params)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Failed to fetch team records: {e}")
        return []
