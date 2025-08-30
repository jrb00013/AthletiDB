#!/usr/bin/env python3
"""
Enhanced Sports Data Pipeline - Main Runner
Comprehensive sports data analysis with multiple sources, rate limiting, and advanced features.
"""

import os
import sys
import yaml
import click
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import logging
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sports_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import pipeline components
from pipeline.db import (
    get_engine, upsert_players, insert_upset, insert_injury, 
    upsert_team_record, get_recent_upsets, get_upset_stats,
    get_active_injuries, get_team_records
)
from pipeline.utils import (
    export_csv, export_upsets_csv, export_json, export_excel,
    ensure_dir, format_upset_summary, clean_team_name
)
from pipeline.normalize import detect_upset
from pipeline.providers import thesportsdb

# Import legacy providers for backward compatibility
try:
    from pipeline.providers import nba_balldontlie, nfl_sleeper, mlb_statsapi, nhl_api
    LEGACY_PROVIDERS_AVAILABLE = True
except ImportError:
    LEGACY_PROVIDERS_AVAILABLE = False
    logger.warning("Legacy providers not available")

# Dynamic import map for providers
PROVIDERS = {
    "nfl": {
        "primary": "nflverse",  # Git submodule
        "live": "thesportsdb",   # Live API
        "legacy": "nfl_sleeper" if LEGACY_PROVIDERS_AVAILABLE else None
    },
    "nba": {
        "primary": "thesportsdb",
        "legacy": "nba_balldontlie" if LEGACY_PROVIDERS_AVAILABLE else None
    },
    "mlb": {
        "primary": "thesportsdb", 
        "legacy": "mlb_statsapi" if LEGACY_PROVIDERS_AVAILABLE else None
    },
    "nhl": {
        "primary": "thesportsdb",
        "legacy": "nhl_api" if LEGACY_PROVIDERS_AVAILABLE else None
    }
}

def import_provider(provider_name: str):
    """Dynamically import a provider module."""
    if provider_name == "thesportsdb":
        return thesportsdb
    elif provider_name == "nflverse":
        # Handle nflverse submodule data
        return None  # Will be implemented separately
    elif LEGACY_PROVIDERS_AVAILABLE:
        import importlib
        return importlib.import_module(f"pipeline.providers.{provider_name}")
    else:
        return None

def validate_player_data(row: Dict[str, Any]) -> List[str]:
    """Basic validation for player data."""
    errors = []
    if not row.get('name'):
        errors.append("Missing player name")
    if not row.get('team'):
        errors.append("Missing player team")
    if not row.get('position'):
        errors.append("Missing player position")
    if not row.get('age'):
        errors.append("Missing player age")
    if not row.get('height'):
        errors.append("Missing player height")
    if not row.get('weight'):
        errors.append("Missing player weight")
    if not row.get('birth_date'):
        errors.append("Missing player birth date")
    if not row.get('nationality'):
        errors.append("Missing player nationality")
    return errors

def fetch_players(engine, league: str, season: int = None, source: str = "primary") -> tuple[int, str]:
    """Fetch players for a specific league from specified source."""
    provider_config = PROVIDERS.get(league)
    if not provider_config:
        return 0, f"No provider configuration found for league: {league}"
    
    provider_name = provider_config.get(source)
    if not provider_name:
        return 0, f"No {source} provider available for {league}"
    
    try:
        if provider_name == "thesportsdb":
            provider = thesportsdb
            logger.info(f"[fetch] {league.upper()} from TheSportsDB season={season}")
            rows = provider.fetch_players(league, season)
        elif provider_name == "nflverse":
            # Handle nflverse submodule data
            logger.info(f"[fetch] {league.upper()} from nflverse submodule")
            rows = fetch_nflverse_players(league, season)
        else:
            # Legacy providers
            provider = import_provider(provider_name)
            if not provider:
                return 0, f"Failed to import provider: {provider_name}"
            logger.info(f"[fetch] {league.upper()} from {provider_name} season={season}")
            rows = provider.fetch(season=season)
        
        if rows:
            # Validate and clean data
            valid_rows = []
            for row in rows:
                errors = validate_player_data(row)
                if not errors:
                    # Clean team names
                    if 'team' in row and row['team']:
                        row['team'] = clean_team_name(row['team'])
                    valid_rows.append(row)
                else:
                    logger.warning(f"Player validation errors: {errors}")
            
            if valid_rows:
                upsert_players(engine, valid_rows)
                csv_path = export_csv(valid_rows, "exports", league)
                return len(valid_rows), f"Saved {len(valid_rows)} players to {csv_path}"
            else:
                return 0, f"No valid players found for {league}"
        else:
            return 0, f"No players found for {league}"
            
    except Exception as e:
        logger.error(f"Error fetching {league} from {source}: {e}")
        return 0, f"Error fetching {league}: {e}"

def fetch_nflverse_players(league: str, season: int = None) -> List[Dict[str, Any]]:
    """Fetch NFL players from nflverse git submodule."""
    try:
        # This would read from the git submodule data
        # Implementation depends on the specific data format
        logger.info("nflverse player fetching not yet implemented")
        return []
    except Exception as e:
        logger.error(f"Error fetching from nflverse: {e}")
        return []

def fetch_upsets(engine, league: str = None) -> tuple[int, str]:
    """Fetch and detect upsets for recent games."""
    total_upsets = 0
    upsets_found = []
    
    leagues_to_check = [league] if league else PROVIDERS.keys()
    
    for lg in leagues_to_check:
        try:
            # Try TheSportsDB first for live data
            if "thesportsdb" in PROVIDERS[lg].values():
                try:
                    games = thesportsdb.fetch_games(lg, datetime.now().year)
                    logger.info(f"[upsets] {lg.upper()}: checking {len(games)} games from TheSportsDB")
                    
                    for game in games:
                        if not all(game.get(k) for k in ['home_team', 'away_team', 'home_score', 'away_score']):
                            continue
                        
                        # Detect upsets using enhanced logic
                        upset = detect_upset(
                            league=lg.upper(),
                            home_team=game['home_team'],
                            away_team=game['away_team'],
                            home_score=game['home_score'],
                            away_score=game['away_score'],
                            additional_context={
                                'venue': game.get('venue'),
                                'game_status': game.get('game_status')
                            }
                        )
                        
                        if upset:
                            upset_data = upset.model_dump()
                            insert_upset(engine, upset_data)
                            upsets_found.append(upset_data)
                            total_upsets += 1
                    
                except Exception as e:
                    logger.warning(f"TheSportsDB upset detection failed for {lg}: {e}")
            
            # Fall back to legacy providers if available
            if LEGACY_PROVIDERS_AVAILABLE:
                legacy_provider = PROVIDERS[lg].get("legacy")
                if legacy_provider:
                    provider = import_provider(legacy_provider)
                    if hasattr(provider, 'fetch_recent_games'):
                        games = provider.fetch_recent_games(limit=20)
                        logger.info(f"[upsets] {lg.upper()}: checked {len(games)} games from legacy provider")
                        
                        for game in games:
                            if not all(game.get(k) for k in ['home_team', 'away_team', 'home_score', 'away_score']):
                                continue
                            
                            upset = detect_upset(
                                league=lg.upper(),
                                home_team=game['home_team'],
                                away_team=game['away_team'],
                                home_score=game['home_score'],
                                away_score=game['away_score']
                            )
                            
                            if upset:
                                upset_data = upset.model_dump()
                                insert_upset(engine, upset_data)
                                upsets_found.append(upset_data)
                                total_upsets += 1
                
        except Exception as e:
            logger.error(f"Upset detection for {lg}: {e}")
            continue
    
    if upsets_found:
        csv_path = export_upsets_csv(upsets_found, "exports", league)
        return total_upsets, f"Detected {total_upsets} upsets, saved to {csv_path}"
    else:
        return 0, "No new upsets detected"

def fetch_injuries(engine, league: str = None) -> tuple[int, str]:
    """Fetch injury data for specified league(s)."""
    total_injuries = 0
    injuries_found = []
    
    leagues_to_check = [league] if league else PROVIDERS.keys()
    
    for lg in leagues_to_check:
        try:
            # Try TheSportsDB for injury data
            if "thesportsdb" in PROVIDERS[lg].values():
                injuries = thesportsdb.fetch_injuries(lg)
                if injuries:
                    for injury in injuries:
                        try:
                            insert_injury(engine, injury)
                            injuries_found.append(injury)
                            total_injuries += 1
                        except Exception as e:
                            logger.warning(f"Failed to insert injury: {e}")
                            continue
            
            logger.info(f"[injuries] {lg.upper()}: processed {len(injuries) if 'injuries' in locals() else 0} injuries")
            
        except Exception as e:
            logger.error(f"Injury fetching for {lg}: {e}")
            continue
    
    if injuries_found:
        csv_path = export_csv(injuries_found, "exports", f"{league or 'all'}_injuries")
        return total_injuries, f"Processed {total_injuries} injuries, saved to {csv_path}"
    else:
        return 0, "No new injuries processed"

def show_upset_stats(engine, league: str = None):
    """Display comprehensive upset statistics."""
    stats = get_upset_stats(engine, league)
    recent = get_recent_upsets(engine, league, limit=5)
    
    click.echo(f"\n=== Upset Statistics {'(' + league.upper() + ')' if league else '(All Leagues)'} ===")
    
    if stats:
        click.echo(f"Total Upsets: {stats.get('total_upsets', 0)}")
        click.echo(f"Unique Upset Teams: {stats.get('unique_upset_teams', 0)}")
        click.echo(f"Average Magnitude: {stats.get('avg_magnitude', 0):.2f}")
        click.echo(f"Max Magnitude: {stats.get('max_magnitude', 0):.2f}")
        click.echo(f"Spread Upsets: {stats.get('spread_upsets', 0)}")
        click.echo(f"Odds Upsets: {stats.get('odds_upsets', 0)}")
        click.echo(f"Performance Upsets: {stats.get('performance_upsets', 0)}")
        click.echo(f"Historical Upsets: {stats.get('historical_upsets', 0)}")
    
    if recent:
        click.echo(f"\nRecent Upsets:")
        for upset in recent:
            click.echo(f"  {format_upset_summary(upset)}")
    else:
        click.echo("\nNo upsets recorded yet.")

def show_pipeline_status(engine, league: str = None):
    """Show comprehensive pipeline status."""
    click.echo(f"\n📊 Pipeline Status {'(' + league.upper() + ')' if league else '(All Leagues)'}:")
    click.echo("=" * 60)
    
    # Upset stats
    upset_stats = get_upset_stats(engine, league)
    if upset_stats:
        click.echo(f"Total Upsets: {upset_stats.get('total_upsets', 0)}")
    
    # Injury count
    injuries = get_active_injuries(engine, league)
    click.echo(f"Active Injuries: {len(injuries)}")
    
    # Team records
    if league:
        team_records = get_team_records(engine, league)
        click.echo(f"Teams Tracked: {len(team_records)}")
    
    # Rate limit status
    try:
        rate_status = thesportsdb.get_rate_limit_status()
        click.echo(f"TheSportsDB API: {rate_status['current_requests']}/{rate_status['requests_per_hour']} requests")
    except Exception as e:
        click.echo(f"TheSportsDB API: Status unavailable ({e})")
    
    click.echo("\n✅ Pipeline is running normally!")

@click.command()
@click.option('--league', '-l', help='Specific league to process (nfl, nba, mlb, nhl)')
@click.option('--source', '-s', type=click.Choice(['primary', 'live', 'legacy']), 
              default='primary', help='Data source to use')
@click.option('--upsets-only', is_flag=True, help='Only process upsets')
@click.option('--injuries-only', is_flag=True, help='Only process injuries')
@click.option('--include-upsets', is_flag=True, help='Include upset detection when fetching players')
@click.option('--include-injuries', is_flag=True, help='Include injury tracking when fetching players')
@click.option('--show-stats', is_flag=True, help='Show upset statistics')
@click.option('--show-status', is_flag=True, help='Show pipeline status')
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(league, source, upsets_only, injuries_only, include_upsets, 
         include_injuries, show_stats, show_status, config, verbose):
    """Enhanced Sports Data Pipeline - Comprehensive sports data analysis."""
    
    # Configure logging
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    # Load configuration
    load_dotenv()
    
    if not Path(config).exists():
        click.echo(f"Configuration file {config} not found!")
        sys.exit(1)
    
    try:
        cfg = yaml.safe_load(Path(config).read_text())
    except Exception as e:
        click.echo(f"Error loading configuration: {e}")
        sys.exit(1)
    
    # Extract configuration
    db_url = cfg.get("database_url", "sqlite:///sports_data.db")
    csv_dir = cfg.get("csv_dir", "exports")
    cache_dir = cfg.get("cache_dir", "cache")
    leagues = cfg.get("leagues", [])
    seasons = cfg.get("seasons", {})
    upsets_config = cfg.get("upsets", {})
    injuries_config = cfg.get("injuries", {})
    
    # Initialize database and directories
    try:
        engine = get_engine(db_url)
        ensure_dir(csv_dir)
        ensure_dir(cache_dir)
    except Exception as e:
        click.echo(f"Failed to initialize database: {e}")
        sys.exit(1)
    
    # Show status if requested
    if show_status:
        show_pipeline_status(engine, league)
        return
    
    # Process injuries only
    if injuries_only:
        logger.info("[mode] Injury tracking only")
        count, message = fetch_injuries(engine, league)
        click.echo(f"[result] {message}")
        return
    
    # Process upsets only
    if upsets_only:
        logger.info("[mode] Upset detection only")
        count, message = fetch_upsets(engine, league)
        click.echo(f"[result] {message}")
        
        if show_stats:
            show_upset_stats(engine, league)
        return
    
    # Process players
    if league:
        if league not in leagues:
            click.echo(f"League {league} not configured in {config}")
            sys.exit(1)
        leagues_to_process = [league]
    else:
        leagues_to_process = leagues
    
    total_players = 0
    for lg in leagues_to_process:
        count, message = fetch_players(engine, lg, seasons.get(lg), source)
        click.echo(f"[result] {message}")
        total_players += count
    
    click.echo(f"[summary] Total players processed: {total_players}")
    
    # Process upsets if requested
    if include_upsets and upsets_config.get("enabled", True):
        logger.info("\n[upsets] Processing upsets...")
        count, message = fetch_upsets(engine, league)
        click.echo(f"[result] {message}")
    
    # Process injuries if requested
    if include_injuries and injuries_config.get("enabled", True):
        logger.info("\n[injuries] Processing injuries...")
        count, message = fetch_injuries(engine, league)
        click.echo(f"[result] {message}")
    
    # Show statistics if requested
    if show_stats:
        show_upset_stats(engine, league)
    
    click.echo("[done] Enhanced pipeline completed successfully!")

if __name__ == "__main__":
    main()
