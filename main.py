#!/usr/bin/env python3
"""
Players Pipeline - Fetch and normalize player data from major sports leagues
Includes upset tracking and detection capabilities.
"""

import os
import sys
import yaml
import click
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from pipeline.db import get_engine, upsert_players, insert_upset, get_recent_upsets, get_upset_stats
from pipeline.utils import export_csv, export_upsets_csv, ensure_dir, format_upset_summary
from pipeline.normalize import detect_upset
from pipeline import providers as _  # noqa: F401

# Dynamic import map for providers
PROVIDERS = {
    "nba": "pipeline.providers.nba_balldontlie",
    "nfl": "pipeline.providers.nfl_sleeper", 
    "mlb": "pipeline.providers.mlb_statsapi",
    "nhl": "pipeline.providers.nhl_api",
}

def import_provider(modname: str):
    """Dynamically import a provider module."""
    import importlib
    return importlib.import_module(modname)

def fetch_players(engine, league: str, season: int = None) -> tuple[int, str]:
    """Fetch players for a specific league."""
    modname = PROVIDERS.get(league)
    if not modname:
        return 0, f"No provider found for league: {league}"
    
    try:
        provider = import_provider(modname)
        print(f"[fetch] {league.upper()} season={season}")
        
        rows = provider.fetch(season=season)
        if rows:
            upsert_players(engine, rows)
            csv_path = export_csv(rows, "exports", league)
            return len(rows), f"Saved {len(rows)} players to {csv_path}"
        else:
            return 0, f"No players found for {league}"
            
    except Exception as e:
        return 0, f"Error fetching {league}: {e}"

def fetch_upsets(engine, league: str = None) -> tuple[int, str]:
    """Fetch and detect upsets for recent games."""
    total_upsets = 0
    upsets_found = []
    
    leagues_to_check = [league] if league else PROVIDERS.keys()
    
    for lg in leagues_to_check:
        try:
            provider = import_provider(PROVIDERS[lg])
            
            # Check if provider has game fetching capability
            if hasattr(provider, 'fetch_recent_games'):
                games = provider.fetch_recent_games(limit=20)
                
                for game in games:
                    if not all(game.get(k) for k in ['home_team', 'away_team', 'home_score', 'away_score']):
                        continue
                    
                    # Detect upsets using league-specific config
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
                
                print(f"[upsets] {lg.upper()}: checked {len(games)} games")
                
        except Exception as e:
            print(f"[error] Upset detection for {lg}: {e}")
            continue
    
    if upsets_found:
        csv_path = export_upsets_csv(upsets_found, "exports", league)
        return total_upsets, f"Detected {total_upsets} upsets, saved to {csv_path}"
    else:
        return 0, "No new upsets detected"

def show_upset_stats(engine, league: str = None):
    """Display upset statistics."""
    stats = get_upset_stats(engine, league)
    recent = get_recent_upsets(engine, league, limit=5)
    
    print(f"\n=== Upset Statistics {'(' + league.upper() + ')' if league else '(All Leagues)'} ===")
    
    if stats:
        print(f"Total Upsets: {stats.get('total_upsets', 0)}")
        print(f"Unique Upset Teams: {stats.get('unique_upset_teams', 0)}")
        print(f"Average Magnitude: {stats.get('avg_magnitude', 0):.2f}")
        print(f"Max Magnitude: {stats.get('max_magnitude', 0):.2f}")
        print(f"Spread Upsets: {stats.get('spread_upsets', 0)}")
        print(f"Odds Upsets: {stats.get('odds_upsets', 0)}")
    
    if recent:
        print(f"\nRecent Upsets:")
        for upset in recent:
            print(f"  {format_upset_summary(upset)}")
    else:
        print("\nNo upsets recorded yet.")

@click.command()
@click.option('--league', '-l', help='Specific league to process (nba, nfl, mlb, nhl)')
@click.option('--upsets-only', is_flag=True, help='Only fetch and analyze upsets')
@click.option('--include-upsets', is_flag=True, help='Include upset detection when fetching players')
@click.option('--show-stats', is_flag=True, help='Show upset statistics')
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
def main(league, upsets_only, include_upsets, show_stats, config):
    """Players Pipeline - Fetch sports data and track upsets."""
    
    # Load configuration
    load_dotenv()
    
    if not Path(config).exists():
        click.echo(f"Configuration file {config} not found!")
        sys.exit(1)
    
    cfg = yaml.safe_load(Path(config).read_text())
    db_url = cfg.get("database_url", "sqlite:///players.db")
    csv_dir = cfg.get("csv_dir", "exports")
    leagues = cfg.get("leagues", [])
    seasons = cfg.get("seasons", {})
    upsets_config = cfg.get("upsets", {})
    
    # Initialize database and directories
    engine = get_engine(db_url)
    ensure_dir(csv_dir)
    
    if upsets_only:
        # Only process upsets
        print("[mode] Upset detection only")
        count, message = fetch_upsets(engine, league)
        print(f"[result] {message}")
        
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
        count, message = fetch_players(engine, lg, seasons.get(lg))
        print(f"[result] {message}")
        total_players += count
    
    print(f"[summary] Total players processed: {total_players}")
    
    # Process upsets if requested
    if include_upsets and upsets_config.get("enabled", True):
        print("\n[upsets] Processing upsets...")
        count, message = fetch_upsets(engine, league)
        print(f"[result] {message}")
    
    # Show statistics if requested
    if show_stats:
        show_upset_stats(engine, league)
    
    print("[done] Pipeline completed successfully!")

if __name__ == "__main__":
    main()
