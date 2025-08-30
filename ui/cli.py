#!/usr/bin/env python3
"""
Enhanced CLI interface for the Sports Data Pipeline.
Provides comprehensive access to all pipeline features with better organization.
"""

import click
import sys
from pathlib import Path
from typing import Optional, List
import logging

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.db import (
    get_engine, get_recent_upsets, get_upset_stats, 
    get_active_injuries, get_team_records
)
from pipeline.utils import (
    export_csv, export_upsets_csv, export_json, export_excel,
    format_upset_summary, clean_team_name, validate_player_data
)
from pipeline.normalize import detect_upset
from pipeline.providers import thesportsdb

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.group()
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config, verbose):
    """🏈🏀⚾🏒 Sports Data Pipeline - Comprehensive Sports Data Analysis"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['verbose'] = verbose
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

@cli.group()
def players():
    """Manage player data across all leagues."""
    pass

@players.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Specific league to fetch')
@click.option('--season', '-s', type=int, help='Season year (null for current)')
@click.option('--team', '-t', help='Filter by specific team')
@click.option('--position', '-p', help='Filter by position')
@click.option('--active-only', is_flag=True, help='Show only active players')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'excel']), 
              default='csv', help='Export format')
@click.option('--output', '-o', help='Output directory')
@click.pass_context
def fetch(ctx, league, season, team, position, active_only, format, output):
    """Fetch player data from configured sources."""
    click.echo(f"🔍 Fetching players for {league or 'all leagues'}...")
    
    # Implementation will be added here
    click.echo("Player fetch functionality coming soon!")

@players.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.option('--team', '-t', help='Filter by team')
@click.option('--position', '-p', help='Filter by position')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'excel']), 
              default='csv', help='Export format')
@click.option('--output', '-o', help='Output directory')
@click.pass_context
def export(ctx, league, team, position, format, output):
    """Export player data in various formats."""
    click.echo(f"📤 Exporting players in {format.upper()} format...")
    
    # Implementation will be added here
    click.echo("Player export functionality coming soon!")

@cli.group()
def upsets():
    """Manage and analyze upset data."""
    pass

@upsets.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.option('--limit', '-n', type=int, default=10, help='Number of upsets to show')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.pass_context
def list(ctx, league, limit, format):
    """List recent upsets with filtering options."""
    try:
        engine = get_engine("sqlite:///sports_data.db")
        upsets = get_recent_upsets(engine, league, limit)
        
        if not upsets:
            click.echo("No upsets found.")
            return
        
        if format == 'table':
            click.echo(f"\n📊 Recent Upsets ({len(upsets)} found):")
            click.echo("=" * 80)
            for upset in upsets:
                click.echo(f"  {format_upset_summary(upset)}")
        elif format == 'json':
            import json
            click.echo(json.dumps(upsets, indent=2))
        elif format == 'csv':
            # Export to CSV
            output_file = export_upsets_csv(upsets, "exports", league)
            click.echo(f"Exported to: {output_file}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)

@upsets.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.pass_context
def stats(ctx, league):
    """Show upset statistics and analysis."""
    try:
        engine = get_engine("sqlite:///sports_data.db")
        stats = get_upset_stats(engine, league)
        
        if not stats:
            click.echo("No upset statistics available.")
            return
        
        click.echo(f"\n📈 Upset Statistics {'(' + league.upper() + ')' if league else '(All Leagues)'}:")
        click.echo("=" * 50)
        click.echo(f"Total Upsets: {stats.get('total_upsets', 0)}")
        click.echo(f"Unique Upset Teams: {stats.get('unique_upset_teams', 0)}")
        click.echo(f"Average Magnitude: {stats.get('avg_magnitude', 0):.2f}")
        click.echo(f"Max Magnitude: {stats.get('max_magnitude', 0):.2f}")
        click.echo(f"Spread Upsets: {stats.get('spread_upsets', 0)}")
        click.echo(f"Odds Upsets: {stats.get('odds_upsets', 0)}")
        click.echo(f"Performance Upsets: {stats.get('performance_upsets', 0)}")
        click.echo(f"Historical Upsets: {stats.get('historical_upsets', 0)}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)

@cli.group()
def injuries():
    """Manage and track player injuries."""
    pass

@injuries.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.option('--team', '-t', help='Filter by team')
@click.option('--severity', '-s', type=click.Choice(['questionable', 'doubtful', 'out', 'ir']), 
              help='Filter by injury severity')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.pass_context
def list(ctx, league, team, severity, format):
    """List active injuries with filtering options."""
    try:
        engine = get_engine("sqlite:///sports_data.db")
        injuries = get_active_injuries(engine, league, team)
        
        if not injuries:
            click.echo("No active injuries found.")
            return
        
        if format == 'table':
            click.echo(f"\n🏥 Active Injuries ({len(injuries)} found):")
            click.echo("=" * 80)
            for injury in injuries:
                player_name = injury.get('full_name', 'Unknown')
                team_name = injury.get('team', 'Unknown')
                status = injury.get('status', 'Unknown')
                injury_type = injury.get('injury_type', 'Unknown')
                click.echo(f"  {player_name} ({team_name}) - {status}: {injury_type}")
        elif format == 'json':
            import json
            click.echo(json.dumps(injuries, indent=2))
        elif format == 'csv':
            # Export to CSV
            output_file = export_csv(injuries, "exports", f"{league or 'all'}_injuries")
            click.echo(f"Exported to: {output_file}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)

@cli.group()
def teams():
    """Manage and analyze team data."""
    pass

@teams.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              required=True, help='League to analyze')
@click.option('--season', '-s', type=int, help='Season year (null for current)')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.pass_context
def records(ctx, league, season, format):
    """Show team records and standings."""
    try:
        engine = get_engine("sqlite:///sports_data.db")
        records = get_team_records(engine, league, season)
        
        if not records:
            click.echo(f"No team records found for {league.upper()} {season or 'current season'}.")
            return
        
        if format == 'table':
            click.echo(f"\n🏆 {league.upper()} Team Records {season or '(Current Season)'}:")
            click.echo("=" * 80)
            click.echo(f"{'Team':<20} {'W':<3} {'L':<3} {'T':<3} {'PCT':<6} {'PF':<4} {'PA':<4} {'DIFF':<6}")
            click.echo("-" * 80)
            for record in records:
                team = record.get('team', 'Unknown')
                wins = record.get('wins', 0)
                losses = record.get('losses', 0)
                ties = record.get('ties', 0)
                pct = record.get('win_percentage', 0.0)
                pf = record.get('points_for', 0)
                pa = record.get('points_against', 0)
                diff = record.get('point_differential', 0)
                click.echo(f"{team:<20} {wins:<3} {losses:<3} {ties:<3} {pct:<6.3f} {pf:<4} {pa:<4} {diff:<6}")
        elif format == 'json':
            import json
            click.echo(json.dumps(records, indent=2))
        elif format == 'csv':
            # Export to CSV
            output_file = export_csv(records, "exports", f"{league}_team_records")
            click.echo(f"Exported to: {output_file}")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)

@cli.group()
def data():
    """Data management and exploration commands."""
    pass

@data.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'excel']), 
              default='csv', help='Export format')
@click.option('--output', '-o', help='Output directory')
@click.pass_context
def export_all(ctx, league, format, output):
    """Export all data for a league or all leagues."""
    click.echo(f"📤 Exporting all data in {format.upper()} format...")
    
    # Implementation will be added here
    click.echo("Data export functionality coming soon!")

@data.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.pass_context
def validate(ctx, league):
    """Validate data quality and integrity."""
    click.echo(f"🔍 Validating data for {league or 'all leagues'}...")
    
    # Implementation will be added here
    click.echo("Data validation functionality coming soon!")

@cli.group()
def analysis():
    """Data analysis and insights commands."""
    pass

@analysis.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              required=True, help='League to analyze')
@click.option('--season', '-s', type=int, help='Season year')
@click.option('--team', '-t', help='Specific team to analyze')
@click.pass_context
def trends(ctx, league, season, team):
    """Analyze trends and patterns in the data."""
    click.echo(f"📊 Analyzing trends for {league.upper()} {season or 'current season'}...")
    
    # Implementation will be added here
    click.echo("Trend analysis functionality coming soon!")

@analysis.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              required=True, help='League to analyze')
@click.option('--season', '-s', type=int, help='Season year')
@click.pass_context
def predictions(ctx, league, season):
    """Generate predictions based on historical data."""
    click.echo(f"🔮 Generating predictions for {league.upper()} {season or 'current season'}...")
    
    # Implementation will be added here
    click.echo("Prediction functionality coming soon!")

@cli.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), 
              help='Filter by league')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
@click.pass_context
def status(ctx, league, format):
    """Show overall pipeline status and data summary."""
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        click.echo(f"\n📊 Pipeline Status {'(' + league.upper() + ')' if league else '(All Leagues)'}:")
        click.echo("=" * 60)
        
        # Get upset stats
        upset_stats = get_upset_stats(engine, league)
        if upset_stats:
            click.echo(f"Total Upsets: {upset_stats.get('total_upsets', 0)}")
            click.echo(f"Recent Upsets: {upset_stats.get('recent_upsets', 0)}")
        
        # Get injury count
        injuries = get_active_injuries(engine, league)
        click.echo(f"Active Injuries: {len(injuries)}")
        
        # Get team count (simplified)
        if league:
            team_records = get_team_records(engine, league)
            click.echo(f"Teams Tracked: {len(team_records)}")
        
        click.echo("\n✅ Pipeline is running normally!")
    
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)

@cli.command()
def version():
    """Show pipeline version and information."""
    click.echo("🏈🏀⚾🏒 Sports Data Pipeline v2.0.0")
    click.echo("Enhanced sports data analysis with comprehensive tracking")
    click.echo("Includes: Players, Upsets, Injuries, Team Records, Analysis")

@cli.command()
def help():
    """Show detailed help information."""
    click.echo("🏈🏀⚾🏒 Sports Data Pipeline - Help")
    click.echo("=" * 50)
    click.echo("\nAvailable Commands:")
    click.echo("  players     - Manage player data")
    click.echo("  upsets      - Track and analyze upsets")
    click.echo("  injuries    - Monitor player injuries")
    click.echo("  teams       - Team records and analysis")
    click.echo("  data        - Data management and export")
    click.echo("  analysis    - Data analysis and insights")
    click.echo("  status      - Pipeline status overview")
    click.echo("  version     - Show version information")
    click.echo("\nUse 'sports-pipeline <command> --help' for detailed help on each command.")

if __name__ == '__main__':
    cli()
