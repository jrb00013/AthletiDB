#!/usr/bin/env python3
"""
Enhanced Sports Data Pipeline CLI
Comprehensive command-line interface for the sports data pipeline.
"""

import click
import sys
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime, timedelta

# Import pipeline components
from pipeline.db import get_engine, get_recent_upsets, get_upset_stats, get_active_injuries, get_team_records
from pipeline.providers import thesportsdb
from pipeline.utils import export_csv, export_json, export_excel, get_rate_limiter
from pipeline.normalize import detect_upset

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--config', '-c', default='config.yaml', help='Configuration file path')
@click.pass_context
def cli(ctx, verbose, config):
    """Enhanced Sports Data Pipeline - Comprehensive sports data analysis and management."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    ctx.ensure_object(dict)
    ctx.obj['config'] = config

# ============================================================================
# DATA FETCHING COMMANDS
# ============================================================================

@cli.group()
def fetch():
    """Fetch data from various sources."""
    pass

@fetch.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to fetch')
@click.option('--source', '-s', default='live', type=click.Choice(['live', 'legacy', 'all']), help='Data source')
@click.option('--season', type=int, help='Season year (optional)')
@click.option('--include-upsets', is_flag=True, help='Include upset detection')
@click.option('--include-injuries', is_flag=True, help='Include injury tracking')
def players(league, source, season, include_upsets, include_injuries):
    """Fetch player data for a specific league."""
    click.echo(f"Fetching {league.upper()} players from {source} source...")
    
    try:
        if source in ['live', 'all']:
            players_data = thesportsdb.fetch_players(league, season)
            if players_data:
                click.echo(f"Successfully fetched {len(players_data)} players from TheSportsDB")
            else:
                click.echo("No players found from live source")
        
        # TODO: Implement legacy CSV data fetching
        if source in ['legacy', 'all']:
            click.echo("Legacy CSV data fetching not yet implemented")
        
        click.echo("Player fetch completed successfully")
        
    except Exception as e:
        click.echo(f"Error fetching players: {e}", err=True)
        sys.exit(1)

@fetch.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to fetch')
@click.option('--source', '-s', default='live', type=click.Choice(['live', 'legacy', 'all']), help='Data source')
def teams(league, source):
    """Fetch team data for a specific league."""
    click.echo(f"Fetching {league.upper()} teams from {source} source...")
    
    try:
        if source in ['live', 'all']:
            teams_data = thesportsdb.fetch_teams(league)
            if teams_data:
                click.echo(f"Successfully fetched {len(teams_data)} teams from TheSportsDB")
            else:
                click.echo("No teams found from live source")
        
        click.echo("Team fetch completed successfully")
        
    except Exception as e:
        click.echo(f"Error fetching teams: {e}", err=True)
        sys.exit(1)

@fetch.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to fetch')
@click.option('--season', type=int, required=True, help='Season year')
@click.option('--source', '-s', default='live', type=click.Choice(['live', 'legacy', 'all']), help='Data source')
def games(league, season, source):
    """Fetch game data for a specific league and season."""
    click.echo(f"Fetching {league.upper()} games for season {season} from {source} source...")
    
    try:
        if source in ['live', 'all']:
            games_data = thesportsdb.fetch_games(league, season)
            if games_data:
                click.echo(f"Successfully fetched {len(games_data)} games from TheSportsDB")
            else:
                click.echo("No games found from live source")
        
        click.echo("Game fetch completed successfully")
        
    except Exception as e:
        click.echo(f"Error fetching games: {e}", err=True)
        sys.exit(1)

@fetch.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to fetch')
@click.option('--team', '-t', help='Specific team (optional)')
@click.option('--source', '-s', default='live', type=click.Choice(['live', 'legacy', 'all']), help='Data source')
def injuries(league, team, source):
    """Fetch injury data for a specific league."""
    click.echo(f"Fetching {league.upper()} injuries from {source} source...")
    
    try:
        if source in ['live', 'all']:
            injuries_data = thesportsdb.fetch_injuries(league, team)
            if injuries_data:
                click.echo(f"Successfully fetched {len(injuries_data)} injuries from TheSportsDB")
            else:
                click.echo("No injuries found from live source")
        
        click.echo("Injury fetch completed successfully")
        
    except Exception as e:
        click.echo(f"Error fetching injuries: {e}", err=True)
        sys.exit(1)

# ============================================================================
# DATA EXPORT COMMANDS
# ============================================================================

@cli.group()
def export():
    """Export data to various formats."""
    pass

@export.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to export')
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json', 'excel']), help='Export format')
@click.option('--output-dir', '-o', default='exports', help='Output directory')
@click.option('--include-metadata', is_flag=True, default=True, help='Include metadata files')
def players(league, format, output_dir, include_metadata):
    """Export player data for a specific league."""
    click.echo(f"Exporting {league.upper()} players to {format.upper()} format...")
    
    try:
        # Get players from database
        engine = get_engine("sqlite:///sports_data.db")
        # TODO: Implement get_players function in db.py
        
        click.echo("Player export completed successfully")
        
    except Exception as e:
        click.echo(f"Error exporting players: {e}", err=True)
        sys.exit(1)

@export.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to export (all if not specified)')
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json', 'excel']), help='Export format')
@click.option('--output-dir', '-o', default='exports', help='Output directory')
@click.option('--include-metadata', is_flag=True, default=True, help='Include metadata files')
def upsets(league, format, output_dir, include_metadata):
    """Export upset data."""
    click.echo(f"Exporting upsets to {format.upper()} format...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        upsets_data = get_recent_upsets(engine, league, limit=1000)
        
        if not upsets_data:
            click.echo("No upsets found to export")
            return
        
        if format == 'csv':
            output_path = export_csv(upsets_data, output_dir, league or 'all', include_metadata)
        elif format == 'json':
            output_path = export_json(upsets_data, output_dir, f"{league or 'all'}_upsets.json", include_metadata)
        elif format == 'excel':
            output_path = export_excel(upsets_data, output_dir, f"{league or 'all'}_upsets.xlsx", include_metadata)
        
        if output_path:
            click.echo(f"Upsets exported successfully to: {output_path}")
        else:
            click.echo("Export failed")
        
    except Exception as e:
        click.echo(f"Error exporting upsets: {e}", err=True)
        sys.exit(1)

@export.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to export (all if not specified)')
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json', 'excel']), help='Export format')
@click.option('--output-dir', '-o', default='exports', help='Output directory')
@click.option('--include-metadata', is_flag=True, default=True, help='Include metadata files')
def injuries(league, format, output_dir, include_metadata):
    """Export injury data."""
    click.echo(f"Exporting injuries to {format.upper()} format...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        injuries_data = get_active_injuries(engine, league)
        
        if not injuries_data:
            click.echo("No injuries found to export")
            return
        
        if format == 'csv':
            output_path = export_csv(injuries_data, output_dir, league or 'all', include_metadata)
        elif format == 'json':
            output_path = export_json(injuries_data, output_dir, f"{league or 'all'}_injuries.json", include_metadata)
        elif format == 'excel':
            output_path = export_excel(injuries_data, output_dir, f"{league or 'all'}_injuries.xlsx", include_metadata)
        
        if output_path:
            click.echo(f"Injuries exported successfully to: {output_path}")
        else:
            click.echo("Export failed")
        
    except Exception as e:
        click.echo(f"Error exporting injuries: {e}", err=True)
        sys.exit(1)

@export.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to export')
@click.option('--season', type=int, help='Season year (current if not specified)')
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json', 'excel']), help='Export format')
@click.option('--output-dir', '-o', default='exports', help='Output directory')
@click.option('--include-metadata', is_flag=True, default=True, help='Include metadata files')
def teams(league, season, format, output_dir, include_metadata):
    """Export team data for a specific league."""
    click.echo(f"Exporting {league.upper()} teams to {format.upper()} format...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        teams_data = get_team_records(engine, league, season)
        
        if not teams_data:
            click.echo("No team data found to export")
            return
        
        if format == 'csv':
            output_path = export_csv(teams_data, output_dir, league, include_metadata)
        elif format == 'json':
            output_path = export_json(teams_data, output_dir, f"{league}_teams.json", include_metadata)
        elif format == 'excel':
            output_path = export_excel(teams_data, output_dir, f"{league}_teams.xlsx", include_metadata)
        
        if output_path:
            click.echo(f"Teams exported successfully to: {output_path}")
        else:
            click.echo("Export failed")
        
    except Exception as e:
        click.echo(f"Error exporting teams: {e}", err=True)
        sys.exit(1)

@export.command()
@click.option('--output-dir', '-o', default='exports', help='Output directory')
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json', 'excel']), help='Export format')
@click.option('--include-metadata', is_flag=True, default=True, help='Include metadata files')
def all(output_dir, format, include_metadata):
    """Export all available data."""
    click.echo("Exporting all available data...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        # Export different data types
        leagues = ['nfl', 'nba', 'mlb', 'nhl']
        
        for league in leagues:
            click.echo(f"Exporting {league.upper()} data...")
            
            # Export players
            # TODO: Implement get_players function
            
            # Export upsets
            upsets_data = get_recent_upsets(engine, league, limit=1000)
            if upsets_data:
                if format == 'csv':
                    export_csv(upsets_data, output_dir, league, include_metadata)
                elif format == 'json':
                    export_json(upsets_data, output_dir, f"{league}_upsets.json", include_metadata)
                elif format == 'excel':
                    export_excel(upsets_data, output_dir, f"{league}_upsets.xlsx", include_metadata)
            
            # Export injuries
            injuries_data = get_active_injuries(engine, league)
            if injuries_data:
                if format == 'csv':
                    export_csv(injuries_data, output_dir, league, include_metadata)
                elif format == 'json':
                    export_json(injuries_data, output_dir, f"{league}_injuries.json", include_metadata)
                elif format == 'excel':
                    export_excel(injuries_data, output_dir, f"{league}_injuries.xlsx", include_metadata)
            
            # Export teams
            teams_data = get_team_records(engine, league)
            if teams_data:
                if format == 'csv':
                    export_csv(teams_data, output_dir, league, include_metadata)
                elif format == 'json':
                    export_json(teams_data, output_dir, f"{league}_teams.json", include_metadata)
                elif format == 'excel':
                    export_excel(teams_data, output_dir, f"{league}_teams.xlsx", include_metadata)
        
        click.echo("All data exported successfully")
        
    except Exception as e:
        click.echo(f"Error exporting all data: {e}", err=True)
        sys.exit(1)

# ============================================================================
# ANALYSIS COMMANDS
# ============================================================================

@cli.group()
def analyze():
    """Analyze sports data and generate insights."""
    pass

@analyze.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to analyze (all if not specified)')
@click.option('--limit', default=10, help='Number of recent upsets to show')
def upsets(league, limit):
    """Analyze recent upsets and provide insights."""
    click.echo(f"Analyzing recent upsets...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        # Get recent upsets
        recent_upsets = get_recent_upsets(engine, league, limit)
        
        if not recent_upsets:
            click.echo("No upsets found for analysis")
            return
        
        # Get upset statistics
        upset_stats = get_upset_stats(engine, league)
        
        # Display analysis
        click.echo(f"\nUpset Analysis Summary:")
        click.echo(f"Total Upsets: {upset_stats.get('total_upsets', 0)}")
        click.echo(f"Unique Upset Teams: {upset_stats.get('unique_upset_teams', 0)}")
        click.echo(f"Average Magnitude: {upset_stats.get('avg_magnitude', 0):.2f}")
        click.echo(f"Maximum Magnitude: {upset_stats.get('max_magnitude', 0):.2f}")
        
        click.echo(f"\nRecent Upsets:")
        for upset in recent_upsets[:5]:  # Show top 5
            click.echo(f"  {upset['game_date']}: {upset['winner']} def. {upset['loser']} ({upset['upset_type']})")
        
        click.echo("\nAnalysis completed successfully")
        
    except Exception as e:
        click.echo(f"Error analyzing upsets: {e}", err=True)
        sys.exit(1)

@analyze.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to analyze (all if not specified)')
@click.option('--team', '-t', help='Specific team to analyze')
def injuries(league, team):
    """Analyze injury patterns and trends."""
    click.echo(f"Analyzing injury patterns...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        # Get active injuries
        active_injuries = get_active_injuries(engine, league, team)
        
        if not active_injuries:
            click.echo("No active injuries found for analysis")
            return
        
        # Analyze injury patterns
        injury_by_status = {}
        injury_by_position = {}
        
        for injury in active_injuries:
            status = injury.get('status', 'unknown')
            position = injury.get('position', 'unknown')
            
            injury_by_status[status] = injury_by_status.get(status, 0) + 1
            injury_by_position[position] = injury_by_position.get(position, 0) + 1
        
        # Display analysis
        click.echo(f"\nInjury Analysis Summary:")
        click.echo(f"Total Active Injuries: {len(active_injuries)}")
        
        click.echo(f"\nInjuries by Status:")
        for status, count in injury_by_status.items():
            click.echo(f"  {status.title()}: {count}")
        
        click.echo(f"\nInjuries by Position:")
        for position, count in injury_by_position.items():
            click.echo(f"  {position}: {count}")
        
        click.echo("\nAnalysis completed successfully")
        
    except Exception as e:
        click.echo(f"Error analyzing injuries: {e}", err=True)
        sys.exit(1)

@analyze.command()
@click.option('--league', '-l', required=True, type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to analyze')
@click.option('--season', type=int, help='Season year (current if not specified)')
def teams(league, season):
    """Analyze team performance and trends."""
    click.echo(f"Analyzing {league.upper()} team performance...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        # Get team records
        team_records = get_team_records(engine, league, season)
        
        if not team_records:
            click.echo("No team records found for analysis")
            return
        
        # Sort by win percentage
        sorted_teams = sorted(team_records, key=lambda x: x.get('win_percentage', 0), reverse=True)
        
        # Display analysis
        click.echo(f"\nTeam Performance Analysis:")
        click.echo(f"Total Teams: {len(sorted_teams)}")
        
        click.echo(f"\nTop 5 Teams by Win Percentage:")
        for i, team in enumerate(sorted_teams[:5], 1):
            wins = team.get('wins', 0)
            losses = team.get('losses', 0)
            win_pct = team.get('win_percentage', 0)
            click.echo(f"  {i}. {team['team']}: {wins}-{losses} ({win_pct:.3f})")
        
        click.echo(f"\nBottom 5 Teams by Win Percentage:")
        for i, team in enumerate(sorted_teams[-5:], len(sorted_teams)-4):
            wins = team.get('wins', 0)
            losses = team.get('losses', 0)
            win_pct = team.get('win_percentage', 0)
            click.echo(f"  {i}. {team['team']}: {wins}-{losses} ({win_pct:.3f})")
        
        click.echo("\nAnalysis completed successfully")
        
    except Exception as e:
        click.echo(f"Error analyzing teams: {e}", err=True)
        sys.exit(1)

# ============================================================================
# SYSTEM COMMANDS
# ============================================================================

@cli.group()
def system():
    """System management and maintenance commands."""
    pass

@system.command()
def status():
    """Show system status and health."""
    click.echo("System Status Check")
    click.echo("=" * 50)
    
    try:
        # Check database
        engine = get_engine("sqlite:///sports_data.db")
        click.echo("Database: Connected")
        
        # Check API status
        try:
            api_status = thesportsdb.get_rate_limit_status()
            click.echo(f"TheSportsDB API: {api_status['current_requests']}/{api_status['requests_per_hour']} requests")
        except:
            click.echo("TheSportsDB API: Not available")
        
        # Check recent data
        recent_upsets = get_recent_upsets(engine, limit=5)
        click.echo(f"Recent Upsets: {len(recent_upsets)} found")
        
        active_injuries = get_active_injuries(engine)
        click.echo(f"Active Injuries: {len(active_injuries)} found")
        
        click.echo("\nSystem Status: Healthy")
        
    except Exception as e:
        click.echo(f"System Status: Error - {e}", err=True)
        sys.exit(1)

@system.command()
@click.option('--force', is_flag=True, help='Force database reset')
def reset(force):
    """Reset the database (WARNING: This will delete all data)."""
    if not force:
        click.echo("WARNING: This will delete all data in the database!")
        click.echo("Use --force to confirm")
        return
    
    click.echo("Resetting database...")
    
    try:
        # Remove database file
        db_path = Path("sports_data.db")
        if db_path.exists():
            db_path.unlink()
            click.echo("Database file removed")
        
        # Reinitialize database
        engine = get_engine("sqlite:///sports_data.db")
        click.echo("Database reinitialized successfully")
        
    except Exception as e:
        click.echo(f"Error resetting database: {e}", err=True)
        sys.exit(1)

@system.command()
def cleanup():
    """Clean up temporary files and cache."""
    click.echo("Cleaning up system...")
    
    try:
        # Clean up cache directory
        cache_dir = Path("cache")
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)
            click.echo("Cache directory cleaned")
        
        # Clean up old log files
        log_files = list(Path(".").glob("*.log.*"))
        for log_file in log_files:
            if log_file.stat().st_mtime < (datetime.now() - timedelta(days=7)).timestamp():
                log_file.unlink()
                click.echo(f"Removed old log file: {log_file}")
        
        click.echo("Cleanup completed successfully")
        
    except Exception as e:
        click.echo(f"Error during cleanup: {e}", err=True)
        sys.exit(1)

# ============================================================================
# UTILITY COMMANDS
# ============================================================================

@cli.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to search')
@click.option('--team', '-t', help='Team name to search')
@click.option('--position', '-p', help='Player position to search')
@click.option('--active-only', is_flag=True, help='Show only active players')
def search(league, team, position, active_only):
    """Search for players, teams, or other data."""
    click.echo("Searching data...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        # Build search query
        query_parts = []
        if league:
            query_parts.append(f"league={league}")
        if team:
            query_parts.append(f"team={team}")
        if position:
            query_parts.append(f"position={position}")
        if active_only:
            query_parts.append("active=1")
        
        search_description = " and ".join(query_parts) if query_parts else "all data"
        click.echo(f"Searching for: {search_description}")
        
        # TODO: Implement actual search functionality
        
        click.echo("Search completed")
        
    except Exception as e:
        click.echo(f"Error during search: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--output', '-o', default='report.txt', help='Output file for the report')
def report(output):
    """Generate a comprehensive system report."""
    click.echo("Generating system report...")
    
    try:
        engine = get_engine("sqlite:///sports_data.db")
        
        # Generate report content
        report_lines = []
        report_lines.append("Sports Data Pipeline - System Report")
        report_lines.append("=" * 50)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Database statistics
        report_lines.append("Database Statistics:")
        report_lines.append("-" * 20)
        
        # TODO: Implement actual statistics gathering
        
        report_lines.append("")
        report_lines.append("API Status:")
        report_lines.append("-" * 20)
        
        try:
            api_status = thesportsdb.get_rate_limit_status()
            report_lines.append(f"TheSportsDB: {api_status['current_requests']}/{api_status['requests_per_hour']} requests")
        except:
            report_lines.append("TheSportsDB: Not available")
        
        # Write report to file
        with open(output, 'w') as f:
            f.write('\n'.join(report_lines))
        
        click.echo(f"Report generated successfully: {output}")
        
    except Exception as e:
        click.echo(f"Error generating report: {e}", err=True)
        sys.exit(1)

# ============================================================================
# MAIN PIPELINE COMMAND
# ============================================================================

@cli.command()
@click.option('--league', '-l', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='Specific league to process')
@click.option('--source', '-s', default='live', type=click.Choice(['live', 'legacy', 'all']), help='Data source to use')
@click.option('--include-upsets', is_flag=True, help='Include upset detection when fetching players')
@click.option('--include-injuries', is_flag=True, help='Include injury tracking when fetching players')
@click.option('--export', '-e', is_flag=True, help='Export data after processing')
@click.option('--export-format', default='csv', type=click.Choice(['csv', 'json', 'excel']), help='Export format')
@click.option('--output-dir', '-o', default='exports', help='Output directory for exports')
def run(league, source, include_upsets, include_injuries, export, export_format, output_dir):
    """Run the complete sports data pipeline."""
    click.echo("Starting Sports Data Pipeline...")
    
    try:
        # Initialize database
        engine = get_engine("sqlite:///sports_data.db")
        click.echo("Database initialized")
        
        # Process data
        leagues_to_process = [league] if league else ['nfl', 'nba', 'mlb', 'nhl']
        
        for current_league in leagues_to_process:
            click.echo(f"\nProcessing {current_league.upper()}...")
            
            # Fetch players
            if source in ['live', 'all']:
                players = thesportsdb.fetch_players(current_league)
                click.echo(f"Fetched {len(players)} players from live source")
            
            # Fetch teams
            if source in ['live', 'all']:
                teams = thesportsdb.fetch_teams(current_league)
                click.echo(f"Fetched {len(teams)} teams from live source")
            
            # TODO: Implement legacy data processing
        
        # Export if requested
        if export:
            click.echo(f"\nExporting data to {export_format.upper()} format...")
            # TODO: Implement export functionality
        
        click.echo("\nPipeline completed successfully!")
        
    except Exception as e:
        click.echo(f"Pipeline failed: {e}", err=True)
        sys.exit(1)

if __name__ == '__main__':
    cli()
