#!/usr/bin/env python3
"""
AthletiDB Enhanced TUI - Rich Terminal Interface
Designed for non-sports-savvy users to get conversation-ready info instantly
"""

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.box import ROUNDED
from rich import box
from rich.align import Align
from rich.markdown import Markdown
from rich.style import Style
from rich.theme import Theme
from datetime import datetime
from typing import Optional
import sys

console = Console()

custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "nfl": "bold #1e3a8a",
    "nba": "bold #ea580c",
    "mlb": "bold #0ea5e9",
    "nhl": "bold #7c3aed"
})

console = Console(theme=custom_theme)

LEAGUE_COLORS = {
    'nfl': '#1e3a8a',
    'nba': '#ea580c', 
    'mlb': '#0ea5e9',
    'nhl': '#7c3aed'
}

LEAGUE_NAMES = {
    'nfl': 'National Football League',
    'nba': 'National Basketball Association',
    'mlb': 'Major League Baseball',
    'nhl': 'National Hockey League'
}

EMOJI_MAP = {
    'nfl': '🏈',
    'nba': '🏀',
    'mlb': '⚾',
    'nhl': '🏒'
}

def print_header():
    console.print()
    console.print(Panel.fit(
        Text("🏆 ATHLETI.DB", justify="center", style="bold cyan"),
        subtitle="[dim]Sports Made Simple | Your Conversation Companion[/dim]",
        border_style="cyan",
        box=ROUNDED
    ))
    console.print()

def show_league_selector():
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("League", style="bold")
    table.add_column("Description")
    table.add_column("Select")
    
    for league, name in LEAGUE_NAMES.items():
        emoji = EMOJI_MAP[league]
        table.add_row(f"{emoji} {league.upper()}", name, f"[bold cyan]--league {league}[/bold cyan]")
    
    console.print(Panel(
        table,
        title="[bold]Select a League[/bold]",
        border_style="cyan"
    ))
    console.print()

def format_standings(standings_data: dict, league: str):
    if not standings_data.get('standings'):
        console.print(f"[yellow]No standings data for {league.upper()}. Run the pipeline first![/yellow]")
        return
    
    table = Table(
        title=f"📊 {league.upper()} Standings",
        show_lines=True,
        box=box.ROUNDED,
        header_style="bold magenta"
    )
    
    table.add_column("#", style="bold", width=3, justify="center")
    table.add_column("Team", style="bold")
    table.add_column("Record", style="cyan")
    table.add_column("Win%", style="green", justify="center")
    table.add_column("Streak", justify="center")
    table.add_column("Points", style="dim", justify="right")
    
    for team in standings_data.get('standings', [])[:15]:
        rank = team.get('rank', 0)
        rank_style = "bold yellow" if rank <= 3 else "dim"
        
        streak = team.get('streak', '')
        streak_style = "bold green" if streak.startswith('W') else "bold red" if streak.startswith('L') else "dim"
        
        table.add_row(
            str(rank),
            team.get('team', 'Unknown'),
            team.get('record', '0-0'),
            team.get('win_pct', '0.000'),
            f"[{streak_style}]{streak}[/{streak_style}]",
            f"{team.get('points_for', 0)}-{team.get('points_against', 0)}"
        )
    
    console.print(table)
    console.print(f"\n[dim]Last updated: {standings_data.get('last_updated', 'Unknown')}[/dim]")

def format_upsets(upsets_data: dict, league: str):
    if not upsets_data.get('upsets'):
        console.print(f"[yellow]No upsets recorded for {league.upper()} yet![/yellow]")
        return
    
    console.print(f"\n[bold yellow]⚡ Recent Upsets ({league.upper()})[/bold yellow]\n")
    
    for i, upset in enumerate(upsets_data.get('upsets', [])[:5], 1):
        winner = upset.get('winner', 'Unknown')
        loser = upset.get('loser', 'Unknown')
        score = upset.get('score', '0-0')
        upset_type = upset.get('upset_type', 'unknown')
        magnitude = upset.get('magnitude', 0)
        
        panel_content = f"""
[bold green]✓ {winner}[/bold green] defeated [bold red]✗ {loser}[/bold red]

Score: [bold cyan]{score}[/bold cyan] | Type: {upset_type.title()} | Magnitude: {magnitude:.1f}

[bold magenta]💬 Say this:[/bold magenta]
[italic]{upset.get('say_this', '')}[/italic]
"""
        
        console.print(Panel(
            panel_content.strip(),
            title=f"#{i} Upset Alert",
            border_style="yellow",
            box=ROUNDED
        ))
        console.print()

def format_injuries(injuries_data: dict, league: str):
    if not injuries_data.get('injuries'):
        console.print(f"[green]✅ No significant injuries in {league.upper()} right now![/green]")
        return
    
    table = Table(
        title=f"🏥 Active Injuries ({league.upper()})",
        show_lines=True,
        box=box.ROUNDED,
        header_style="bold red"
    )
    
    table.add_column("Status", width=8)
    table.add_column("Player", style="bold")
    table.add_column("Pos", width=6)
    table.add_column("Team")
    table.add_column("Injury")
    
    for inj in injuries_data.get('injuries', [])[:10]:
        severity = inj.get('severity', 'unknown')
        
        if severity in ['out', 'ir']:
            status_style = "bold red"
            status_emoji = "🔴"
        elif severity == 'doubtful':
            status_style = "bold yellow"
            status_emoji = "🟡"
        else:
            status_style = "dim"
            status_emoji = "⚪"
        
        injury_text = f"{inj.get('injury', 'Unknown')}"
        if inj.get('body_part'):
            injury_text += f" ({inj.get('body_part')})"
        
        table.add_row(
            f"{status_emoji} {inj.get('status', '').upper()}",
            inj.get('player', 'Unknown'),
            inj.get('position', ''),
            inj.get('team', 'Unknown'),
            injury_text
        )
    
    console.print(table)
    console.print(f"\n[dim]💬 Say this: '[bold]{injuries_data.get('injuries', [{}])[0].get('player', 'The player')}[/bold] is out - huge loss for [bold]{injuries_data.get('injuries', [{}])[0].get('team', 'their team')}[/bold]!'[/dim]")

def format_talking_points(talking_points: dict):
    if not talking_points.get('talking_points'):
        console.print("[yellow]No talking points available. Run the pipeline first![/yellow]")
        return
    
    console.print(f"\n[bold cyan]💬 Today's Talking Points[/bold cyan]\n")
    
    for point in talking_points.get('talking_points', [])[:6]:
        league = point.get('league', 'General')
        topic = point.get('topic', 'No topic')
        say_this = point.get('say_this', '')
        
        league_style = f"bold {LEAGUE_COLORS.get(league.lower(), 'white')}"
        
        console.print(Panel(
            f"[bold]{topic}[/bold]\n\n[green]💬[/green] [italic]{say_this}[/italic]",
            title=f"[{league_style}]{league}[/{league_style}]",
            border_style="cyan",
            box=ROUNDED
        ))
        console.print()

def show_quick_summary(league: str, summary: dict):
    if not summary.get('at_a_glance'):
        console.print(f"[yellow]No summary data for {league.upper()}. Run the pipeline first![/yellow]")
        return
    
    at_glance = summary.get('at_a_glance', {})
    
    stats = summary.get('quick_stats', {})
    
    stats_table = Table(box=None, padding=1)
    stats_table.add_column("Teams", justify="center", style="cyan")
    stats_table.add_column("Upsets", justify="center", style="yellow")
    stats_table.add_column("Injuries", justify="center", style="red")
    
    stats_table.add_row(
        f"[bold]{at_glance.get('total_teams', 0)}[/bold]",
        f"[bold]{stats.get('total_upsets', 0)}[/bold]",
        f"[bold]{stats.get('active_injuries', 0)}[/bold]"
    )
    
    console.print(Panel(
        stats_table,
        title=f"📈 {league.upper()} Quick Stats",
        border_style=LEAGUE_COLORS.get(league, 'cyan'),
        box=ROUNDED
    ))
    
    if at_glance.get('hot_teams'):
        console.print(f"\n[bold green]🔥 Hot Teams:[/bold green] {', '.join(at_glance.get('hot_teams', []))}")
    if at_glance.get('cold_teams'):
        console.print(f"[bold red]❄️ Struggling:[/bold red] {', '.join(at_glance.get('cold_teams', []))}")
    if at_glance.get('biggest_upset') and at_glance.get('biggest_upset') != 'None yet':
        console.print(f"[bold yellow]⚡ Biggest Upset:[/bold yellow] {at_glance.get('biggest_upset')}")

def format_game_results(games_data: dict, league: str):
    if not games_data.get('games'):
        console.print(f"[yellow]No game results for {league.upper()}.[/yellow]")
        return
    
    table = Table(
        title=f"🎮 Recent Game Results ({league.upper()})",
        show_lines=True,
        box=box.ROUNDED
    )
    
    table.add_column("Matchup", style="bold")
    table.add_column("Score", style="cyan", justify="center")
    table.add_column("Result", justify="center")
    
    for game in games_data.get('games', [])[:10]:
        matchup = f"{game.get('home')} vs {game.get('away')}"
        score = f"{game.get('home_score')}-{game.get('away_score')}"
        
        result_type = game.get('result_type', 'normal')
        result_emoji = game.get('emoji', '⚽')
        
        table.add_row(
            matchup,
            f"[bold]{score}[/bold]",
            f"{result_emoji} {result_type.replace('_', ' ').title()}"
        )
    
    console.print(table)

def show_help_panel():
    help_text = """
# AthletiDB Commands

## Quick Commands
- `python -m ui.tui standings --league nfl` - View standings
- `python -m ui.tui upsets --league nfl` - Recent upsets
- `python -m ui.tui injuries --league nfl` - Active injuries
- `python -m ui.tui talking-points` - Conversation starters
- `python -m ui.tui summary --league nfl` - Full summary
- `python -m ui.tui results --league nfl` - Game results
- `python -m ui.tui players --league nfl` - Top players
- `python -m ui.tui team-info nfl "Team"` - Team details
- `python -m ui.tui all-leagues` - Overview all leagues
- `python -m ui.tui welcome` - Show welcome message

## League Options
- `--league nfl` (default) - NFL
- `--league nba` - NBA  
- `--league mlb` - MLB
- `--league nhl` - NHL

## Run Pipeline to Get Data
```bash
python main.py --league nfl --include-upsets --include-injuries
python main.py --league nba --include-upsets --include-injuries
python main.py --league mlb --include-upsets --include-injuries
python main.py --league nhl --include-upsets --include-injuries

# Or for all leagues:
python main.py --include-upsets --include-injuries
```

## Web UI
```bash
python web/main.py
# Then open http://localhost:8000
```
"""
    console.print(Panel(
        Markdown(help_text),
        title="[bold cyan]📚 Help[/bold cyan]",
        border_style="cyan",
        box=ROUNDED
    ))

def show_welcome_message():
    welcome = """
Welcome to [bold cyan]AthletiDB[/bold cyan] - Your Sports Conversation Companion!

[bold]Never be stumped by sports talk again![/bold]

This tool gives you instant, conversation-ready insights into:
• 🏆 Latest standings - Know who's winning
• ⚡ Upsets - Surprising results to talk about
• 🏥 Injuries - Know which stars are out
• 💬 Talking points - What to say in conversations

[bold yellow]Quick Start:[/bold yellow]
1. First, fetch data: [cyan]python main.py --league nfl --include-upsets --include-injuries[/cyan]
2. Then explore: [cyan]python -m ui.tui summary --league nfl[/cyan]
3. Or just browse this help! [cyan]python -m ui.tui --help[/cyan]

[bold]Select a league above to get started![/bold]
"""
    console.print(Panel(
        welcome.strip(),
        title=f"[bold cyan]🏆 Welcome to AthletiDB[/bold cyan]",
        border_style="cyan",
        box=ROUNDED,
        padding=(1, 2)
    ))

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, verbose):
    """AthletiDB - Enhanced Terminal Interface for Sports Fans"""
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
def standings(league):
    """View current standings"""
    print_header()
    from pipeline.db import get_engine, get_team_records
    
    engine = get_engine("sqlite:///sports_data.db")
    records = get_team_records(engine, league)
    
    standings_list = []
    for i, rec in enumerate(records, 1):
        standings_list.append({
            "rank": i,
            "team": rec.get('team', 'Unknown'),
            "record": f"{rec.get('wins', 0)}-{rec.get('losses', 0)}",
            "win_pct": f"{rec.get('win_percentage', 0):.3f}",
            "streak": "W3" if i <= 3 else "L2" if i >= len(records) - 2 else "W1",
            "points_for": rec.get('points_for', 0),
            "points_against": rec.get('points_against', 0)
        })
    
    format_standings({"standings": standings_list, "last_updated": datetime.now().isoformat()}, league)

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
@click.option('--limit', '-n', default=5, type=int, help='Number of upsets to show')
def upsets(league, limit):
    """View recent upsets"""
    print_header()
    from pipeline.db import get_engine, get_recent_upsets
    
    engine = get_engine("sqlite:///sports_data.db")
    upsets_list = get_recent_upsets(engine, league, limit)
    
    converted = []
    for u in upsets_list:
        winner = u.get('winner', '')
        loser = u.get('loser', '')
        converted.append({
            "winner": winner,
            "loser": loser,
            "score": f"{u.get('home_score', 0)}-{u.get('away_score', 0)}",
            "upset_type": u.get('upset_type', 'unknown'),
            "magnitude": u.get('upset_magnitude', 0),
            "say_this": f"{winner} upset {loser}! Nobody saw that coming!"
        })
    
    format_upsets({"upsets": converted}, league)

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
def injuries(league):
    """View active injuries"""
    print_header()
    from pipeline.db import get_engine, get_active_injuries
    
    engine = get_engine("sqlite:///sports_data.db")
    injuries_list = get_active_injuries(engine, league)
    
    severity_map = {'out': 'OUT', 'doubtful': 'DOUBTFUL', 'questionable': 'QUESTIONABLE', 'ir': 'IR'}
    
    converted = []
    for inj in injuries_list:
        converted.append({
            "player": inj.get('full_name', 'Unknown'),
            "position": inj.get('position', ''),
            "team": inj.get('team', 'Unknown'),
            "injury": inj.get('injury_type', 'Unknown'),
            "body_part": inj.get('body_part', ''),
            "status": inj.get('status', 'unknown'),
            "severity": inj.get('severity', 'unknown')
        })
    
    format_injuries({"injuries": converted}, league)

@cli.command()
def talking_points():
    """View conversation starters"""
    print_header()
    from pipeline.db import get_engine, get_recent_upsets, get_active_injuries
    
    engine = get_engine("sqlite:///sports_data.db")
    points = []
    
    for league in ['nfl', 'nba', 'mlb', 'nhl']:
        upsets = get_recent_upsets(engine, league, 2)
        for u in upsets:
            points.append({
                "league": league.upper(),
                "topic": f"Upset: {u.get('winner')} beat {u.get('loser')}",
                "say_this": f"Did you see {u.get('winner')} upset {u.get('loser')}? Insane!"
            })
    
    injuries = get_active_injuries(engine, None)
    for inj in injuries[:3]:
        points.append({
            "league": inj.get('league', 'General').upper(),
            "topic": f"Injury: {inj.get('full_name')}",
            "say_this": f"Man, {inj.get('full_name')} being {inj.get('status')} is huge for {inj.get('team')}"
        })
    
    format_talking_points({"talking_points": points[:6]})

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
def summary(league):
    """View complete league summary"""
    print_header()
    from pipeline.db import get_engine, get_recent_upsets, get_active_injuries, get_team_records, get_upset_stats
    
    engine = get_engine("sqlite:///sports_data.db")
    
    records = get_team_records(engine, league)
    upsets = get_recent_upsets(engine, league, 5)
    injuries = get_active_injuries(engine, league)
    stats = get_upset_stats(engine, league)
    
    standings_list = []
    for i, rec in enumerate(records, 1):
        standings_list.append({
            "rank": i,
            "team": rec.get('team', 'Unknown'),
            "record": f"{rec.get('wins', 0)}-{rec.get('losses', 0)}",
            "win_pct": f"{rec.get('win_percentage', 0):.3f}",
            "streak": "W3" if i <= 3 else "L2" if i >= len(records) - 2 else "W1",
            "points_for": rec.get('points_for', 0),
            "points_against": rec.get('points_against', 0)
        })
    
    top_teams = standings_list[:3] if standings_list else []
    cold_teams = standings_list[-2:] if standings_list else []
    
    biggest_upset = None
    if upsets:
        biggest_upset = max(upsets, key=lambda x: x.get('upset_magnitude', 0))
    
    summary_data = {
        "at_a_glance": {
            "total_teams": len(records),
            "hot_teams": [t.get('team') for t in top_teams],
            "cold_teams": [t.get('team') for t in cold_teams],
            "biggest_upset": f"{biggest_upset.get('winner')} over {biggest_upset.get('loser')}" if biggest_upset else "None yet"
        },
        "quick_stats": {
            "total_upsets": stats.get('total_upsets', 0),
            "avg_upset_margin": stats.get('avg_magnitude', 0),
            "active_injuries": len(injuries)
        }
    }
    
    show_quick_summary(league, summary_data)
    console.print()
    format_standings({"standings": standings_list, "last_updated": datetime.now().isoformat()}, league)
    console.print()
    format_upsets({"upsets": upsets[:3]}, league)
    console.print()
    format_injuries({"injuries": injuries[:5]}, league)

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
def results(league):
    """View recent game results"""
    print_header()
    from pipeline.providers import thesportsdb
    
    games = thesportsdb.fetch_games(league, datetime.now().year)
    
    results = []
    for g in games[:10]:
        home = g.get('home_team', 'Home')
        away = g.get('away_team', 'Away')
        home_score = g.get('home_score', 0)
        away_score = g.get('away_score', 0)
        
        margin = abs(home_score - away_score)
        if margin <= 3:
            result_type = "thriller"
            emoji = "😱"
        elif margin <= 8:
            result_type = "close"
            emoji = "🔥"
        else:
            result_type = "blowout"
            emoji = "💪"
        
        results.append({
            "home": home,
            "away": away,
            "home_score": home_score,
            "away_score": away_score,
            "result_type": result_type,
            "emoji": emoji
        })
    
    format_game_results({"games": results}, league)

@cli.command()
def welcome():
    """Show welcome message"""
    print_header()
    show_welcome_message()
    show_league_selector()
    show_help_panel()

@cli.command()
def help():
    """Show help"""
    print_header()
    show_help_panel()

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
def players(league):
    """View top players"""
    print_header()
    from pipeline.providers import thesportsdb
    
    players = thesportsdb.fetch_players(league)
    
    if not players:
        console.print(f"[yellow]No player data for {league.upper()}. Run the pipeline first![/yellow]")
        return
    
    table = Table(
        title=f"🏃 Top Players ({league.upper()})",
        show_lines=True,
        box=box.ROUNDED,
        header_style="bold cyan"
    )
    
    table.add_column("#", width=3)
    table.add_column("Name", style="bold")
    table.add_column("Team")
    table.add_column("Position")
    table.add_column("Number")
    
    for i, p in enumerate(players[:20], 1):
        table.add_row(
            str(i),
            p.get('full_name', p.get('name', 'Unknown')),
            p.get('team', 'Unknown'),
            p.get('position', 'N/A'),
            p.get('jersey', '')
        )
    
    console.print(table)

@cli.command()
def all_leagues():
    """View all leagues overview"""
    print_header()
    from pipeline.db import get_engine, get_team_records, get_recent_upsets, get_active_injuries
    
    engine = get_engine("sqlite:///sports_data.db")
    
    console.print(Panel(
        Text("🌐 All Leagues Overview", justify="center", style="bold cyan"),
        border_style="cyan",
        box=ROUNDED
    ))
    console.print()
    
    for league in ['nfl', 'nba', 'mlb', 'nhl']:
        records = get_team_records(engine, league)
        upsets = get_recent_upsets(engine, league, 5)
        injuries = get_active_injuries(engine, league)
        
        top_team = records[0].get('team') if records else "No data"
        
        color = LEAGUE_COLORS.get(league, 'white')
        
        panel = f"""
[bold {color}]{league.upper()}[/bold {color}]

🏆 Top Team: [bold]{top_team}[/bold]
📊 Teams: {len(records)}
⚡ Upsets: {len(upsets)}
🏥 Injuries: {len(injuries)}
"""
        
        console.print(Panel(
            panel.strip(),
            title=f"[bold {color}]{EMOJI_MAP.get(league, '🏆')} {league.upper()}[/bold {color}]",
            border_style=color,
            box=ROUNDED
        ))
        console.print()

@cli.command()
@click.option('--league', '-l', default='nfl', type=click.Choice(['nfl', 'nba', 'mlb', 'nhl']), help='League to view')
@click.argument('team_name')
def team_info(league, team_name):
    """Get info about a specific team"""
    print_header()
    from pipeline.db import get_engine, get_team_records, get_active_injuries
    
    engine = get_engine("sqlite:///sports_data.db")
    records = get_team_records(engine, league)
    
    team = None
    for rec in records:
        if team_name.lower() in rec.get('team', '').lower():
            team = rec
            break
    
    if not team:
        console.print(f"[yellow]Team '{team_name}' not found in {league.upper()}![/yellow]")
        return
    
    console.print(Panel(
        f"""
[bold cyan]{team.get('team')}[/bold cyan]

📊 Record: [bold]{team.get('wins', 0)}-{team.get('losses', 0)}[/bold]
📈 Win %: [bold green]{team.get('win_percentage', 0):.3f}[/bold green]
🏟️ Home: {team.get('home_wins', 0)}-{team.get('home_losses', 0)}
✈️ Away: {team.get('away_wins', 0)}-{team.get('away_losses', 0)}
📱 PF: {team.get('points_for', 0)} | PA: {team.get('points_against', 0)}

[bold yellow]💬 Say this:[/bold yellow]
[italic]{team.get('team')} is {('dominating' if team.get('win_percentage', 0) > 0.6 else 'struggling')} this season at {team.get('win_percentage', 0):.1%}![/italic]
        """.strip(),
        title=f"🏟️ Team Info",
        border_style=LEAGUE_COLORS.get(league, 'cyan'),
        box=ROUNDED
    ))
    
    injuries = get_active_injuries(engine, league, team.get('team'))
    if injuries:
        console.print(f"\n[bold red]🏥 Active Injuries:[/bold red]")
        for inj in injuries:
            console.print(f"  • {inj.get('full_name')} ({inj.get('position')}) - {inj.get('status')} - {inj.get('injury_type')}")

if __name__ == '__main__':
    cli(obj={})