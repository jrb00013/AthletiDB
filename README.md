# Players Pipeline

Pull up-to-date player lists from NBA, NFL, MLB, and NHL into one table, plus track upsets and notable games.

## Features

- **Multi-League Support**: NBA, NFL, MLB, NHL
- **Unified Schema**: All players normalized to one database table
- **Upsets Tracking**: Monitor surprising game outcomes and underdog victories
- **Export Options**: SQLite database + CSV exports per league
- **Extensible**: Easy to add new leagues or data sources

## Quick Start

### 1) Setup Environment
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
cp .env.example .env
```

### 2) Configure
Edit `config.yaml` to select leagues and optionally specify seasons.

### 3) Run Pipeline
```bash
python main.py
```

## Outputs

- **SQLite Database**: `players.db` with `players` and `upsets` tables
- **CSV Exports**: League-specific player lists in `exports/` directory
- **Upsets Data**: Game results and upset tracking in database

## Data Sources

- **NBA**: balldontlie.io (public API)
- **NFL**: Sleeper public players dump
- **MLB**: MLB Stats API (public)
- **NHL**: NHL Stats API (public)

## Extending the Pipeline

To add a new league/provider:

1. Create `pipeline/providers/<league>.py` with a `fetch(season: int|None) -> list[dict]` function
2. Register it in `PROVIDERS` in `main.py`
3. Optionally add upset tracking logic for the new league

## Upsets Tracking

The pipeline now includes upset detection and tracking:
- Monitors game results for unexpected outcomes
- Tracks underdog victories and surprising performances
- Stores historical upset data for analysis
- Supports different upset definitions per league

## CLI Usage

```bash
# Run all leagues
python main.py

# Run specific league
python main.py --league nba

# Run with upset tracking only
python main.py --upsets-only

# Run specific league with upsets
python main.py --league nfl --include-upsets
```
