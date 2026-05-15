# AthletiDB

## Quick Start

### 🌐 Web UI
```bash
python web/main.py
# Open http://localhost:8000
```

### 📱 Terminal UI (TUI)
```bash
python -m ui.tui summary --league nfl
python -m ui.tui standings --league nba
python -m ui.tui upsets --league mlb
python -m ui.tui injuries --league nhl
python -m ui.tui talking-points
python -m ui.tui all-leagues
python -m ui.tui welcome
python -m ui.tui help
```

### 🏃 Quick Start
```bash
python quick_start.py
```

### 🚀 Shell Commands
```bash
./run_athletidb.sh web           # Start web UI
./run_athletidb.sh tui nfl        # Open TUI
./run_athletidb.sh run nfl        # Fetch data
./run_athletidb.sh standings nba  # View standings
./run_athletidb.sh quick          # Quick update
```

### 📥 Fetch Data
```bash
python main.py --league nfl --include-upsets --include-injuries
python main.py --include-upsets --include-injuries
```

## Features

### Web UI
- 🏈🏀⚾🏒 League tabs (NFL, NBA, MLB, NHL)
- 📊 Standings with rankings and streaks
- ⚡ Upsets with "Say this:" conversation starters  
- 🏥 Injuries with severity indicators
- 💬 Talking points for casual conversations
- 📰 Around the Horn news view
- 📣 Headlines with priority indicators
- 🌐 All leagues overview
- 📊 Quick stats modal
- 💡 Insights modal

### Terminal UI
- Rich colored terminal output
- Standings, upsets, injuries tables
- Team lookup command
- All leagues overview
- Welcome/help panels

### Conversation Features
Every data point comes with ready-to-use conversation scripts!

## API Endpoints
- `/api/league/{league}/standings` - Standings
- `/api/league/{league}/upsets` - Upsets with conversation
- `/api/league/{league}/injuries` - Active injuries
- `/api/league/{league}/teams` - Team list
- `/api/league/{league}/top-players` - Players
- `/api/league/{league}/game-results` - Game results
- `/api/league/{league}/quick-stats` - Quick stats
- `/api/league/{league}/headlines` - Headlines
- `/api/summary/{league}` - Full summary
- `/api/chat/talking-points` - Talking points
- `/api/insights` - All insights
- `/api/quick-stats/{league}` - League stats
- `/api/all-leagues` - All leagues overview
- `/api/around-the-horn` - All sports news
- `/api/streaks/{league}` - Win/loss streaks
- `/api/search?q=query` - Search all data

## Requirements
- Python 3.8+
- See requirements.txt for dependencies