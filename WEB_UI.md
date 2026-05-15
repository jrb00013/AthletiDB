# AthletiDB - Sports Conversation Companion

## Quick Start

### Web UI (Recommended for Beginners)
```bash
# Start the web server
python web/main.py

# Then open http://localhost:8000 in your browser
```

### Terminal UI (TUI)
```bash
# View a league summary
python -m ui.tui summary --league nfl

# View standings
python -m ui.tui standings --league nba

# View upsets
python -m ui.tui upsets --league mlb

# View injuries
python -m ui.tui injuries --league nhl

# Get talking points
python -m ui.tui talking-points

# See all leagues
python -m ui.tui all-leagues
```

### Quick Commands
```bash
# Using the shell script
./run_athletidb.sh web           # Start web UI
./run_athletidb.sh tui nfl       # Open TUI
./run_athletidb.sh run nfl       # Fetch data for NFL
./run_athletidb.sh standings nba # View NBA standings
./run_athletidb.sh quick         # Quick sports update
```

### Fetch Data
```bash
# Fetch data for a specific league
python main.py --league nfl --include-upsets --include-injuries

# Fetch for all leagues
python main.py --include-upsets --include-injuries
```

## Features

### 🌐 Web UI
- **Dark theme** modern interface
- **League tabs** for NFL, NBA, MLB, NHL
- **Standings** with rankings, records, streaks
- **Upset alerts** with "say this" conversation starters
- **Injury tracker** with severity and impact
- **Talking points** for casual conversations
- **Modal popups** for quick stats and insights

### 📱 Terminal UI (TUI)
- **Rich terminal** with colors and tables
- **Standings view** with win streaks
- **Upset alerts** with conversation scripts
- **Injury tracking** with severity indicators
- **Talking points** for any league
- **Team lookup** by name

### 💬 Conversation Features
Every piece of data comes with a "Say this:" suggestion!
- Upset alerts: "Did you see X upset Y? Nobody saw that coming!"
- Injuries: "Oh man, X being out is huge for Y!"
- Hot teams: "X has been playing incredible lately!"

## API Endpoints

The web server exposes these endpoints:

- `/api/league/{league}/standings` - Current standings
- `/api/league/{league}/upsets` - Recent upsets with conversation
- `/api/league/{league}/injuries` - Active injuries with impact
- `/api/league/{league}/teams` - All teams in a league
- `/api/league/{league}/top-players` - Top players
- `/api/league/{league}/game-results` - Recent game results
- `/api/league/{league}/quick-stats` - Quick stats summary
- `/api/summary/{league}` - Full league summary
- `/api/chat/talking-points` - Conversation starters
- `/api/insights` - All current insights
- `/api/quick-stats/{league}` - Quick stats
- `/api/all-leagues` - Overview of all leagues
- `/api/health` - Health check

## Leagues Supported
- 🏈 NFL - National Football League
- 🏀 NBA - National Basketball Association  
- ⚾ MLB - Major League Baseball
- 🏒 NHL - National Hockey League

## Examples

### For a Sports Conversation:
> "Did you see that Chiefs upset the Bills? Nobody saw that coming!"

> "Oh man, LeBron being out is huge for the Lakers!"

> "Everyone's talking about how the Celtics are dominating right now."

### Quick Talking Points:
```bash
python -m ui.tui talking-points
```

### Team Lookup:
```bash
python -m ui.tui team-info nfl "Patriots"
```

## Troubleshooting

**No data showing?**
Run the pipeline first:
```bash
python main.py --league nfl --include-upsets --include-injuries
```

**Web UI not loading?**
Make sure you're running from the project root:
```bash
cd /path/to/athletidb
python web/main.py
```

**Database error?**
The database will be created automatically when you run the pipeline.