#!/bin/bash
# AthletiDB - Quick Commands

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}🏆 AthletiDB Quick Commands${NC}"
echo "================================"

# Parse arguments
COMMAND=${1:-help}
LEAGUE=${2:-nfl}

case $COMMAND in
  web)
    echo -e "${BLUE}Starting Web UI on http://localhost:8000...${NC}"
    cd "$(dirname "$0")"
    python web/main.py
    ;;
    
  tui)
    echo -e "${BLUE}Starting Terminal UI...${NC}"
    cd "$(dirname "$0")"
    python -m ui.tui summary --league $LEAGUE
    ;;
    
  run)
    echo -e "${BLUE}Running data pipeline for ${LEAGUE}...${NC}"
    cd "$(dirname "$0")"
    python main.py --league $LEAGUE --include-upsets --include-injuries
    ;;
    
  all)
    echo -e "${BLUE}Running pipeline for all leagues...${NC}"
    cd "$(dirname "$0")"
    for lg in nfl nba mlb nhl; do
      echo "Running for $lg..."
      python main.py --league $lg --include-upsets --include-injuries
    done
    ;;
    
  quick)
    echo -e "${BLUE}Running quick update...${NC}"
    cd "$(dirname "$0")"
    python quick_start.py
    ;;
    
  standings)
    echo -e "${BLUE}Getting standings for ${LEAGUE}...${NC}"
    cd "$(dirname "$0")"
    python -m ui.tui standings --league $LEAGUE
    ;;
    
  upsets)
    echo -e "${BLUE}Getting upsets for ${LEAGUE}...${NC}"
    cd "$(dirname "$0")"
    python -m ui.tui upsets --league $LEAGUE
    ;;
    
  injuries)
    echo -e "${BLUE}Getting injuries for ${LEAGUE}...${NC}"
    cd "$(dirname "$0")"
    python -m ui.tui injuries --league $LEAGUE
    ;;
    
  help|*)
    echo -e "${YELLOW}Usage: ./run.sh [COMMAND] [LEAGUE]${NC}"
    echo ""
    echo "Commands:"
    echo "  web            - Start web UI (http://localhost:8000)"
    echo "  tui [league]   - Open terminal UI (default: nfl)"
    echo "  run [league]   - Run data pipeline for a league"
    echo "  all            - Run pipeline for all leagues"
    echo "  quick          - Quick sports update"
    echo "  standings [league] - View standings"
    echo "  upsets [league]   - View recent upsets"
    echo "  injuries [league] - View active injuries"
    echo ""
    echo "Leagues: nfl, nba, mlb, nhl (default: nfl)"
    echo ""
    echo "Examples:"
    echo "  ./run.sh web"
    echo "  ./run.sh tui nba"
    echo "  ./run.sh run nfl"
    echo "  ./run.sh standings nba"
    ;;
esac