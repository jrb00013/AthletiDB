#!/bin/bash
# Sports Data Pipeline - Automated Cron Jobs
# This script contains cron job examples for WSL/Ubuntu
# Usage: Copy the desired lines to your crontab (crontab -e)

# Set the project directory (adjust this path to match your setup)
PROJECT_DIR="/mnt/c/Users/josep/Documents/AthletiDB_ws/AthletiDB"

# Activate virtual environment
cd "$PROJECT_DIR" || exit 1

# Daily player data refresh at 6 AM
# 0 6 * * * cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --include-upsets --include-injuries

# Weekly full refresh on Sundays at 2 AM
# 0 2 * * 0 cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --include-upsets --include-injuries --show-stats

# Upset tracking only, every 4 hours during weekdays (Mon-Fri)
# 0 */4 * * 1-5 cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --upsets-only

# Specific league updates
# NBA on weekdays at 8 AM
# 0 8 * * 1-5 cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --league nba

# NFL on weekends at 9 AM
# 0 9 * * 0,6 cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --league nfl

# Monthly statistics report on the 1st of each month at 7 AM
# 0 7 1 * * cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --upsets-only --show-stats > /var/log/sports_pipeline_monthly.log 2>&1

# Hourly quick check during game days (Mon-Sun, 6 PM to 11 PM)
# 0 18-23 * * 1-7 cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --upsets-only --show-status

# Daily backup and cleanup at 3 AM
# 0 3 * * * cd "$PROJECT_DIR" && source .venv/bin/activate && python main.py --show-status && find cache/ -name "*.cache" -mtime +7 -delete

# Notes for WSL/Ubuntu setup:
# 1. Edit crontab: crontab -e
# 2. Uncomment the lines you want to use
# 3. Adjust PROJECT_DIR path if needed
# 4. Ensure .venv exists and has required packages
# 5. Test manually first: python main.py --help
# 6. Check logs: tail -f /var/log/syslog | grep CRON
# 7. For debugging: add >> /tmp/cron_debug.log 2>&1 to any line
