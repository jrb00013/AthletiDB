#!/bin/bash
# Sports Data Pipeline Cron Jobs
# Comprehensive automation examples for different platforms

# Set project directory (update this path)
PROJECT_DIR="/mnt/c/Users/josep/Documents/AthletiDB_ws/AthletiDB"

# Activate virtual environment
cd "$PROJECT_DIR" || exit 1
source .venv/bin/activate

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:${PROJECT_DIR}"
export LOG_LEVEL="INFO"

# ============================================================================
# DAILY OPERATIONS
# ============================================================================

# Daily data collection (6:00 AM)
# 0 6 * * * /path/to/cron_jobs.sh daily_update

daily_update() {
    echo "Starting daily update at $(date)"
    
    # Update all leagues
    python main.py --league nfl --source live --include-upsets --include-injuries
    python main.py --league nba --source live --include-upsets --include-injuries
    python main.py --league mlb --source live --include-upsets --include-injuries
    python main.py --league nhl --source live --include-upsets --include-injuries
    
    # Export daily report
    python ui/cli.py report --output "reports/daily_$(date +%Y%m%d).txt"
    
    echo "Daily update completed at $(date)"
}

# ============================================================================
# HOURLY OPERATIONS
# ============================================================================

# Hourly upset monitoring (every hour)
# 0 * * * * /path/to/cron_jobs.sh hourly_upset_check

hourly_upset_check() {
    echo "Checking for upsets at $(date)"
    
    # Check recent upsets for all leagues
    python ui/cli.py analyze upsets --league nfl --limit 50
    python ui/cli.py analyze upsets --league nba --limit 50
    python ui/cli.py analyze upsets --league mlb --limit 50
    python ui/cli.py analyze upsets --league nhl --limit 50
    
    echo "Upset check completed at $(date)"
}

# ============================================================================
# WEEKLY OPERATIONS
# ============================================================================

# Weekly comprehensive analysis (Sunday 2:00 AM)
# 0 2 * * 0 /path/to/cron_jobs.sh weekly_analysis

weekly_analysis() {
    echo "Starting weekly analysis at $(date)"
    
    # Export all data
    python ui/cli.py export all --format excel --output-dir "reports/weekly_$(date +%Y%m%d)"
    
    # Generate comprehensive reports
    python ui/cli.py analyze teams --league nfl
    python ui/cli.py analyze teams --league nba
    python ui/cli.py analyze teams --league mlb
    python ui/cli.py analyze teams --league nhl
    
    # Create backup
    python scripts/backup.py create --include-logs --include-exports --compress
    
    echo "Weekly analysis completed at $(date)"
}

# ============================================================================
# MAINTENANCE OPERATIONS
# ============================================================================

# Daily cleanup (2:00 AM)
# 0 2 * * * /path/to/cron_jobs.sh daily_cleanup

daily_cleanup() {
    echo "Starting daily cleanup at $(date)"
    
    # Clean up old logs
    find logs/ -name "*.log.*" -mtime +7 -delete
    
    # Clean up old exports (keep last 30 days)
    find exports/ -name "*.csv" -mtime +30 -delete
    find exports/ -name "*.json" -mtime +30 -delete
    find exports/ -name "*.xlsx" -mtime +30 -delete
    
    # Clean up old reports (keep last 90 days)
    find reports/ -name "*.txt" -mtime +90 -delete
    
    # System cleanup
    python ui/cli.py system cleanup
    
    echo "Daily cleanup completed at $(date)"
}

# Weekly backup cleanup (Saturday 3:00 AM)
# 0 3 * * 6 /path/to/cron_jobs.sh weekly_backup_cleanup

weekly_backup_cleanup() {
    echo "Starting weekly backup cleanup at $(date)"
    
    # Clean up old backups (keep last 30 days, minimum 10 backups)
    python scripts/backup.py cleanup --keep-days 30 --keep-count 10
    
    echo "Weekly backup cleanup completed at $(date)"
}

# ============================================================================
# MONITORING OPERATIONS
# ============================================================================

# Health check (every 15 minutes)
# */15 * * * * /path/to/cron_jobs.sh health_check

health_check() {
    echo "Health check at $(date)"
    
    # Check system status
    python ui/cli.py system status
    
    # Check database health
    python -c "
from pipeline.db import get_engine
try:
    engine = get_engine('sqlite:///sports_data.db')
    print('Database: OK')
except Exception as e:
    print(f'Database: ERROR - {e}')
    exit(1)
"
    
    # Check API status
    python -c "
from pipeline.providers import thesportsdb
try:
    status = thesportsdb.get_rate_limit_status()
    print(f'API: OK - {status[\"current_requests\"]}/{status[\"requests_per_hour\"]} requests')
except Exception as e:
    print(f'API: ERROR - {e}')
"
    
    echo "Health check completed at $(date)"
}

# ============================================================================
# SEASONAL OPERATIONS
# ============================================================================

# Season start preparation (run manually)
season_start() {
    echo "Preparing for new season at $(date)"
    
    # Reset database for new season
    python ui/cli.py system reset --force
    
    # Initialize with new season data
    python main.py --league nfl --source live
    python main.py --league nba --source live
    python main.py --league mlb --source live
    python main.py --league nhl --source live
    
    # Create season backup
    python scripts/backup.py create --include-logs --include-exports --compress
    
    echo "Season start preparation completed at $(date)"
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

# Show current cron jobs
show_cron_jobs() {
    echo "Current cron jobs:"
    crontab -l 2>/dev/null || echo "No cron jobs found"
}

# Install cron jobs
install_cron_jobs() {
    echo "Installing cron jobs..."
    
    # Create temporary cron file
    TEMP_CRON=$(mktemp)
    
    # Add cron jobs
    cat >> "$TEMP_CRON" << 'EOF'
# Sports Data Pipeline Cron Jobs

# Health check every 15 minutes
*/15 * * * * /path/to/cron_jobs.sh health_check

# Daily operations
0 2 * * * /path/to/cron_jobs.sh daily_cleanup
0 6 * * * /path/to/cron_jobs.sh daily_update

# Hourly operations
0 * * * * /path/to/cron_jobs.sh hourly_upset_check

# Weekly operations
0 2 * * 0 /path/to/cron_jobs.sh weekly_analysis
0 3 * * 6 /path/to/cron_jobs.sh weekly_backup_cleanup
EOF
    
    # Install cron jobs
    crontab "$TEMP_CRON"
    rm "$TEMP_CRON"
    
    echo "Cron jobs installed successfully"
    show_cron_jobs
}

# Remove all cron jobs
remove_cron_jobs() {
    echo "Removing all cron jobs..."
    crontab -r
    echo "All cron jobs removed"
}

# ============================================================================
# MAIN SCRIPT LOGIC
# ============================================================================

# Check if function name provided
if [ $# -eq 0 ]; then
    echo "Sports Data Pipeline Cron Jobs"
    echo "Usage: $0 <function_name>"
    echo ""
    echo "Available functions:"
    echo "  daily_update           - Daily data collection"
    echo "  hourly_upset_check    - Hourly upset monitoring"
    echo "  weekly_analysis       - Weekly comprehensive analysis"
    echo "  daily_cleanup         - Daily cleanup operations"
    echo "  weekly_backup_cleanup - Weekly backup cleanup"
    echo "  health_check          - System health check"
    echo "  season_start          - Season start preparation"
    echo "  show_cron_jobs        - Show current cron jobs"
    echo "  install_cron_jobs     - Install cron jobs"
    echo "  remove_cron_jobs      - Remove all cron jobs"
    exit 1
fi

# Execute requested function
FUNCTION_NAME="$1"
shift

if [ "$(type -t "$FUNCTION_NAME")" = "function" ]; then
    "$FUNCTION_NAME" "$@"
else
    echo "Error: Function '$FUNCTION_NAME' not found"
    exit 1
fi
