#!/usr/bin/env python3
"""
Sports Data Pipeline Setup Script
Comprehensive setup and initialization for the sports data pipeline.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 8):
        logger.error("Python 3.8 or higher is required")
        sys.exit(1)
    logger.info(f"Python {sys.version_info.major}.{sys.version_info.minor} detected")

def create_directories():
    """Create necessary directories."""
    directories = [
        "exports",
        "cache", 
        "logs",
        "data/nfl",
        "data/nba",
        "data/mlb",
        "data/nhl",
        "reports",
        "backups"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def install_dependencies():
    """Install required Python packages."""
    logger.info("Installing Python dependencies...")
    
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        logger.info("Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install dependencies: {e}")
        sys.exit(1)

def setup_virtual_environment():
    """Set up Python virtual environment."""
    venv_path = Path(".venv")
    
    if venv_path.exists():
        logger.info("Virtual environment already exists")
        return
    
    logger.info("Creating virtual environment...")
    
    try:
        subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
        logger.info("Virtual environment created successfully")
        
        # Activate and install dependencies
        if os.name == 'nt':  # Windows
            activate_script = ".venv\\Scripts\\activate"
            pip_path = ".venv\\Scripts\\pip"
        else:  # Unix/Linux/Mac
            activate_script = ".venv/bin/activate"
            pip_path = ".venv/bin/pip"
        
        logger.info(f"Virtual environment ready. Activate with: {activate_script}")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create virtual environment: {e}")
        sys.exit(1)

def setup_git_submodules():
    """Set up Git submodules for external data sources."""
    logger.info("Setting up Git submodules...")
    
    try:
        # Initialize submodules
        subprocess.run(["git", "submodule", "init"], check=True, capture_output=True)
        subprocess.run(["git", "submodule", "update"], check=True, capture_output=True)
        logger.info("Git submodules initialized successfully")
    except subprocess.CalledProcessError as e:
        logger.warning(f"Git submodule setup failed (may not be in a git repo): {e}")

def create_config_files():
    """Create configuration files if they don't exist."""
    config_files = {
        "config.yaml": """# Sports Data Pipeline Configuration
# Database Configuration
database:
  url: "sqlite:///sports_data.db"
  backup_enabled: true
  backup_interval: 86400  # 24 hours

# API Configuration
apis:
  thesportsdb:
    base_url: "https://www.thesportsdb.com/api/v1/json"
    rate_limit: 1800  # requests per hour
    timeout: 30

# Data Processing
processing:
  batch_size: 1000
  max_workers: 4
  retry_attempts: 3

# Export Configuration
export:
  default_format: "csv"
  include_metadata: true
  compression: false

# Logging
logging:
  level: "INFO"
  file: "logs/pipeline.log"
  max_size: "10MB"
  backup_count: 5
""",
        
        ".env": """# Sports Data Pipeline Environment Variables
# Copy this file to .env and fill in your actual values

# TheSportsDB API Configuration
THESPORTSDB_API_KEY=your_api_key_here
THESPORTSDB_RATE_LIMIT=30
THESPORTSDB_BASE_URL=https://www.thesportsdb.com/api/v1/json

# Database Configuration
DATABASE_URL=sqlite:///sports_data.db
CSV_EXPORT_DIR=exports

# Data Source Configuration
ENABLE_LIVE_API=true
ENABLE_NFLVERSE=true
ENABLE_HISTORICAL_CSV=true

# Rate Limiting and Caching
CACHE_DURATION=3600
REQUEST_TIMEOUT=30
MAX_RETRIES=3

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/sports_pipeline.log

# Optional External API Keys
# BALDONTLIE_API_KEY=your_key_here
# ESPN_API_KEY=your_key_here
# SPORTRADAR_API_KEY=your_key_here
"""
    }
    
    for filename, content in config_files.items():
        if not Path(filename).exists():
            with open(filename, 'w') as f:
                f.write(content)
            logger.info(f"Created configuration file: {filename}")
        else:
            logger.info(f"Configuration file already exists: {filename}")

def setup_database():
    """Initialize the database."""
    logger.info("Initializing database...")
    
    try:
        from pipeline.db import get_engine
        engine = get_engine("sqlite:///sports_data.db")
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)

def run_tests():
    """Run basic tests to verify setup."""
    logger.info("Running basic tests...")
    
    try:
        # Test database connection
        from pipeline.db import get_engine
        engine = get_engine("sqlite:///sports_data.db")
        
        # Test basic imports
        from pipeline.providers import thesportsdb
        from pipeline.utils import get_rate_limiter
        
        logger.info("Basic tests passed")
        
    except Exception as e:
        logger.error(f"Tests failed: {e}")
        sys.exit(1)

def create_startup_scripts():
    """Create startup scripts for different platforms."""
    scripts = {
        "start.sh": """#!/bin/bash
# Sports Data Pipeline Startup Script (Linux/Mac)

# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Run the pipeline
python main.py "$@"
""",
        
        "start.bat": """@echo off
REM Sports Data Pipeline Startup Script (Windows)

REM Activate virtual environment
call .venv\\Scripts\\activate.bat

REM Set environment variables
set PYTHONPATH=%PYTHONPATH%;%CD%

REM Run the pipeline
python main.py %*
""",
        
        "start.ps1": """# Sports Data Pipeline Startup Script (PowerShell)

# Activate virtual environment
& .venv\\Scripts\\Activate.ps1

# Set environment variables
$env:PYTHONPATH = "$env:PYTHONPATH;$(Get-Location)"

# Run the pipeline
python main.py $args
"""
    }
    
    for filename, content in scripts.items():
        script_path = Path(filename)
        if not script_path.exists():
            with open(script_path, 'w') as f:
                f.write(content)
            
            # Make shell scripts executable on Unix systems
            if filename.endswith('.sh'):
                script_path.chmod(0o755)
            
            logger.info(f"Created startup script: {filename}")

def main():
    """Main setup function."""
    logger.info("Starting Sports Data Pipeline Setup...")
    
    # Check requirements
    check_python_version()
    
    # Create directories
    create_directories()
    
    # Set up virtual environment
    setup_virtual_environment()
    
    # Install dependencies
    install_dependencies()
    
    # Set up Git submodules
    setup_git_submodules()
    
    # Create configuration files
    create_config_files()
    
    # Initialize database
    setup_database()
    
    # Run tests
    run_tests()
    
    # Create startup scripts
    create_startup_scripts()
    
    logger.info("Setup completed successfully!")
    logger.info("\nNext steps:")
    logger.info("1. Copy .env.example to .env and fill in your API keys")
    logger.info("2. Activate the virtual environment: source .venv/bin/activate (Linux/Mac) or .venv\\Scripts\\activate (Windows)")
    logger.info("3. Run the pipeline: python main.py --help")
    logger.info("4. Or use the CLI: python ui/cli.py --help")

if __name__ == "__main__":
    main()
