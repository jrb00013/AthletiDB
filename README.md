# AthletiDB

## Features

### Core Functionality
- **Multi-League Support**: NFL, NBA, MLB, NHL with extensible architecture
- **Real-time Data Integration**: Live API integration with TheSportsDB
- **Historical Data Management**: CSV data import and legacy data support
- **Upset Detection**: Advanced algorithms for identifying surprising game outcomes
- **Injury Tracking**: Comprehensive player injury monitoring and analysis
- **Team Performance Analysis**: Detailed team records and statistical analysis
- **Data Export**: Multiple formats (CSV, JSON, Excel) with metadata

### Technical Features
- **Rate Limiting**: Intelligent API request management with burst protection
- **Caching System**: File-based caching for improved performance
- **Database Management**: SQLite with comprehensive schema and indexing
- **Data Validation**: Pydantic models for data integrity
- **Modular Architecture**: Extensible provider system for new data sources
- **Comprehensive Testing**: Full test suite with performance benchmarks

## Quick Start

### Prerequisites
- Python 3.8 or higher
- Git (for submodules)
- 8GB+ RAM (for large datasets)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/sports-data-pipeline.git
   cd sports-data-pipeline
   ```

2. **Run the setup script**
   ```bash
   python scripts/setup.py
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

4. **Activate virtual environment**
   ```bash
   # Linux/Mac
   source .venv/bin/activate
   
   # Windows
   .venv\Scripts\activate
   ```

5. **Test the installation**
   ```bash
   python tests/test_suite.py
   ```

## Usage

### Command Line Interface

The pipeline provides a comprehensive CLI with multiple command grou#### Data Fetching
```bash
# Fetch NFL players 
python ui/cli.py fetch players --league nfl --source live

# Fetch NBA teams
python ui/cli.py fetch teams --league nba --source live

# Fetch MLB games 
python ui/cli.py fetch games --league mlb --season 2024 --source live

# Fetch injury data    (replace nfl with mlb or nba)
python ui/cli.py fetch injuries --league nfl --team "New England Patriots"
```

#### Data Export
```bash
# Export all NFL data to CSV
python ui/cli.py export players --league nfl --format csv

# Export upsets to Excel
python ui/cli.py export upsets --format excel --output-dir reports

# Export all data for all leagues
python ui/cli.py export all --format json --include-metadata
```

#### Data Analysis
```bash
# Analyze recent upsets
python ui/cli.py analyze upsets --league nfl --limit 20

# Analyze injury patterns
python ui/cli.py analyze injuries --league nba --team "Los Angeles Lakers"

# Analyze team performance
python ui/cli.py analyze teams --league mlb --season 2024
```

#### System Management
```bash
# Check system status
python ui/cli.py system status

# Reset database (WARNING: deletes all data)
python ui/cli.py system reset --force

# Clean up temporary files
python ui/cli.py system cleanup
```

#### Complete Pipeline
```bash
# Run complete pipeline for NFL
python ui/cli.py run --league nfl --source live --export --export-format csv

# Run for all leagues with legacy data
python ui/cli.py run --source all --include-upsets --include-injuries
```

### Programmatic Usage

```python
from pipeline.db import get_engine, get_recent_upsets
from pipeline.providers import thesportsdb

# Initialize database
engine = get_engine("sqlite:///sports_data.db")

# Fetch live data
players = thesportsdb.fetch_players("nfl")
teams = thesportsdb.fetch_teams("nba")

# Query data
recent_upsets = get_recent_upsets(engine, league="nfl", limit=10)
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
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
```

### Configuration File

The `config.yaml` file provides additional configuration options:

```yaml
# Database Configuration
database:
  url: "sqlite:///sports_data.db"
  backup_enabled: true
  backup_interval: 86400

# API Configuration
apis:
  thesportsdb:
    base_url: "https://www.thesportsdb.com/api/v1/json"
    rate_limit: 1800
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
```

## Data Sources

### Live APIs
- **TheSportsDB**: Primary data source for live sports data
- **Rate Limits**: 30 requests per minute (free tier)
- **Data Types**: Players, teams, games, injuries

### Historical Data
- **CSV Files**: Local historical data for MLB, NBA, NHL
- **NFL Data**: Git submodule integration with nflverse-pbp
- **Data Formats**: Standardized CSV with metadata

### Data Schema

The pipeline uses a comprehensive database schema with the following tables:

- **players**: Comprehensive player information
- **upsets**: Upset detection and analysis
- **injuries**: Player injury tracking
- **team_records**: Team performance and statistics
- **games**: Game results and statistics
- **player_stats**: Individual player statistics
- **team_analysis**: Team strengths and weaknesses

### Provider System

The pipeline uses a modular provider system for data sources:

- **BaseProvider**: Abstract base class for all providers
- **TheSportsDBProvider**: Live API integration
- **CSVProvider**: Historical data import
- **NFLVerseProvider**: NFL-specific data

### Rate Limiting

Intelligent rate limiting with burst protection:

- **Configurable Limits**: Per-API rate limits
- **Burst Protection**: Prevents API abuse
- **Automatic Reset**: Hourly rate limit resets
- **Queue Management**: Request queuing when limits exceeded

## Development

### Setting Up Development Environment

1. **Clone and setup**
   ```bash
   git clone https://github.com/yourusername/sports-data-pipeline.git
   cd sports-data-pipeline
   python scripts/setup.py
   ```

2. **Install development dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```

3. **Run tests**
   ```bash
   python tests/test_suite.py
   ```

4. **Run linting**
   ```bash
   flake8 pipeline/ ui/ tests/
   black pipeline/ ui/ tests/
   ```

### Adding New Data Sources

1. **Create provider class**
   ```python
   from pipeline.providers.base import BaseProvider
   
   class NewProvider(BaseProvider):
       def fetch_players(self, league: str, season: int = None):
           # Implementation here
           pass
   ```

2. **Register provider**
   ```python
   # In pipeline/providers/__init__.py
   from .new_provider import NewProvider
   
   providers = {
       'new_source': NewProvider()
   }
   ```

3. **Add tests**
   ```python
   class TestNewProvider(unittest.TestCase):
       def test_fetch_players(self):
           # Test implementation
           pass
   ```

### Testing

The project includes comprehensive testing:

- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end pipeline testing
- **Performance Tests**: Performance benchmarking
- **Coverage Reports**: Code coverage analysis

Run tests with:
```bash
python tests/test_suite.py
```

## Performance

### Benchmarks

- **Database Operations**: 1000+ records/second
- **API Requests**: 30 requests/minute (TheSportsDB limit)
- **Data Export**: 10MB/second CSV export
- **Memory Usage**: <500MB for typical operations

### Optimization

- **Database Indexing**: Optimized queries with proper indexing
- **Caching**: File-based caching for API responses
- **Batch Processing**: Efficient bulk operations
- **Memory Management**: Streaming for large datasets

## Deployment

### Production Setup

1. **Environment Configuration**
   ```bash
   export PYTHONPATH=/path/to/sports-data-pipeline
   export LOG_LEVEL=WARNING
   ```

2. **Database Configuration**
   ```bash
   # Use PostgreSQL for production
   export DATABASE_URL=postgresql://user:pass@localhost/sports_db
   ```

3. **Process Management**
   ```bash
   # Use systemd or supervisor
   sudo systemctl enable sports-pipeline
   sudo systemctl start sports-pipeline
   ```

### Docker Deployment

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "main.py"]
```

### Cron Jobs

Automated data collection:

```bash
# Update data every hour
0 * * * * /path/to/sports-pipeline/start.sh run --league nfl

# Daily analysis report
0 6 * * * /path/to/sports-pipeline/start.sh report --output daily_report.txt

# Weekly cleanup
0 2 * * 0 /path/to/sports-pipeline/start.sh system cleanup
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Check file permissions for `sports_data.db`
   - Verify SQLite installation
   - Check disk space

2. **API Rate Limit Errors**
   - Verify rate limit configuration
   - Check API key validity
   - Monitor request frequency

3. **Import Errors**
   - Verify Python path configuration
   - Check virtual environment activation
   - Install missing dependencies

### Debug Mode

Enable verbose logging:
```bash
python ui/cli.py --verbose system status
```

### Log Files

Check log files for detailed error information:
- `logs/pipeline.log`: Main pipeline logs
- `logs/error.log`: Error-specific logs
- `logs/access.log`: API access logs

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints
- Add docstrings for all functions
- Keep functions focused and small

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.



## Acknowledgments

- **TheSportsDB**: For providing comprehensive sports data APIs
- **NFLVerse**: For NFL play-by-play data


---

**AthletiDB** - Empowering sports analysts since 2025.

