# Contributing to Sports Data Pipeline

Thank you for your interest in contributing to the Sports Data Pipeline! This project aims to provide comprehensive sports data analysis and tracking capabilities across multiple leagues.

## Quick Start

1. **Fork** the repository
2. **Clone** your fork: `git clone https://github.com/YOUR_USERNAME/sports-data-pipeline.git`
3. **Add submodules**: `git submodule update --init --recursive`
4. **Create** a virtual environment: `python -m venv .venv`
5. **Install** dependencies: `pip install -r requirements.txt`
6. **Run tests**: `python test_setup.py`

## Project Structure

```
sports-data-pipeline/
├── data/                    # Local data storage
│   ├── nfl/               # NFL data (from nflverse submodule)
│   ├── mlb/               # MLB historical CSV data
│   ├── nba/               # NBA historical CSV data
│   └── nhl/               # NHL historical CSV data
├── pipeline/               # Core pipeline modules
├── providers/              # Data source providers
├── ui/                     # CLI and web interface components
├── analysis/               # Data analysis and insights
└── docs/                   # Documentation
```

## Development Setup

### Prerequisites
- Python 3.8+
- Git with submodule support
- Access to TheSportsDB API (optional)

### Environment Variables
```bash
# Copy and configure
cp env.example .env

# Required for live data
THESPORTSDB_API_KEY=your_api_key_here
THESPORTSDB_RATE_LIMIT=1800  # requests per hour (30 per minute)

# Optional: Database configuration
DATABASE_URL=sqlite:///sports_data.db
```

## Areas for Contribution

### 1. Data Providers
- **New League Support**: Add providers for soccer, cricket, etc.
- **Enhanced APIs**: Improve existing provider reliability
- **Data Validation**: Add data quality checks and validation

### 2. Analysis Features
- **Statistical Models**: Advanced analytics and predictions
- **Trend Analysis**: Historical performance patterns
- **Comparative Analysis**: Cross-league comparisons

### 3. User Interface
- **CLI Enhancements**: Better command organization and help
- **Web Dashboard**: Simple web interface for data exploration
- **Data Export**: Additional export formats (JSON, Excel, etc.)

### 4. Data Quality
- **Data Cleaning**: Handle missing or inconsistent data
- **Standardization**: Normalize team names, player IDs across sources
- **Historical Data**: Add more comprehensive historical datasets

## Code Style

### Python Standards
- **PEP 8** compliance
- **Type hints** for all function parameters and returns
- **Docstrings** for all public functions and classes
- **Error handling** with meaningful error messages

### Example Function
```python
def fetch_player_stats(
    player_id: str, 
    season: int, 
    league: str
) -> Dict[str, Any]:
    """
    Fetch comprehensive player statistics for a given season.
    
    Args:
        player_id: Unique player identifier
        season: Season year (e.g., 2024)
        league: League abbreviation (NBA, NFL, MLB, NHL)
        
    Returns:
        Dictionary containing player statistics
        
    Raises:
        PlayerNotFoundError: If player doesn't exist
        SeasonNotAvailableError: If season data unavailable
    """
    # Implementation here
    pass
```

## Testing

### Running Tests
```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/test_providers.py

# Run with coverage
python -m pytest --cov=pipeline
```

### Writing Tests
- **Unit tests** for all functions
- **Integration tests** for data providers
- **Mock external APIs** to avoid rate limits
- **Test data fixtures** for consistent testing

## Data Sources

### Current Sources
- **NFL**: nflverse submodule + TheSportsDB API
- **NBA**: Historical CSV + TheSportsDB API
- **MLB**: Historical CSV + TheSportsDB API
- **NHL**: Historical CSV + TheSportsDB API

### Adding New Sources
1. Create provider in `pipeline/providers/`
2. Implement `fetch()` method
3. Add data normalization
4. Include tests
5. Update documentation

## Rate Limiting

### API Limits
- **TheSportsDB**: Configurable rate limit (default: 100 req/hour)
- **Local Data**: Unlimited access
- **Caching**: Implemented for frequently accessed data

### Best Practices
- **Batch requests** when possible
- **Cache responses** to minimize API calls
- **Respect rate limits** to maintain API access
- **Use local data** for historical analysis

## Performance

### Optimization Targets
- **Data Loading**: < 5 seconds for 1000 records
- **Query Response**: < 1 second for filtered searches
- **Memory Usage**: < 500MB for typical operations
- **Disk Space**: Efficient storage of historical data

### Monitoring
- **Request timing** logging
- **Memory usage** tracking
- **API call counts** per session
- **Error rate** monitoring

## Data Quality

### Validation Rules
- **Player IDs**: Must be unique within league
- **Team Names**: Standardized across sources
- **Dates**: ISO format validation
- **Scores**: Non-negative integers
- **Statistics**: Reasonable value ranges

### Data Cleaning
- **Remove duplicates** automatically
- **Standardize formats** (dates, names, etc.)
- **Handle missing values** gracefully
- **Validate relationships** between tables

## API Integration

### TheSportsDB API
- **Authentication**: API key required
- **Rate Limiting**: Configurable per hour
- **Endpoints**: Players, teams, games, statistics
- **Data Format**: JSON responses

### Error Handling
- **Network errors**: Retry with exponential backoff
- **Rate limit exceeded**: Queue requests for later
- **Invalid responses**: Log and skip problematic data
- **Authentication failures**: Clear error messages

## Documentation

### Code Documentation
- **README.md**: Project overview and quick start
- **API.md**: Detailed API documentation
- **Data.md**: Data schema and sources
- **Examples.md**: Usage examples and tutorials

### Contributing to Docs
- **Keep examples current** with code changes
- **Include code snippets** for common tasks
- **Document configuration options**
- **Provide troubleshooting guides**

## Deployment

### Local Development
```bash
# Install in development mode
pip install -e .

# Run with live reload
python -m flask run --debug
```

### Production Considerations
- **Database**: Use PostgreSQL for production
- **Caching**: Redis for API response caching
- **Monitoring**: Log aggregation and metrics
- **Backup**: Regular data backups

## Community Guidelines

### Communication
- **Be respectful** and inclusive
- **Ask questions** when unsure
- **Share knowledge** and help others
- **Report issues** with detailed information

### Pull Request Process
1. **Create issue** describing the problem/feature
2. **Fork and branch** from main
3. **Make changes** with tests
4. **Update documentation** as needed
5. **Submit PR** with clear description
6. **Address feedback** promptly

### Code Review
- **Review for correctness** and performance
- **Check test coverage** and quality
- **Verify documentation** updates
- **Ensure backward compatibility**

## Getting Help

### Resources
- **Issues**: GitHub issues for bugs and features
- **Discussions**: GitHub discussions for questions
- **Wiki**: Project wiki for detailed guides
- **Examples**: Code examples in `/examples` directory

### Contact
- **Maintainers**: Check README for contact info
- **Community**: Join our Discord/Slack (if available)
- **Documentation**: Start with README and examples

## Recognition

### Contributors
- **Code contributions** will be acknowledged
- **Documentation improvements** are valued
- **Bug reports** help improve quality
- **Feature suggestions** drive innovation

### Hall of Fame
- **Top contributors** listed in README
- **Special thanks** for major contributions
- **Contributor badges** for different areas

---
🏈🏀⚾🏒
