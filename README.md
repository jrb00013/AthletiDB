# 🏈🏀⚾🏒 Enhanced Sports Data Pipeline

A comprehensive, production-ready sports data analysis platform that pulls data from multiple sources, tracks upsets, monitors injuries, analyzes team performance, and provides deep insights across NBA, NFL, MLB, and NHL.

## **New in v2.0 - Major Enhancements**

### **Multi-Source Data Architecture**
- **TheSportsDB API**: Live, up-to-date data with rate limiting
- **NFLverse Git Submodule**: Comprehensive NFL play-by-play data
- **Historical CSV Support**: Local data for offline analysis
- **Legacy Provider Support**: Backward compatibility with existing APIs

### **Injury Tracking & Analysis**
- Real-time injury monitoring across all leagues
- Severity classification (questionable, doubtful, out, IR)
- Historical injury patterns and recovery analysis
- Impact assessment on team performance

### **Enhanced Team Analytics**
- Comprehensive team records and standings
- Performance trend analysis
- Strength/weakness identification
- Playoff and championship tracking

### **Advanced Rate Limiting & Caching**
- Configurable API rate limits per source
- Intelligent caching with expiration
- Request queuing and burst protection
- Local data prioritization for historical analysis

### **Enhanced Upset Detection**
- Multi-factor upset analysis (spread, odds, performance, historical)
- League-specific upset definitions
- Magnitude scoring and ranking
- Context-aware detection (weather, venue, attendance)

## **Architecture Overview**

## **Quick Start**

### 1. **Setup Environment**
```bash
# Clone with submodules
git clone --recursive https://github.com/yourusername/sports-data-pipeline.git
cd sports-data-pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp env.example .env
# Edit .env with your TheSportsDB API key
```

### 2. **Configure Data Sources**
```yaml
# config.yaml
data_sources:
  nfl:
    primary: "nflverse"      # Git submodule
    live: "thesportsdb"      # Live API
  nba:
    primary: "thesportsdb"   # Live API
    historical: "local_csv"  # Local data
```

### 3. **Run the Pipeline**
```bash
# Basic run
python main.py

# With specific options
python main.py --league nfl --source live --include-upsets --include-injuries

# Show status
python main.py --show-status

# Use enhanced CLI
python ui/cli.py upsets list --league nfl --format table
```

## **Enhanced CLI Commands**

### **Player Management**
```bash
# Fetch players from specific source
python ui/cli.py players fetch --league nfl --source live

# Export in multiple formats
python ui/cli.py players export --league nba --format excel

# Filter and analyze
python ui/cli.py players list --team "Lakers" --active-only
```

### ** Upset Analysis**
```bash
# List recent upsets
python ui/cli.py upsets list --league nfl --limit 20

# Show statistics
python ui/cli.py upsets stats --league nba

# Export analysis
python ui/cli.py upsets export --format json
```

### **Injury Tracking**
```bash
# Monitor active injuries
python ui/cli.py injuries list --league nfl --severity out

# Team-specific injury report
python ui/cli.py injuries list --team "Patriots"

# Export injury data
python ui/cli.py injuries export --format csv
```

### **Team Analytics**
```bash
# View team records
python ui/cli.py teams records --league nba --season 2024

# Performance analysis
python ui/cli.py teams analyze --league mlb --team "Yankees"

# Export standings
python ui/cli.py teams export --format excel
```

### **Data Analysis**
```bash
# Trend analysis
python ui/cli.py analysis trends --league nfl --season 2024

# Generate predictions
python ui/cli.py analysis predictions --league nba

# Data validation
python ui/cli.py data validate --league nhl
```

## 🔧 **Configuration Options**

### **Rate Limiting**
```yaml
rate_limiting:
  thesportsdb:
    requests_per_hour: 1800  # 30 requests per minute (free tier)
    burst_limit: 10
    cooldown_period: 3600
```

### **Data Quality**
```yaml
data_quality:
  validate_player_ids: true
  standardize_team_names: true
  handle_missing_data: true
  duplicate_detection: true
```

### **Performance Settings**
```yaml
performance:
  batch_size: 1000
  max_concurrent_requests: 5
  cache_duration: 3600
  memory_limit_mb: 500
```

## **Data Outputs**

### **Database Tables**
- **`players`**: Enhanced player profiles with draft info, experience
- **`upsets`**: Multi-factor upset analysis with context
- **`injuries`**: Comprehensive injury tracking and analysis
- **`team_records`**: Detailed team performance metrics
- **`games`**: Game results with detailed statistics
- **`team_analysis`**: Strength/weakness identification

### **Export Formats**
- **CSV**: Standard data export with metadata
- **JSON**: Structured data with schema information
- **Excel**: Multi-sheet exports with formatting
- **Compressed**: Gzip compression for large datasets

### **Metadata & Validation**
- Export timestamps and record counts
- Data quality validation reports
- Schema version tracking
- Source attribution and lineage

## **API Integration**

### **TheSportsDB API**
- **Authentication**: API key required
- **Rate Limits**: Configurable per hour
- **Endpoints**: Players, teams, games, statistics
- **Data Format**: JSON with comprehensive metadata

### **NFLverse Integration**
- **Git Submodule**: Automatic updates
- **Data Types**: Play-by-play, player stats, team data
- **Format**: RDS files with Python/R support
- **Updates**: Automatic via GitHub releases

## **Performance & Scalability**

### **Optimization Features**
- **Intelligent Caching**: API response caching with expiration
- **Batch Processing**: Efficient bulk data operations
- **Memory Management**: Configurable memory limits
- **Concurrent Processing**: Parallel data fetching

### **Monitoring & Metrics**
- **Request Timing**: Performance tracking
- **Memory Usage**: Resource monitoring
- **API Call Counts**: Usage analytics
- **Error Rates**: Quality monitoring

## **Testing & Quality**

### **Test Coverage**
```bash
# Run all tests
python -m pytest

# With coverage
python -m pytest --cov=pipeline

# Specific test categories
python -m pytest tests/test_providers.py
python -m pytest tests/test_analysis.py
```

### **Data Validation**
- **Schema Validation**: Pydantic model validation
- **Data Quality Checks**: Missing data detection
- **Relationship Validation**: Foreign key integrity
- **Format Validation**: Date, number, text validation

## **Deployment & Production**

### **Environment Setup**
```bash
# Production environment
export THESPORTSDB_API_KEY="your_key"
export DATABASE_URL="postgresql://user:pass@host/db"
export LOG_LEVEL="INFO"
export CACHE_DURATION="7200"
```

### **Cron Jobs**
```bash
# Daily player updates
0 6 * * * cd /path/to/pipeline && python main.py --include-upsets --include-injuries

# Hourly upset tracking
0 * * * * cd /path/to/pipeline && python main.py --upsets-only

# Weekly analysis
0 2 * * 0 cd /path/to/pipeline && python main.py --show-stats
```

## **Contributing**

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### **Areas for Contribution**
- **New Data Sources**: Additional sports leagues
- **Analysis Features**: Advanced statistical models
- **UI Enhancements**: Web dashboard, mobile app
- **Performance**: Optimization and scaling
- **Documentation**: Examples and tutorials

##**Documentation & Resources**

- **API Reference**: [API.md](docs/API.md)
- **Data Schema**: [Data.md](docs/Data.md)
- **Examples**: [Examples.md](docs/Examples.md)
- **Troubleshooting**: [Troubleshooting.md](docs/Troubleshooting.md)

## **Support & Community**

- **Issues**: [GitHub Issues](https://github.com/yourusername/sports-data-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/sports-data-pipeline/discussions)
- **Wiki**: [Project Wiki](https://github.com/yourusername/sports-data-pipeline/wiki)

## **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## **Acknowledgments**

- **TheSportsDB**: Comprehensive sports data API
- **NFLverse**: NFL play-by-play data and analysis
- **Open Source Community**: Contributors and maintainers

---

**🏈🏀⚾🏒**

*Built with for sports analytics enthusiasts.*
