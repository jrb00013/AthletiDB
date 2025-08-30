import os
import time
import math
import csv
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
import logging
from functools import wraps
import pickle

logger = logging.getLogger(__name__)

# Rate limiting and caching
class RateLimiter:
    """Rate limiter for API calls with configurable limits."""
    
    def __init__(self, requests_per_hour: int = 1800, burst_limit: int = 30):
        self.requests_per_hour = requests_per_hour
        self.burst_limit = burst_limit
        self.request_times = []
        self.last_reset = datetime.now()
    
    def can_make_request(self) -> bool:
        """Check if a request can be made without exceeding rate limits."""
        now = datetime.now()
        
        # Reset counter if hour has passed
        if now - self.last_reset > timedelta(hours=1):
            self.request_times.clear()
            self.last_reset = now
        
        # Check hourly limit
        if len(self.request_times) >= self.requests_per_hour:
            return False
        
        # Check burst limit
        recent_requests = [t for t in self.request_times if now - t < timedelta(minutes=1)]
        if len(recent_requests) >= self.burst_limit:
            return False
        
        return True
    
    def record_request(self):
        """Record that a request was made."""
        self.request_times.append(datetime.now())
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        while not self.can_make_request():
            time.sleep(1)

class Cache:
    """Simple file-based cache for API responses."""
    
    def __init__(self, cache_dir: str = "cache", duration: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.duration = duration
        self.cache_dir.mkdir(exist_ok=True)
    
    def _get_cache_key(self, key: str) -> str:
        """Generate a cache key from a string."""
        return hashlib.md5(key.encode()).hexdigest()
    
    def _get_cache_path(self, key: str) -> Path:
        """Get the cache file path for a key."""
        cache_key = self._get_cache_key(key)
        return self.cache_dir / f"{cache_key}.cache"
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from cache if it exists and is not expired."""
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'rb') as f:
                cached_data = pickle.load(f)
            
            # Check if cache is expired
            if datetime.now() - cached_data['timestamp'] > timedelta(seconds=self.duration):
                cache_path.unlink()  # Remove expired cache
                return None
            
            return cached_data['data']
        
        except Exception as e:
            logger.warning(f"Failed to read cache for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any):
        """Set a value in cache with timestamp."""
        cache_path = self._get_cache_path(key)
        
        try:
            cached_data = {
                'timestamp': datetime.now(),
                'data': value
            }
            
            with open(cache_path, 'wb') as f:
                pickle.dump(cached_data, f)
        
        except Exception as e:
            logger.warning(f"Failed to write cache for key {key}: {e}")
    
    def clear(self):
        """Clear all cached data."""
        for cache_file in self.cache_dir.glob("*.cache"):
            try:
                cache_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to remove cache file {cache_file}: {e}")

# Global instances
rate_limiters = {}
cache = Cache()

def get_rate_limiter(api_name: str) -> RateLimiter:
    """Get or create a rate limiter for a specific API."""
    if api_name not in rate_limiters:
        # Get configuration from environment first, then try config file
        requests_per_hour = int(os.getenv(f"{api_name.upper()}_RATE_LIMIT", 1800))
        burst_limit = int(os.getenv(f"{api_name.upper()}_BURST_LIMIT", 30))
        
        # Try to load from config.yaml if environment variables not set
        if requests_per_hour == 1800:  # Default value, try config file
            try:
                import yaml
                config_path = Path("config.yaml")
                if config_path.exists():
                    with open(config_path, 'r') as f:
                        config = yaml.safe_load(f)
                    
                    rate_limiting_config = config.get("rate_limiting", {})
                    api_config = rate_limiting_config.get(api_name, {})
                    
                    if api_config.get("requests_per_hour"):
                        requests_per_hour = api_config["requests_per_hour"]
                    if api_config.get("burst_limit"):
                        burst_limit = api_config["burst_limit"]
            except Exception as e:
                logger.debug(f"Could not load rate limit config from YAML: {e}")
        
        rate_limiters[api_name] = RateLimiter(requests_per_hour, burst_limit)
    
    return rate_limiters[api_name]

def rate_limited(api_name: str):
    """Decorator to apply rate limiting to API calls."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter = get_rate_limiter(api_name)
            limiter.wait_if_needed()
            limiter.record_request()
            return func(*args, **kwargs)
        return wrapper
    return decorator

def cached(api_name: str, key_func=None):
    """Decorator to cache API responses."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = f"{api_name}:{key_func(*args, **kwargs)}"
            else:
                cache_key = f"{api_name}:{func.__name__}:{hash(str(args) + str(kwargs))}"
            
            # Try to get from cache first
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_result
            
            # Call function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            logger.debug(f"Cached result for {cache_key}")
            
            return result
        return wrapper
    return decorator

# Enhanced utility functions
def envget(key: str, default: str | None = None) -> str | None:
    """Get environment variable with fallback to default."""
    val = os.getenv(key)
    return val if val not in ("", None) else default

def ensure_dir(path: str | Path) -> None:
    """Ensure directory exists, create if it doesn't."""
    Path(path).mkdir(parents=True, exist_ok=True)

def export_csv(rows: List[Dict[str, Any]], out_dir: str, league: str, 
               include_metadata: bool = True) -> str | None:
    """Export rows to CSV file with optional metadata."""
    if not rows:
        return None
    
    ensure_dir(out_dir)
    df = pd.DataFrame(rows)
    
    # Add metadata if requested
    if include_metadata:
        metadata = {
            'export_date': datetime.now().isoformat(),
            'league': league,
            'record_count': len(rows),
            'columns': list(df.columns)
        }
        
        # Create metadata file
        metadata_path = Path(out_dir) / f"{league}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    # Export CSV
    p = Path(out_dir) / f"{league}_players.csv"
    df.to_csv(p, index=False)
    return str(p)

def export_upsets_csv(upsets: List[Dict[str, Any]], out_dir: str, 
                      league: str = None, include_metadata: bool = True) -> str | None:
    """Export upsets to CSV file with enhanced formatting."""
    if not upsets:
        return None
    
    ensure_dir(out_dir)
    df = pd.DataFrame(upsets)
    
    # Add metadata if requested
    if include_metadata:
        metadata = {
            'export_date': datetime.now().isoformat(),
            'league': league or 'all',
            'upset_count': len(upsets),
            'upset_types': df['upset_type'].value_counts().to_dict(),
            'columns': list(df.columns)
        }
        
        metadata_path = Path(out_dir) / f"{league or 'all'}_upsets_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    # Export CSV
    if league:
        filename = f"{league}_upsets.csv"
    else:
        filename = "all_upsets.csv"
    
    p = Path(out_dir) / filename
    df.to_csv(p, index=False)
    return str(p)

def export_json(data: List[Dict[str, Any]], out_dir: str, filename: str,
                include_metadata: bool = True) -> str | None:
    """Export data to JSON file with optional metadata."""
    if not data:
        return None
    
    ensure_dir(out_dir)
    
    export_data = {
        'data': data,
        'export_info': {
            'export_date': datetime.now().isoformat(),
            'record_count': len(data),
            'schema_version': '1.0'
        }
    }
    
    if include_metadata:
        export_data['export_info']['columns'] = list(data[0].keys()) if data else []
    
    p = Path(out_dir) / filename
    with open(p, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    return str(p)

def export_excel(data: List[Dict[str, Any]], out_dir: str, filename: str,
                 sheet_name: str = "Data", include_metadata: bool = True) -> str | None:
    """Export data to Excel file with optional metadata."""
    if not data:
        return None
    
    ensure_dir(out_dir)
    df = pd.DataFrame(data)
    
    p = Path(out_dir) / filename
    
    with pd.ExcelWriter(p, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        if include_metadata:
            metadata_df = pd.DataFrame([{
                'Export Date': datetime.now().isoformat(),
                'Record Count': len(data),
                'Columns': ', '.join(df.columns)
            }])
            metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
    
    return str(p)

def paginate(total: int, per_page: int) -> range:
    """Calculate pagination range."""
    pages = math.ceil(total / per_page) if per_page else 1
    return range(1, pages + 1)

def polite_delay(seconds: float = 0.2) -> None:
    """Delay execution to be polite to APIs."""
    time.sleep(seconds)

def format_upset_summary(upset: Dict[str, Any]) -> str:
    """Format upset data into a readable summary."""
    league = upset.get('league', 'Unknown')
    winner = upset.get('winner', 'Unknown')
    loser = upset.get('loser', 'Unknown')
    upset_type = upset.get('upset_type', 'Unknown')
    reason = upset.get('upset_reason', 'No reason provided')
    magnitude = upset.get('upset_magnitude', 0)
    
    return f"[{league}] {winner} upset {loser} ({upset_type}, magnitude: {magnitude:.1f}): {reason}"

def calculate_upset_magnitude(
    point_spread: float = None,
    odds: float = None,
    score_differential: int = None,
    historical_context: bool = False
) -> float:
    """Calculate the magnitude of an upset based on various factors."""
    magnitude = 0.0
    
    if point_spread is not None:
        magnitude += abs(point_spread)
    
    if odds is not None and odds > 1.0:
        magnitude += (odds - 1.0) * 10  # Scale odds impact
    
    if score_differential is not None:
        # Lower differential = higher upset (closer game than expected)
        magnitude += max(0, 10 - score_differential)
    
    if historical_context:
        magnitude += 5.0  # Bonus for historical significance
    
    return magnitude

def validate_team_names(team1: str, team2: str) -> bool:
    """Basic validation that team names are different and not empty."""
    return (
        team1 and team2 and 
        team1.strip() != team2.strip() and
        len(team1.strip()) > 1 and 
        len(team2.strip()) > 1
    )

def validate_player_data(player_data: Dict[str, Any]) -> List[str]:
    """Validate player data and return list of validation errors."""
    errors = []
    
    required_fields = ['id', 'full_name', 'league']
    for field in required_fields:
        if not player_data.get(field):
            errors.append(f"Missing required field: {field}")
    
    # Validate numeric fields
    numeric_fields = ['height_cm', 'weight_kg', 'rookie_year', 'experience_years']
    for field in numeric_fields:
        value = player_data.get(field)
        if value is not None:
            if not isinstance(value, (int, float)) or value < 0:
                errors.append(f"Invalid {field}: must be non-negative number")
    
    # Validate league
    valid_leagues = ['NBA', 'NFL', 'MLB', 'NHL']
    if player_data.get('league') not in valid_leagues:
        errors.append(f"Invalid league: must be one of {valid_leagues}")
    
    return errors

def clean_team_name(team_name: str) -> str:
    """Standardize team names across different sources."""
    if not team_name:
        return team_name
    
    # Common abbreviations and variations
    team_mappings = {
        'la': 'Los Angeles',
        'ny': 'New York',
        'sf': 'San Francisco',
        'kc': 'Kansas City',
        'tb': 'Tampa Bay',
        'gb': 'Green Bay',
        'ne': 'New England',
        'lv': 'Las Vegas',
        'laa': 'Los Angeles Angels',
        'lad': 'Los Angeles Dodgers',
        'lal': 'Los Angeles Lakers',
        'lac': 'Los Angeles Clippers'
    }
    
    team_lower = team_name.lower().strip()
    
    # Check for exact matches
    if team_lower in team_mappings:
        return team_mappings[team_lower]
    
    # Handle common patterns
    if team_lower.startswith('la '):
        return 'Los Angeles ' + team_name[3:]
    
    # Return original if no mapping found
    return team_name.strip()

def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def get_file_size_mb(file_path: str) -> float:
    """Get file size in megabytes."""
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except OSError:
        return 0.0

def compress_data(data: List[Dict[str, Any]]) -> bytes:
    """Compress data using gzip compression."""
    import gzip
    json_str = json.dumps(data)
    return gzip.compress(json_str.encode('utf-8'))

def decompress_data(compressed_data: bytes) -> List[Dict[str, Any]]:
    """Decompress data using gzip decompression."""
    import gzip
    json_str = gzip.decompress(compressed_data).decode('utf-8')
    return json.loads(json_str)
