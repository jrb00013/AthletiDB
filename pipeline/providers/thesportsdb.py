#!/usr/bin/env python3
"""
TheSportsDB API Provider
Provides access to comprehensive sports data with rate limiting and caching.
"""

import os
import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from ..utils import rate_limited, cached, get_rate_limiter, cache
from ..normalize import Player, Injury, TeamRecord, Game, PlayerStats

logger = logging.getLogger(__name__)

class TheSportsDBProvider:
    """Provider for TheSportsDB API with rate limiting and caching."""
    
    def __init__(self):
        self.api_key = os.getenv("THESPORTSDB_API_KEY")
        self.base_url = os.getenv("THESPORTSDB_BASE_URL", "https://www.thesportsdb.com/api/v1/json")
        self.rate_limiter = get_rate_limiter("thesportsdb")
        
        if not self.api_key:
            logger.warning("No TheSportsDB API key found. Live data will not be available.")
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make a rate-limited request to the API."""
        if not self.api_key:
            logger.error("Cannot make request without API key")
            return None
        
        # Wait for rate limiter
        self.rate_limiter.wait_if_needed()
        
        # Prepare request
        url = f"{self.base_url}/{endpoint}"
        request_params = {"apikey": self.api_key}
        if params:
            request_params.update(params)
        
        try:
            response = requests.get(url, params=request_params, timeout=30)
            response.raise_for_status()
            
            # Record the request
            self.rate_limiter.record_request()
            
            data = response.json()
            if data.get("error"):
                logger.error(f"API error: {data['error']}")
                return None
            
            return data
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    @cached("thesportsdb", lambda self, league: f"players_{league}")
    def fetch_players(self, league: str, season: int = None) -> List[Dict[str, Any]]:
        """Fetch players for a specific league."""
        logger.info(f"Fetching {league.upper()} players from TheSportsDB...")
        
        # Map league names to API format
        league_mapping = {
            'nfl': 'American%20football_nfl',
            'nba': 'Basketball_nba',
            'mlb': 'Baseball_mlb',
            'nhl': 'Ice%20hockey_nhl'
        }
        
        api_league = league_mapping.get(league.lower())
        if not api_league:
            logger.error(f"Unsupported league: {league}")
            return []
        
        endpoint = f"search_all_players.php"
        params = {"l": api_league}
        
        if season:
            params["s"] = season
        
        data = self._make_request(endpoint, params)
        if not data or "player" not in data:
            logger.warning(f"No player data found for {league}")
            return []
        
        players = []
        for player_data in data["player"]:
            try:
                player = self._normalize_player(player_data, league)
                if player:
                    players.append(player)
            except Exception as e:
                logger.warning(f"Failed to normalize player {player_data.get('idPlayer')}: {e}")
                continue
        
        logger.info(f"Successfully fetched {len(players)} {league.upper()} players")
        return players
    
    def _normalize_player(self, player_data: Dict[str, Any], league: str) -> Optional[Dict[str, Any]]:
        """Normalize player data from TheSportsDB format."""
        try:
            # Extract basic information
            player_id = player_data.get("idPlayer")
            if not player_id:
                return None
            
            # Parse name
            full_name = player_data.get("strPlayer", "").strip()
            if not full_name:
                return None
            
            # Split name into first and last
            name_parts = full_name.split()
            first_name = name_parts[0] if name_parts else None
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else None
            
            # Parse date of birth
            birthdate = player_data.get("dateBorn")
            if birthdate and birthdate != "0000-00-00":
                try:
                    # Validate date format
                    datetime.strptime(birthdate, "%Y-%m-%d")
                except ValueError:
                    birthdate = None
            
            # Parse height and weight
            height_str = player_data.get("strHeight")
            height_cm = None
            if height_str:
                try:
                    # Convert feet'inches" to cm
                    if '"' in height_str and "'" in height_str:
                        feet, inches = height_str.replace('"', '').split("'")
                        height_cm = (int(feet) * 12 + int(inches)) * 2.54
                except (ValueError, TypeError):
                    pass
            
            weight_str = player_data.get("strWeight")
            weight_kg = None
            if weight_str:
                try:
                    # Convert lbs to kg
                    weight_kg = float(weight_str.replace("lbs", "").strip()) * 0.453592
                except (ValueError, TypeError):
                    pass
            
            # Create normalized player data
            normalized_player = {
                "id": f"thesportsdb_{league}_{player_id}",
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "league": league.upper(),
                "team": player_data.get("strTeam"),
                "team_id": player_data.get("idTeam"),
                "position": player_data.get("strPosition"),
                "jersey": player_data.get("strNumber"),
                "nationality": player_data.get("strNationality"),
                "birthdate": birthdate,
                "height_cm": height_cm,
                "weight_kg": weight_kg,
                "active": True,  # Assume active if we're getting data
                "rookie_year": None,  # Not provided by this API
                "experience_years": None,  # Not provided by this API
                "college": player_data.get("strCollege"),
                "draft_round": None,  # Not provided by this API
                "draft_pick": None,  # Not provided by this API
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"
            }
            
            return normalized_player
        
        except Exception as e:
            logger.error(f"Error normalizing player data: {e}")
            return None
    
    @cached("thesportsdb", lambda self, league: f"teams_{league}")
    def fetch_teams(self, league: str) -> List[Dict[str, Any]]:
        """Fetch teams for a specific league."""
        logger.info(f"Fetching {league.upper()} teams from TheSportsDB...")
        
        league_mapping = {
            'nfl': 'American%20football_nfl',
            'nba': 'Basketball_nba',
            'mlb': 'Baseball_mlb',
            'nhl': 'Ice%20hockey_nhl'
        }
        
        api_league = league_mapping.get(league.lower())
        if not api_league:
            logger.error(f"Unsupported league: {league}")
            return []
        
        endpoint = f"search_all_teams.php"
        params = {"l": api_league}
        
        data = self._make_request(endpoint, params)
        if not data or "teams" not in data:
            logger.warning(f"No team data found for {league}")
            return []
        
        teams = []
        for team_data in data["teams"]:
            try:
                team = self._normalize_team(team_data, league)
                if team:
                    teams.append(team)
            except Exception as e:
                logger.warning(f"Failed to normalize team {team_data.get('idTeam')}: {e}")
                continue
        
        logger.info(f"Successfully fetched {len(teams)} {league.upper()} teams")
        return teams
    
    def _normalize_team(self, team_data: Dict[str, Any], league: str) -> Optional[Dict[str, Any]]:
        """Normalize team data from TheSportsDB format."""
        try:
            team_id = team_data.get("idTeam")
            if not team_id:
                return None
            
            normalized_team = {
                "id": f"thesportsdb_{league}_{team_id}",
                "name": team_data.get("strTeam"),
                "league": league.upper(),
                "alternate_name": team_data.get("strAlternate"),
                "formed_year": team_data.get("intFormedYear"),
                "league_name": team_data.get("strLeague"),
                "division": team_data.get("strDivision"),
                "stadium": team_data.get("strStadium"),
                "capacity": team_data.get("intStadiumCapacity"),
                "website": team_data.get("strWebsite"),
                "facebook": team_data.get("strFacebook"),
                "twitter": team_data.get("strTwitter"),
                "instagram": team_data.get("strInstagram"),
                "description": team_data.get("strDescriptionEN"),
                "country": team_data.get("strCountry"),
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"
            }
            
            return normalized_team
        
        except Exception as e:
            logger.error(f"Error normalizing team data: {e}")
            return None
    
    @cached("thesportsdb", lambda self, league, season: f"games_{league}_{season}")
    def fetch_games(self, league: str, season: int) -> List[Dict[str, Any]]:
        """Fetch games for a specific league and season."""
        logger.info(f"Fetching {league.upper()} games for season {season} from TheSportsDB...")
        
        league_mapping = {
            'nfl': 'American%20football_nfl',
            'nba': 'Basketball_nba',
            'mlb': 'Baseball_mlb',
            'nhl': 'Ice%20hockey_nhl'
        }
        
        api_league = league_mapping.get(league.lower())
        if not api_league:
            logger.error(f"Unsupported league: {league}")
            return []
        
        endpoint = f"eventsseason.php"
        params = {"id": api_league, "s": season}
        
        data = self._make_request(endpoint, params)
        if not data or "events" not in data:
            logger.warning(f"No game data found for {league} season {season}")
            return []
        
        games = []
        for game_data in data["events"]:
            try:
                game = self._normalize_game(game_data, league, season)
                if game:
                    games.append(game)
            except Exception as e:
                logger.warning(f"Failed to normalize game {game_data.get('idEvent')}: {e}")
                continue
        
        logger.info(f"Successfully fetched {len(games)} {league.upper()} games for season {season}")
        return games
    
    def _normalize_game(self, game_data: Dict[str, Any], league: str, season: int) -> Optional[Dict[str, Any]]:
        """Normalize game data from TheSportsDB format."""
        try:
            game_id = game_data.get("idEvent")
            if not game_id:
                return None
            
            # Parse scores
            home_score = None
            away_score = None
            score_str = game_data.get("intHomeScore")
            if score_str and score_str != "0":
                try:
                    home_score = int(score_str)
                except (ValueError, TypeError):
                    pass
            
            score_str = game_data.get("intAwayScore")
            if score_str and score_str != "0":
                try:
                    away_score = int(score_str)
                except (ValueError, TypeError):
                    pass
            
            # Determine game status
            game_status = "scheduled"
            if home_score is not None and away_score is not None:
                game_status = "final"
            
            normalized_game = {
                "id": f"thesportsdb_{league}_{game_id}",
                "league": league.upper(),
                "season": season,
                "game_date": game_data.get("dateEvent"),
                "home_team": game_data.get("strHomeTeam"),
                "away_team": game_data.get("strAwayTeam"),
                "home_score": home_score,
                "away_score": away_score,
                "venue": game_data.get("strVenue"),
                "game_status": game_status,
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"
            }
            
            return normalized_game
        
        except Exception as e:
            logger.error(f"Error normalizing game data: {e}")
            return None
    
    def fetch_injuries(self, league: str, team: str = None) -> List[Dict[str, Any]]:
        """Fetch injury data for a specific league."""
        logger.info(f"Fetching {league.upper()} injuries from TheSportsDB...")
        
        # Note: TheSportsDB may not have comprehensive injury data
        # This is a placeholder for future implementation
        logger.warning("Injury data not yet implemented for TheSportsDB")
        return []
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limit status."""
        return {
            "api_name": "thesportsdb",
            "requests_per_hour": self.rate_limiter.requests_per_hour,
            "burst_limit": self.rate_limiter.burst_limit,
            "current_requests": len(self.rate_limiter.request_times),
            "can_make_request": self.rate_limiter.can_make_request(),
            "last_reset": self.rate_limiter.last_reset.isoformat()
        }

# Global provider instance
thesportsdb_provider = TheSportsDBProvider()

# Convenience functions for external use
def fetch_players(league: str, season: int = None) -> List[Dict[str, Any]]:
    """Fetch players from TheSportsDB."""
    return thesportsdb_provider.fetch_players(league, season)

def fetch_teams(league: str) -> List[Dict[str, Any]]:
    """Fetch teams from TheSportsDB."""
    return thesportsdb_provider.fetch_teams(league)

def fetch_games(league: str, season: int) -> List[Dict[str, Any]]:
    """Fetch games from TheSportsDB."""
    return thesportsdb_provider.fetch_games(league, season)

def get_rate_limit_status() -> Dict[str, Any]:
    """Get TheSportsDB rate limit status."""
    return thesportsdb_provider.get_rate_limit_status()
