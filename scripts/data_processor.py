#!/usr/bin/env python3
"""
Sports Data Pipeline - Advanced Data Processor
Comprehensive data processing, analysis, and transformation utilities.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.db import get_engine
from pipeline.normalize import Player, Upset, Injury, TeamRecord, Game, PlayerStats, TeamAnalysis
from pipeline.utils import RateLimiter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdvancedDataProcessor:
    """Advanced data processing and analysis engine."""
    
    def __init__(self, db_url: str = "sqlite:///sports_data.db"):
        """Initialize the data processor."""
        self.db_url = db_url
        self.engine = get_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
    def process_player_trends(self, league: str, days: int = 30) -> pd.DataFrame:
        """Analyze player performance trends over time."""
        logger.info(f"Processing player trends for {league} over {days} days")
        
        query = """
        SELECT 
            p.name,
            p.team,
            p.position,
            ps.points,
            ps.rebounds,
            ps.assists,
            ps.steals,
            ps.blocks,
            ps.games_played,
            ps.date,
            ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY ps.date) as game_number
        FROM players p
        JOIN player_stats ps ON p.id = ps.player_id
        WHERE p.league = :league 
        AND ps.date >= date('now', '-' || :days || ' days')
        ORDER BY p.name, ps.date
        """
        
        try:
            df = pd.read_sql_query(
                query, 
                self.engine, 
                params={'league': league, 'days': days}
            )
            
            if df.empty:
                logger.warning(f"No player stats found for {league}")
                return pd.DataFrame()
            
            # Calculate rolling averages
            df['rolling_3_game_avg'] = df.groupby('name')['points'].rolling(3, min_periods=1).mean().reset_index(0, drop=True)
            df['rolling_5_game_avg'] = df.groupby('name')['points'].rolling(5, min_periods=1).mean().reset_index(0, drop=True)
            
            # Calculate trend direction
            df['trend_3_game'] = df.groupby('name')['rolling_3_game_avg'].diff()
            df['trend_5_game'] = df.groupby('name')['rolling_5_game_avg'].diff()
            
            # Add trend classification
            df['trend_direction'] = df['trend_3_game'].apply(
                lambda x: 'Improving' if x > 0 else 'Declining' if x < 0 else 'Stable'
            )
            
            logger.info(f"Processed {len(df)} player trend records for {league}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing player trends: {e}")
            return pd.DataFrame()
    
    def analyze_team_performance(self, league: str, season: Optional[int] = None) -> Dict[str, Any]:
        """Analyze comprehensive team performance metrics."""
        logger.info(f"Analyzing team performance for {league}")
        
        try:
            # Get team records
            records_query = """
            SELECT team, wins, losses, win_percentage, points_for, points_against
            FROM team_records 
            WHERE league = :league
            """
            if season:
                records_query += " AND season = :season"
            
            records_df = pd.read_sql_query(
                records_query, 
                self.engine, 
                params={'league': league, 'season': season} if season else {'league': league}
            )
            
            # Get recent games
            games_query = """
            SELECT 
                home_team, away_team, home_score, away_score, date,
                CASE 
                    WHEN home_score > away_score THEN home_team 
                    ELSE away_team 
                END as winner,
                ABS(home_score - away_score) as margin
            FROM games 
            WHERE league = :league
            ORDER BY date DESC
            LIMIT 100
            """
            
            games_df = pd.read_sql_query(
                games_query, 
                self.engine, 
                params={'league': league}
            )
            
            if records_df.empty or games_df.empty:
                logger.warning(f"Insufficient data for team analysis in {league}")
                return {}
            
            # Calculate advanced metrics
            analysis = {
                'league': league,
                'season': season,
                'total_teams': len(records_df),
                'teams': {}
            }
            
            for _, team_record in records_df.iterrows():
                team = team_record['team']
                
                # Get team's recent games
                team_games = games_df[
                    (games_df['home_team'] == team) | 
                    (games_df['away_team'] == team)
                ].head(10)
                
                if team_games.empty:
                    continue
                
                # Calculate recent performance
                team_wins = len(team_games[team_games['winner'] == team])
                team_losses = len(team_games) - team_wins
                recent_win_pct = team_wins / len(team_games) if len(team_games) > 0 else 0
                
                # Calculate scoring trends
                team_scores = []
                for _, game in team_games.iterrows():
                    if game['home_team'] == team:
                        team_scores.append(game['home_score'])
                    else:
                        team_scores.append(game['away_score'])
                
                avg_score = np.mean(team_scores) if team_scores else 0
                score_volatility = np.std(team_scores) if len(team_scores) > 1 else 0
                
                # Determine team strength areas
                strengths = []
                if team_record['win_percentage'] > 0.6:
                    strengths.append('High Win Rate')
                if team_record['points_for'] > records_df['points_for'].mean():
                    strengths.append('High Scoring')
                if team_record['points_against'] < records_df['points_against'].mean():
                    strengths.append('Strong Defense')
                if recent_win_pct > team_record['win_percentage']:
                    strengths.append('Improving Form')
                
                analysis['teams'][team] = {
                    'overall_record': {
                        'wins': team_record['wins'],
                        'losses': team_record['losses'],
                        'win_percentage': team_record['win_percentage']
                    },
                    'recent_form': {
                        'recent_games': len(team_games),
                        'recent_wins': team_wins,
                        'recent_losses': team_losses,
                        'recent_win_percentage': recent_win_pct
                    },
                    'scoring': {
                        'points_for': team_record['points_for'],
                        'points_against': team_record['points_against'],
                        'recent_avg_score': avg_score,
                        'score_volatility': score_volatility
                    },
                    'strengths': strengths,
                    'trend': 'Improving' if recent_win_pct > team_record['win_percentage'] else 'Declining'
                }
            
            logger.info(f"Completed team performance analysis for {league}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing team performance: {e}")
            return {}
    
    def detect_upset_patterns(self, league: str, min_upsets: int = 5) -> Dict[str, Any]:
        """Analyze patterns in upsets to identify predictive factors."""
        logger.info(f"Detecting upset patterns for {league}")
        
        try:
            # Get upsets with game context
            query = """
            SELECT 
                u.*,
                g.home_team, g.away_team, g.home_score, g.away_score,
                g.home_odds, g.away_odds, g.spread,
                p1.name as player1_name, p1.position as player1_position,
                p2.name as player2_name, p2.position as player2_position
            FROM upsets u
            JOIN games g ON u.game_id = g.id
            LEFT JOIN players p1 ON u.key_player1_id = p1.id
            LEFT JOIN players p2 ON u.key_player2_id = p2.id
            WHERE u.league = :league
            ORDER BY u.date DESC
            """
            
            df = pd.read_sql_query(query, self.engine, params={'league': league})
            
            if len(df) < min_upsets:
                logger.warning(f"Insufficient upsets ({len(df)}) for pattern analysis in {league}")
                return {}
            
            # Analyze upset characteristics
            patterns = {
                'league': league,
                'total_upsets': len(df),
                'upset_types': {},
                'common_factors': {},
                'time_patterns': {},
                'team_patterns': {}
            }
            
            # Upset type distribution
            upset_types = df['upset_type'].value_counts()
            patterns['upset_types'] = upset_types.to_dict()
            
            # Common factors
            factors = []
            for _, row in df.iterrows():
                if row['point_spread'] > 10:
                    factors.append('Large Point Spread')
                if row['odds_difference'] > 200:
                    factors.append('High Odds Difference')
                if row['score_differential'] > 20:
                    factors.append('High Score Differential')
                if pd.notna(row['key_player1_id']):
                    factors.append('Key Player Performance')
            
            factor_counts = pd.Series(factors).value_counts()
            patterns['common_factors'] = factor_counts.to_dict()
            
            # Time patterns
            df['hour'] = pd.to_datetime(df['date']).dt.hour
            hour_counts = df['hour'].value_counts().sort_index()
            patterns['time_patterns'] = hour_counts.to_dict()
            
            # Team patterns
            upset_teams = []
            for _, row in df.iterrows():
                if row['home_score'] > row['away_score']:
                    upset_teams.append(row['away_team'])  # Away team won (upset)
                else:
                    upset_teams.append(row['home_team'])  # Home team won (upset)
            
            team_counts = pd.Series(upset_teams).value_counts()
            patterns['team_patterns'] = team_counts.head(10).to_dict()
            
            # Predictive insights
            insights = []
            if len(df) > 10:
                avg_spread = df['point_spread'].mean()
                avg_odds_diff = df['odds_difference'].mean()
                
                insights.append(f"Average point spread in upsets: {avg_spread:.1f}")
                insights.append(f"Average odds difference in upsets: {avg_odds_diff:.0f}")
                
                if hour_counts.max() > len(df) * 0.3:
                    peak_hour = hour_counts.idxmax()
                    insights.append(f"Peak upset hour: {peak_hour}:00")
                
                if team_counts.max() > len(df) * 0.2:
                    upset_prone_team = team_counts.idxmax()
                    insights.append(f"Most upset-prone team: {upset_prone_team}")
            
            patterns['insights'] = insights
            
            logger.info(f"Completed upset pattern analysis for {league}")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting upset patterns: {e}")
            return {}
    
    def generate_player_rankings(self, league: str, metric: str = 'points', limit: int = 50) -> pd.DataFrame:
        """Generate player rankings based on various metrics."""
        logger.info(f"Generating {metric} rankings for {league}")
        
        valid_metrics = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'games_played']
        if metric not in valid_metrics:
            logger.error(f"Invalid metric: {metric}. Valid metrics: {valid_metrics}")
            return pd.DataFrame()
        
        try:
            query = f"""
            SELECT 
                p.name,
                p.team,
                p.position,
                p.active,
                SUM(ps.{metric}) as total_{metric},
                AVG(ps.{metric}) as avg_{metric},
                COUNT(ps.id) as games_played,
                MAX(ps.date) as last_game
            FROM players p
            JOIN player_stats ps ON p.id = ps.player_id
            WHERE p.league = :league
            GROUP BY p.id, p.name, p.team, p.position, p.active
            HAVING games_played >= 5
            ORDER BY total_{metric} DESC
            LIMIT :limit
            """
            
            df = pd.read_sql_query(
                query, 
                self.engine, 
                params={'league': league, 'limit': limit}
            )
            
            if df.empty:
                logger.warning(f"No player rankings data found for {league}")
                return pd.DataFrame()
            
            # Add rankings
            df['rank'] = range(1, len(df) + 1)
            
            # Add performance tiers
            df['tier'] = pd.cut(
                df[f'total_{metric}'], 
                bins=5, 
                labels=['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond']
            )
            
            # Add recent form indicator
            df['last_game'] = pd.to_datetime(df['last_game'])
            df['days_since_last_game'] = (datetime.now() - df['last_game']).dt.days
            df['recent_form'] = df['days_since_last_game'].apply(
                lambda x: 'Active' if x <= 7 else 'Recent' if x <= 30 else 'Inactive'
            )
            
            logger.info(f"Generated {len(df)} player rankings for {league}")
            return df
            
        except Exception as e:
            logger.error(f"Error generating player rankings: {e}")
            return pd.DataFrame()
    
    def export_analysis_report(self, output_dir: str = "reports", format: str = "excel") -> str:
        """Export comprehensive analysis report."""
        logger.info(f"Exporting analysis report to {output_dir}")
        
        try:
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format.lower() == "excel":
                filename = f"comprehensive_analysis_{timestamp}.xlsx"
                filepath = output_path / filename
                
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    # Player trends for each league
                    for league in ['nfl', 'nba', 'mlb', 'nhl']:
                        trends_df = self.process_player_trends(league, days=30)
                        if not trends_df.empty:
                            trends_df.to_excel(writer, sheet_name=f'{league.upper()}_Trends', index=False)
                    
                    # Team performance analysis
                    for league in ['nfl', 'nba', 'mlb', 'nhl']:
                        team_analysis = self.analyze_team_performance(league)
                        if team_analysis:
                            team_df = pd.DataFrame.from_dict(team_analysis['teams'], orient='index')
                            team_df.to_excel(writer, sheet_name=f'{league.upper()}_Teams')
                    
                    # Upset patterns
                    for league in ['nfl', 'nba', 'mlb', 'nhl']:
                        upset_patterns = self.detect_upset_patterns(league)
                        if upset_patterns:
                            patterns_df = pd.DataFrame([upset_patterns])
                            patterns_df.to_excel(writer, sheet_name=f'{league.upper()}_Upsets', index=False)
                    
                    # Player rankings
                    for league in ['nfl', 'nba', 'mlb', 'nhl']:
                        for metric in ['points', 'rebounds', 'assists']:
                            rankings_df = self.generate_player_rankings(league, metric, limit=25)
                            if not rankings_df.empty:
                                sheet_name = f'{league.upper()}_{metric.title()}_Rankings'
                                rankings_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                logger.info(f"Analysis report exported to {filepath}")
                return str(filepath)
                
            elif format.lower() == "csv":
                # Export each analysis as separate CSV
                for league in ['nfl', 'nba', 'mlb', 'nhl']:
                    # Player trends
                    trends_df = self.process_player_trends(league, days=30)
                    if not trends_df.empty:
                        trends_file = output_path / f"{league}_player_trends_{timestamp}.csv"
                        trends_df.to_csv(trends_file, index=False)
                    
                    # Team performance
                    team_analysis = self.analyze_team_performance(league)
                    if team_analysis:
                        team_df = pd.DataFrame.from_dict(team_analysis['teams'], orient='index')
                        team_file = output_path / f"{league}_team_performance_{timestamp}.csv"
                        team_df.to_csv(team_file)
                
                logger.info(f"Analysis reports exported to {output_path}")
                return str(output_path)
            
            else:
                logger.error(f"Unsupported format: {format}")
                return ""
                
        except Exception as e:
            logger.error(f"Error exporting analysis report: {e}")
            return ""

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Advanced Sports Data Processor")
    parser.add_argument('action', choices=[
        'trends', 'teams', 'upsets', 'rankings', 'export', 'all'
    ], help='Action to perform')
    parser.add_argument('--league', '-l', choices=['nfl', 'nba', 'mlb', 'nhl'], 
                       help='League to analyze')
    parser.add_argument('--output-dir', '-o', default='reports', 
                       help='Output directory for reports')
    parser.add_argument('--format', '-f', choices=['excel', 'csv'], default='excel',
                       help='Output format')
    parser.add_argument('--days', '-d', type=int, default=30,
                       help='Number of days for trend analysis')
    parser.add_argument('--metric', '-m', default='points',
                       help='Metric for player rankings')
    parser.add_argument('--limit', type=int, default=50,
                       help='Limit for player rankings')
    
    args = parser.parse_args()
    
    processor = AdvancedDataProcessor()
    
    if args.action == 'trends':
        if not args.league:
            print("Error: League is required for trends analysis")
            return 1
        
        trends_df = processor.process_player_trends(args.league, args.days)
        if not trends_df.empty:
            print(f"\nPlayer Trends for {args.league.upper()} (Last {args.days} days):")
            print(trends_df.head(10).to_string(index=False))
        else:
            print(f"No trends data available for {args.league}")
    
    elif args.action == 'teams':
        if not args.league:
            print("Error: League is required for team analysis")
            return 1
        
        team_analysis = processor.analyze_team_performance(args.league)
        if team_analysis:
            print(f"\nTeam Performance Analysis for {args.league.upper()}:")
            for team, data in list(team_analysis['teams'].items())[:5]:
                print(f"\n{team}:")
                print(f"  Record: {data['overall_record']['wins']}-{data['overall_record']['losses']}")
                print(f"  Recent Form: {data['recent_form']['recent_win_percentage']:.1%}")
                print(f"  Strengths: {', '.join(data['strengths'])}")
        else:
            print(f"No team data available for {args.league}")
    
    elif args.action == 'upsets':
        if not args.league:
            print("Error: League is required for upset analysis")
            return 1
        
        upset_patterns = processor.detect_upset_patterns(args.league)
        if upset_patterns:
            print(f"\nUpset Patterns for {args.league.upper()}:")
            print(f"Total Upsets: {upset_patterns['total_upsets']}")
            print(f"Most Common Type: {max(upset_patterns['upset_types'].items(), key=lambda x: x[1])[0]}")
            if 'insights' in upset_patterns:
                print("\nKey Insights:")
                for insight in upset_patterns['insights']:
                    print(f"  - {insight}")
        else:
            print(f"No upset data available for {args.league}")
    
    elif args.action == 'rankings':
        if not args.league:
            print("Error: League is required for rankings")
            return 1
        
        rankings_df = processor.generate_player_rankings(args.league, args.metric, args.limit)
        if not rankings_df.empty:
            print(f"\nTop {args.limit} {args.metric.title()} Rankings for {args.league.upper()}:")
            print(rankings_df[['rank', 'name', 'team', 'position', f'total_{args.metric}', 'tier']].head(10).to_string(index=False))
        else:
            print(f"No rankings data available for {args.league}")
    
    elif args.action == 'export':
        filepath = processor.export_analysis_report(args.output_dir, args.format)
        if filepath:
            print(f"Analysis report exported to: {filepath}")
        else:
            print("Failed to export analysis report")
            return 1
    
    elif args.action == 'all':
        print("Running comprehensive analysis...")
        filepath = processor.export_analysis_report(args.output_dir, args.format)
        if filepath:
            print(f"Comprehensive analysis completed and exported to: {filepath}")
        else:
            print("Failed to complete comprehensive analysis")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
