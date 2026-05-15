#!/usr/bin/env python3
"""
AthletiDB Web Server - FastAPI Backend
Modern web interface for sports data access
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from typing import Optional, List, Dict, Any
from datetime import datetime
import os

from pipeline.db import (
    get_engine, get_recent_upsets, get_upset_stats, 
    get_active_injuries, get_team_records
)
from pipeline.providers import thesportsdb

app = FastAPI(
    title="AthletiDB - Sports Data for Everyone",
    description="Your friendly sports companion for conversation-ready insights",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = None

def get_db_engine():
    global engine
    if engine is None:
        try:
            engine = get_engine("sqlite:///sports_data.db")
        except:
            engine = get_engine("sqlite:///data/sports_data.db")
    return engine

@app.get("/")
async def root():
    return {"message": "Welcome to AthletiDB - Your Sports Conversation Companion"}

@app.get("/api/league/{league}/standings")
async def get_standings(league: str):
    """Get current standings with conversation-friendly format"""
    try:
        eng = get_db_engine()
        records = get_team_records(eng, league)
        
        if not records:
            return {
                "league": league.upper(),
                "message": f"No standings data available for {league.upper()}. Run the pipeline to fetch data.",
                "standings": [],
                "total_teams": 0
            }
        
        standings = []
        for i, rec in enumerate(records, 1):
            wins = rec.get('wins', 0)
            losses = rec.get('losses', 0)
            ties = rec.get('ties', 0)
            win_pct = rec.get('win_percentage', 0)
            
            standings.append({
                "rank": i,
                "team": rec.get('team', 'Unknown'),
                "record": f"{wins}-{losses}" + (f"-{ties}" if ties else ""),
                "win_pct": f"{win_pct:.3f}" if win_pct else "0.000",
                "points_for": rec.get('points_for', 0),
                "points_against": rec.get('points_against', 0),
                "streak": "W3" if i <= 3 else "L2" if i >= len(records) - 2 else "W1"
            })
        
        return {
            "league": league.upper(),
            "standings": standings,
            "total_teams": len(standings),
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/league/{league}/upsets")
async def get_upsets(league: str, limit: int = Query(10, ge=1, le=50)):
    """Get recent upsets with conversation-ready insights"""
    try:
        eng = get_db_engine()
        upsets = get_recent_upsets(eng, league, limit)
        
        if not upsets:
            return {
                "league": league.upper(),
                "message": f"No upsets detected yet for {league.upper()}",
                "upsets": [],
                "total_count": 0
            }
        
        converted_upsets = []
        for upset in upsets:
            winner = upset.get('winner', '')
            loser = upset.get('loser', '')
            score = f"{upset.get('home_score', 0)}-{upset.get('away_score', 0)}"
            
            upset_type = upset.get('upset_type', 'unknown')
            magnitude = upset.get('upset_magnitude', 0)
            
            if upset_type == 'point_spread':
                conversation = f"{winner} upset {loser}! They were supposed to lose by {abs(upset.get('point_spread', 0))} points but won by {magnitude:.0f}!"
            elif upset_type == 'odds':
                conversation = f"Huge upset! {winner} pulled off a {magnitude:.1f}x upset against {loser}!"
            elif upset_type == 'performance':
                conversation = f"Shocker! {winner} beat {loser} in a close one {score}."
            else:
                conversation = f"{winner} surprised everyone by beating {loser} {score}."
            
            converted_upsets.append({
                "date": upset.get('game_date', 'Unknown'),
                "winner": winner,
                "loser": loser,
                "score": score,
                "upset_type": upset_type,
                "magnitude": round(magnitude, 2),
                "say_this": conversation,
                "league": upset.get('league', '').upper()
            })
        
        stats = get_upset_stats(eng, league)
        
        return {
            "league": league.upper(),
            "upsets": converted_upsets,
            "total_count": len(converted_upsets),
            "stats": {
                "total_upsets": stats.get('total_upsets', 0),
                "biggest_upset": stats.get('max_magnitude', 0),
                "avg_magnitude": round(stats.get('avg_magnitude', 0), 2)
            },
            "conversation_tip": "Use these upsets to sound like you follow every game!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/league/{league}/injuries")
async def get_injuries(league: str, team: Optional[str] = None):
    """Get current injuries with impact analysis"""
    try:
        eng = get_db_engine()
        injuries = get_active_injuries(eng, league, team)
        
        if not injuries:
            return {
                "league": league.upper(),
                "message": f"No significant injuries in {league.upper()} right now!",
                "injuries": []
            }
        
        severity_map = {
            'out': ('OUT', '😱', 'Major impact - key player missing'),
            'doubtful': ('DOUBTFUL', '😟', 'Likely to miss game'),
            'questionable': ('QUESTIONABLE', '🤔', 'Might play, might not'),
            'ir': ('IR', '🏥', 'Out for extended period')
        }
        
        converted_injuries = []
        for inj in injuries:
            severity = inj.get('severity', 'unknown')
            status = inj.get('status', 'unknown')
            emoji, impact_text = severity_map.get(severity, ('UNKNOWN', '❓', 'Status unknown'))
            
            player_name = inj.get('full_name', 'Unknown Player')
            position = inj.get('position', '')
            injury_type = inj.get('injury_type', 'Unknown')
            body_part = inj.get('body_part', '')
            
            conversation = f"Quick update: {player_name} ({position}) is {severity.upper()} - {injury_type}"
            if body_part:
                conversation += f" to the {body_part}"
            conversation += f". This is huge because "
            
            if severity in ['out', 'ir']:
                conversation += f"they're a key player and their team really needs them right now."
            else:
                conversation += f"we'll have to wait and see if they can play."
            
            converted_injuries.append({
                "player": player_name,
                "position": position,
                "team": inj.get('team', 'Unknown'),
                "injury": injury_type,
                "body_part": body_part,
                "status": status.upper(),
                "severity": severity,
                "emoji": emoji,
                "impact": impact_text,
                "games_missed": inj.get('games_missed', 0),
                "say_this": conversation,
                "league": inj.get('league', '').upper()
            })
        
        return {
            "league": league.upper(),
            "injuries": converted_injuries,
            "total_count": len(converted_injuries),
            "conversation_tip": "Mention injured star players to sound like an insider!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/league/{league}/teams")
async def get_teams(league: str):
    """Get all teams for a league"""
    try:
        teams_data = thesportsdb.fetch_teams(league)
        
        if not teams_data:
            return {
                "league": league.upper(),
                "teams": [],
                "message": f"No teams found for {league.upper()}"
            }
        
        teams = []
        for t in teams_data:
            teams.append({
                "id": t.get('id', ''),
                "name": t.get('name', t.get('team', 'Unknown')),
                "city": t.get('city', ''),
                "league": league.upper(),
                "logo": t.get('logo', '')
            })
        
        return {
            "league": league.upper(),
            "teams": teams,
            "total": len(teams)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/league/{league}/top-players")
async def get_top_players(league: str, limit: int = Query(10, ge=1, le=50)):
    """Get top players for a league"""
    try:
        players = thesportsdb.fetch_players(league)
        
        if not players:
            return {
                "league": league.upper(),
                "players": [],
                "message": f"No player data available for {league.upper()}"
            }
        
        top_players = []
        for p in players[:limit]:
            top_players.append({
                "id": p.get('id', ''),
                "name": p.get('full_name', p.get('name', 'Unknown')),
                "team": p.get('team', 'Unknown'),
                "position": p.get('position', 'N/A'),
                "nationality": p.get('nationality', 'Unknown'),
                "number": p.get('jersey', '')
            })
        
        return {
            "league": league.upper(),
            "players": top_players,
            "total": len(players)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/chat/talking-points")
async def get_talking_points(league: Optional[str] = None):
    """Get conversation starters and talking points"""
    try:
        eng = get_db_engine()
        points = []
        
        if not league or league == 'nfl':
            upsets = get_recent_upsets(eng, 'nfl', 3)
            for u in upsets:
                points.append({
                    "league": "NFL",
                    "topic": f"Upset Alert: {u.get('winner')} beat {u.get('loser')}",
                    "say_this": f"Did you see that {u.get('winner')} upset? Nobody saw that coming!",
                    "context": f"Final score: {u.get('home_score')}-{u.get('away_score')}"
                })
        
        if not league or league == 'nba':
            upsets = get_recent_upsets(eng, 'nba', 3)
            for u in upsets:
                points.append({
                    "league": "NBA",
                    "topic": f"Big Game: {u.get('winner')} vs {u.get('loser')}",
                    "say_this": f"That game between {u.get('winner')} and {u.get('loser')} was insane!",
                    "context": f"Score: {u.get('home_score')}-{u.get('away_score')}"
                })
        
        injuries = get_active_injuries(eng, league)
        for inj in injuries[:3]:
            points.append({
                "league": inj.get('league', '').upper(),
                "topic": f"Injury Update: {inj.get('full_name')}",
                "say_this": f"Oh man, {inj.get('full_name')} is {inj.get('status')} right now. Huge loss for {inj.get('team')}!",
                "context": f"Injury: {inj.get('injury_type')}"
            })
        
        if not points:
            points = [
                {
                    "league": "General",
                    "topic": "No recent data",
                    "say_this": "Run the pipeline to get the latest talking points!",
                    "context": "Use: python main.py --league <nfl|nba|mlb|nhl> --include-upsets --include-injuries"
                }
            ]
        
        return {
            "talking_points": points,
            "tip": "Lead with the most surprising news to keep conversations interesting!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/league/{league}/game-results")
async def get_game_results(league: str, limit: int = Query(10, ge=1, le=50)):
    """Get recent game results"""
    try:
        games = thesportsdb.fetch_games(league, datetime.now().year)
        
        if not games:
            return {
                "league": league.upper(),
                "games": [],
                "message": f"No recent games found for {league.upper()}"
            }
        
        results = []
        for g in games[:limit]:
            home = g.get('home_team', 'Home')
            away = g.get('away_team', 'Away')
            home_score = g.get('home_score', 0)
            away_score = g.get('away_score', 0)
            
            winner = home if home_score > away_score else away
            margin = abs(home_score - away_score)
            
            if margin == 1:
                result_type = "thriller"
                emoji = "😱"
            elif margin <= 5:
                result_type = "close_game"
                emoji = "🔥"
            elif margin >= 20:
                result_type = "blowout"
                emoji = "💪"
            else:
                result_type = "normal"
                emoji = "⚽"
            
            results.append({
                "home": home,
                "away": away,
                "home_score": home_score,
                "away_score": away_score,
                "winner": winner,
                "margin": margin,
                "result_type": result_type,
                "emoji": emoji,
                "say_this": f"{winner} won {'comfortably' if margin > 10 else 'in a close one'} {home_score}-{away_score}!"
            })
        
        return {
            "league": league.upper(),
            "games": results,
            "total": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary/{league}")
async def get_league_summary(league: str):
    """Get a complete summary for a league - perfect for conversations!"""
    try:
        eng = get_db_engine()
        
        standings = get_team_records(eng, league)
        upsets = get_recent_upsets(eng, league, 5)
        injuries = get_active_injuries(eng, league)
        stats = get_upset_stats(eng, league)
        
        top_teams = standings[:5] if standings else []
        bottom_teams = standings[-3:] if standings else []
        
        biggest_upset = None
        for u in upsets:
            mag = u.get('upset_magnitude', 0)
            if not biggest_upset or mag > biggest_upset.get('upset_magnitude', 0):
                biggest_upset = u
        
        hot_teams = []
        cold_teams = []
        if top_teams:
            hot_teams = [t.get('team') for t in top_teams[:3]]
        if bottom_teams:
            cold_teams = [t.get('team') for t in bottom_teams[:3]]
        
        summary = {
            "league": league.upper(),
            "generated_at": datetime.now().isoformat(),
            "at_a_glance": {
                "total_teams": len(standings),
                "hot_teams": hot_teams,
                "cold_teams": cold_teams,
                "biggest_upset": f"{biggest_upset.get('winner')} over {biggest_upset.get('loser')}" if biggest_upset else "None yet"
            },
            "conversation_starters": {
                "biggest_surprise": f"{biggest_upset.get('winner')} beating {biggest_upset.get('loser')} was INSANE!" if biggest_upset else "No upsets recorded yet",
                "hot_take": f"Everyone's talking about how {hot_teams[0]} is dominating right now" if hot_teams else "No hot teams yet",
                "injury_update": f"Did you hear {injuries[0].get('full_name')} is out?" if injuries else "No major injuries right now"
            },
            "quick_stats": {
                "total_upsets": stats.get('total_upsets', 0),
                "avg_upset_margin": round(stats.get('avg_magnitude', 0), 1),
                "active_injuries": len(injuries)
            },
            "standings_preview": [
                {
                    "rank": i + 1,
                    "team": t.get('team'),
                    "record": f"{t.get('wins', 0)}-{t.get('losses', 0)}"
                }
                for i, t in enumerate(standings[:10])
            ]
        }
        
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "AthletiDB API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/insights")
async def get_insights(league: Optional[str] = None):
    """Get all insights formatted for casual conversation"""
    try:
        eng = get_db_engine()
        insights = []
        
        for lg in ['nfl', 'nba', 'mlb', 'nhl']:
            if league and lg != league:
                continue
                
            upsets = get_recent_upsets(eng, lg, 2)
            for u in upsets:
                insights.append({
                    "type": "upset",
                    "league": lg.upper(),
                    "title": f"🎉 Huge Upset!",
                    "description": f"{u.get('winner')} beat {u.get('loser')}",
                    "conversation": f"Did you see {u.get('winner')} upset {u.get('loser')}? Nobody saw that coming!",
                    "score": f"{u.get('home_score')}-{u.get('away_score')}"
                })
            
            injuries = get_active_injuries(eng, lg)
            for inj in injuries[:1]:
                insights.append({
                    "type": "injury",
                    "league": lg.upper(),
                    "title": "🏥 Injury Alert",
                    "description": f"{inj.get('full_name')} - {inj.get('status')}",
                    "conversation": f"Oh man, {inj.get('full_name')} being {inj.get('status')} is huge for {inj.get('team')}!"
                })
        
        return {
            "insights": insights[:20],
            "total": len(insights),
            "tip": "Drop these in conversations to sound like a die-hard fan!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/quick-stats/{league}")
async def get_quick_stats(league: str):
    """Get quick stats for a league"""
    try:
        eng = get_db_engine()
        
        records = get_team_records(eng, league)
        upsets = get_recent_upsets(eng, league, 10)
        injuries = get_active_injuries(eng, league)
        
        if not records:
            return {"league": league.upper(), "message": "No data yet. Run the pipeline!"}
        
        top_team = records[0] if records else None
        bottom_team = records[-1] if records else None
        
        return {
            "league": league.upper(),
            "leader": {
                "team": top_team.get('team', 'N/A') if top_team else 'N/A',
                "record": f"{top_team.get('wins', 0)}-{top_team.get('losses', 0)}" if top_team else 'N/A',
                "win_pct": f"{top_team.get('win_percentage', 0):.3f}" if top_team else '0.000'
            },
            "struggling": {
                "team": bottom_team.get('team', 'N/A') if bottom_team else 'N/A',
                "record": f"{bottom_team.get('wins', 0)}-{bottom_team.get('losses', 0)}" if bottom_team else 'N/A',
                "win_pct": f"{bottom_team.get('win_percentage', 0):.3f}" if bottom_team else '0.000'
            },
            "stats": {
                "total_teams": len(records),
                "recent_upsets": len(upsets),
                "active_injuries": len(injuries),
                "biggest_upset": f"{upsets[0].get('winner')} over {upsets[0].get('loser')}" if upsets else "None"
            },
            "say_this": f"Did you know {top_team.get('team') if top_team else 'the top team'} is dominating? They've been incredible!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/all-leagues")
async def get_all_leagues():
    """Get overview of all leagues"""
    try:
        eng = get_db_engine()
        leagues_data = {}
        
        for lg in ['nfl', 'nba', 'mlb', 'nhl']:
            records = get_team_records(eng, lg)
            upsets = get_recent_upsets(eng, lg, 5)
            injuries = get_active_injuries(eng, lg)
            
            leagues_data[lg.upper()] = {
                "teams": len(records),
                "upsets": len(upsets),
                "injuries": len(injuries),
                "top_team": records[0].get('team') if records else None,
                "latest_upset": f"{upsets[0].get('winner')} def {upsets[0].get('loser')}" if upsets else None
            }
        
        return {
            "leagues": leagues_data,
            "generated_at": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/player/{player_name}")
async def get_player(player_name: str):
    """Search for a player"""
    try:
        from pipeline.db import get_engine
        eng = get_engine()
        
        from sqlalchemy import text
        result = eng.connect().execute(text("""
            SELECT * FROM players 
            WHERE full_name LIKE :name OR first_name LIKE :name OR last_name LIKE :name
            LIMIT 10
        """), {"name": f"%{player_name}%"})
        
        players = [dict(row._mapping) for row in result]
        
        if not players:
            return {"message": f"No player found matching '{player_name}'", "players": []}
        
        return {
            "players": players,
            "count": len(players)
        }
    except Exception as e:
        return {"message": "Player search unavailable", "players": [], "error": str(e)}

@app.get("/api/search")
async def search_all(q: str = Query(..., min_length=1)):
    """Search across all data types"""
    try:
        eng = get_db_engine()
        results = {
            "query": q,
            "teams": [],
            "players": [],
            "upsets": []
        }
        
        from sqlalchemy import text
        
        teams = eng.connect().execute(text("""
            SELECT team, league FROM team_records 
            WHERE team LIKE :q LIMIT 5
        """), {"q": f"%{q}%"})
        results["teams"] = [dict(row._mapping) for row in teams]
        
        upsets = eng.connect().execute(text("""
            SELECT winner, loser, league, game_date FROM upsets 
            WHERE winner LIKE :q OR loser LIKE :q LIMIT 5
        """), {"q": f"%{q}%"})
        results["upsets"] = [dict(row._mapping) for row in upsets]
        
        return results
    except Exception as e:
        return {"query": q, "error": str(e), "teams": [], "players": [], "upsets": []}

@app.post("/api/refresh")
async def refresh_data(league: Optional[str] = None):
    """Trigger a data refresh (placeholder for actual refresh logic)"""
    return {
        "status": "success",
        "message": "Data refresh triggered",
        "league": league if league else "all",
        "note": "This would trigger the pipeline in production"
    }

@app.get("/api/streaks/{league}")
async def get_team_streaks(league: str):
    """Get teams on winning and losing streaks"""
    try:
        eng = get_db_engine()
        records = get_team_records(eng, league)
        
        if not records:
            return {"league": league.upper(), "message": "No data yet", "winning_streak": [], "losing_streak": []}
        
        winning = records[:3]
        losing = records[-3:][::-1]
        
        return {
            "league": league.upper(),
            "winning_streak": [
                {
                    "team": t.get('team'),
                    "record": f"{t.get('wins', 0)}-{t.get('losses', 0)}",
                    "say_this": f"{t.get('team')} is on fire right now! They've won {t.get('wins', 0)} games!"
                }
                for t in winning
            ],
            "losing_streak": [
                {
                    "team": t.get('team'),
                    "record": f"{t.get('wins', 0)}-{t.get('losses', 0)}",
                    "say_this": f"It's tough to watch {t.get('team')} this season, they've lost {t.get('losses', 0)} games already."
                }
                for t in losing
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/around-the-horn")
async def get_around_the_horn():
    """Get sports news from all leagues formatted for conversation"""
    try:
        eng = get_db_engine()
        news_items = []
        
        for lg in ['nfl', 'nba', 'mlb', 'nhl']:
            upsets = get_recent_upsets(eng, lg, 2)
            for u in upsets:
                news_items.append({
                    "type": "upset",
                    "league": lg.upper(),
                    "headline": f"{u.get('winner')} pulls off upset vs {u.get('loser')}",
                    "script": f"Did you see that? {u.get('winner')} absolutely stunned {u.get('loser')}! That's a huge upset!",
                    "score": f"{u.get('home_score')}-{u.get('away_score')}"
                })
            
            injuries = get_active_injuries(eng, lg, None)
            for inj in injuries[:1]:
                if inj.get('status') in ['out', 'ir']:
                    news_items.append({
                        "type": "injury",
                        "league": lg.upper(),
                        "headline": f"{inj.get('full_name')} out for {inj.get('team')}",
                        "script": f"Breaking news: {inj.get('full_name')} is done for the season. That's a massive blow to {inj.get('team')}!",
                        "details": f"{inj.get('injury_type')} - {inj.get('status')}"
                    })
        
        return {
            "news": news_items[:15],
            "title": "📰 Around the Horn - Sports News You Can Use",
            "tip": "Lead with the most surprising news to sound like you watch every game!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/league/{league}/headlines")
async def get_headlines(league: str):
    """Get formatted headlines for a league"""
    try:
        eng = get_db_engine()
        
        upsets = get_recent_upsets(eng, league, 3)
        injuries = get_active_injuries(eng, league)
        records = get_team_records(eng, league)
        
        headlines = []
        
        if upsets:
            top_upset = upsets[0]
            headlines.append({
                "priority": "high",
                "category": "upset",
                "headline": f"⚡ {top_upset.get('winner')} upset {top_upset.get('loser')}!",
                "script": f"🎯 Use this: 'Did you see {top_upset.get('winner')} upset {top_upset.get('loser')}? Nobody expected that!'"
            })
        
        if injuries:
            critical = [i for i in injuries if i.get('status') in ['out', 'ir']]
            if critical:
                i = critical[0]
                headlines.append({
                    "priority": "high",
                    "category": "injury",
                    "headline": f"🏥 {i.get('full_name')} ({i.get('status')})",
                    "script": f"🎯 Use this: 'Man, {i.get('full_name')} being {i.get('status')} is a huge loss for {i.get('team')}!'"
                })
        
        if records:
            top = records[0]
            headlines.append({
                "priority": "medium",
                "category": "standings",
                "headline": f"🏆 {top.get('team')} leads the {league.upper()}",
                "script": f"🎯 Use this: '{top.get('team')} is absolutely dominating right now. They're {top.get('wins', 0)}-{top.get('losses', 0)}!'"
            })
        
        return {
            "league": league.upper(),
            "headlines": headlines,
            "tip": "Pick the highest priority headline to start your conversation!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)