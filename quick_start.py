#!/usr/bin/env python3
"""
AthletiDB - Quick Start Script
Run this to get instant sports conversation ready data!
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.db import get_engine, get_team_records, get_recent_upsets, get_active_injuries
from pipeline.providers import thesportsdb
from datetime import datetime

def main():
    print("🏆 AthletiDB - Quick Sports Update")
    print("=" * 50)
    
    engine = get_engine("sqlite:///sports_data.db")
    
    for league in ['nfl', 'nba', 'mlb', 'nhl']:
        emoji = {'nfl': '🏈', 'nba': '🏀', 'mlb': '⚾', 'nhl': '🏒'}.get(league, '🏆')
        
        print(f"\n{emoji} {league.upper()}")
        print("-" * 30)
        
        records = get_team_records(engine, league)
        upsets = get_recent_upsets(engine, league, 3)
        injuries = get_active_injuries(engine, league)
        
        if records:
            top = records[0]
            print(f"🏆 Leader: {top.get('team')} ({top.get('wins', 0)}-{top.get('losses', 0)})")
        
        if upsets:
            u = upsets[0]
            print(f"⚡ Big Upset: {u.get('winner')} beat {u.get('loser')}!")
        
        if injuries:
            i = injuries[0]
            print(f"🏥 Injury Alert: {i.get('full_name')} ({i.get('status')})")
        
        if not records and not upsets and not injuries:
            print("  No data yet. Run: python main.py --league " + league)
    
    print("\n" + "=" * 50)
    print("\n💬 Quick Talking Points:")
    print("-" * 30)
    
    all_upsets = get_recent_upsets(engine, limit=5)
    all_injuries = get_active_injuries(engine)
    
    if all_upsets:
        print("\n🎉 Recent Upsets (drop these in conversations!):")
        for u in all_upsets[:3]:
            print(f"  • '{u.get('winner')} upset {u.get('loser')}!'")
    
    if all_injuries:
        print("\n🏥 Key Injuries:")
        for i in all_injuries[:3]:
            print(f"  • {i.get('full_name')} is {i.get('status')} for {i.get('team')}")
    
    if not all_upsets and not all_injuries:
        print("  No data yet! Run the pipeline to get started:")
        print("  python main.py --include-upsets --include-injuries")
    
    print("\n🌐 Web UI: python web/main.py")
    print("📱 TUI: python -m ui.tui summary --league nfl")

if __name__ == "__main__":
    main()