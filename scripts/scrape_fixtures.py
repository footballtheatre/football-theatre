#!/usr/bin/env python3
"""
Premier League Fixture Scraper
================================

Scrapes Premier League fixtures from public sources and saves to JSON.

This uses football-data.org API (free, no key needed for basic data)
Alternative: Manually export from premierleague.com or ESPN

Requirements:
    pip install requests --break-system-packages

Usage:
    python scrape_fixtures.py --season 2024 --output data/2024-25-fixtures-full.json
"""

import json
import argparse
from datetime import datetime
from typing import List, Dict
import time

try:
    import requests
except ImportError:
    print("ERROR: requests library not installed")
    print("Install with: pip install requests --break-system-packages")
    exit(1)


class FixtureScraper:
    """Scrapes Premier League fixtures from various sources."""
    
    def __init__(self):
        self.base_url = "https://api.football-data.org/v4"
        self.headers = {
            'X-Auth-Token': 'YOUR_API_KEY_HERE'  # Optional: Get free key from football-data.org
        }
        # Premier League ID in football-data.org
        self.premier_league_id = 'PL'
    
    def get_season_fixtures(self, season: str) -> Dict:
        """
        Get all fixtures for a Premier League season.
        
        Args:
            season: Season year (e.g., '2024' for 2024/25)
            
        Returns:
            Dictionary with organized fixtures by gameweek
        """
        print(f"📡 Fetching fixtures for {season}/{int(season)+1} season...")
        
        # Try football-data.org API
        try:
            fixtures = self._fetch_from_football_data(season)
            if fixtures:
                return fixtures
        except Exception as e:
            print(f"⚠️  football-data.org failed: {e}")
        
        # Fallback: Return template for manual entry
        print("⚠️  Automatic fetch failed. Generating template...")
        return self._generate_template()
    
    def _fetch_from_football_data(self, season: str) -> Dict:
        """Fetch from football-data.org API."""
        url = f"{self.base_url}/competitions/{self.premier_league_id}/matches"
        params = {
            'season': season
        }
        
        print(f"   Calling API: {url}")
        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        
        if response.status_code == 403:
            print("   ⚠️  API key required or quota exceeded")
            return None
        
        if response.status_code != 200:
            print(f"   ❌ Error {response.status_code}")
            return None
        
        data = response.json()
        matches = data.get('matches', [])
        
        if not matches:
            print("   ⚠️  No matches found")
            return None
        
        print(f"   ✅ Found {len(matches)} matches")
        
        # Organize by gameweek
        return self._organize_by_gameweek(matches)
    
    def _organize_by_gameweek(self, matches: List[Dict]) -> Dict:
        """Organize matches into gameweek structure."""
        gameweeks = {}
        
        for match in matches:
            # Skip non-regular season matches
            if match['stage'] != 'REGULAR_SEASON':
                continue
            
            matchday = match['matchday']
            
            if matchday not in gameweeks:
                gameweeks[matchday] = {
                    'gameweek': matchday,
                    'dates': '',  # Will be set later
                    'fixtures': []
                }
            
            # Extract team names
            home_team = match['homeTeam']['name']
            away_team = match['awayTeam']['name']
            
            # Shorten common team names
            home_team = self._shorten_team_name(home_team)
            away_team = self._shorten_team_name(away_team)
            
            # Extract score if match is finished
            score = ''
            if match['status'] == 'FINISHED':
                score_data = match['score']['fullTime']
                if score_data['home'] is not None and score_data['away'] is not None:
                    score = f"{score_data['home']}-{score_data['away']}"
            
            # Extract date
            match_date = match['utcDate'][:10]  # YYYY-MM-DD
            
            fixture = {
                'home': home_team,
                'away': away_team,
                'score': score,
                'date': match_date
            }
            
            gameweeks[matchday]['fixtures'].append(fixture)
        
        # Convert to list and sort
        gameweek_list = sorted(gameweeks.values(), key=lambda x: x['gameweek'])
        
        # Set date ranges for each gameweek
        for gw in gameweek_list:
            if gw['fixtures']:
                dates = [f['date'] for f in gw['fixtures']]
                min_date = min(dates)
                max_date = max(dates)
                
                if min_date == max_date:
                    gw['dates'] = datetime.strptime(min_date, '%Y-%m-%d').strftime('%d %B %Y')
                else:
                    min_str = datetime.strptime(min_date, '%Y-%m-%d').strftime('%d')
                    max_str = datetime.strptime(max_date, '%Y-%m-%d').strftime('%d %B %Y')
                    gw['dates'] = f"{min_str}-{max_str}"
        
        return {
            'season': f'{gameweek_list[0]["fixtures"][0]["date"][:4]}-{str(int(gameweek_list[0]["fixtures"][0]["date"][:4])+1)[2:]}',
            'gameweeks': gameweek_list
        }
    
    def _shorten_team_name(self, name: str) -> str:
        """Shorten team names to common abbreviations."""
        replacements = {
            'Manchester United FC': 'Manchester United',
            'Manchester City FC': 'Manchester City',
            'Liverpool FC': 'Liverpool',
            'Arsenal FC': 'Arsenal',
            'Chelsea FC': 'Chelsea',
            'Tottenham Hotspur FC': 'Tottenham',
            'Newcastle United FC': 'Newcastle United',
            'West Ham United FC': 'West Ham',
            'Brighton & Hove Albion FC': 'Brighton',
            'Aston Villa FC': 'Aston Villa',
            'Crystal Palace FC': 'Crystal Palace',
            'Everton FC': 'Everton',
            'Brentford FC': 'Brentford',
            'Fulham FC': 'Fulham',
            'Wolverhampton Wanderers FC': 'Wolves',
            'AFC Bournemouth': 'Bournemouth',
            'Nottingham Forest FC': 'Nottingham Forest',
            'Leicester City FC': 'Leicester',
            'Ipswich Town FC': 'Ipswich',
            'Southampton FC': 'Southampton',
        }
        
        return replacements.get(name, name)
    
    def _generate_template(self) -> Dict:
        """Generate empty template for manual entry."""
        return {
            'season': '2024-25',
            'note': 'TEMPLATE - Fill in manually or use web scraper',
            'gameweeks': [
                {
                    'gameweek': i,
                    'dates': '',
                    'fixtures': []
                }
                for i in range(1, 39)
            ]
        }
    
    def save_to_json(self, data: Dict, output_file: str):
        """Save fixtures to JSON file."""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Saved to {output_file}")
        
        # Print summary
        if 'gameweeks' in data:
            total_fixtures = sum(len(gw['fixtures']) for gw in data['gameweeks'])
            with_scores = sum(
                1 for gw in data['gameweeks'] 
                for f in gw['fixtures'] 
                if f.get('score')
            )
            print(f"   Total fixtures: {total_fixtures}")
            print(f"   With scores: {with_scores}")
            print(f"   Pending: {total_fixtures - with_scores}")


def main():
    parser = argparse.ArgumentParser(description='Scrape Premier League fixtures')
    parser.add_argument('--season', type=str, default='2024', 
                       help='Season year (e.g., 2024 for 2024/25)')
    parser.add_argument('--output', type=str, default='fixtures.json',
                       help='Output JSON file')
    
    args = parser.parse_args()
    
    print("""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║           Premier League Fixture Scraper                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    scraper = FixtureScraper()
    fixtures = scraper.get_season_fixtures(args.season)
    scraper.save_to_json(fixtures, args.output)
    
    print("""
NOTE: If automatic scraping failed, you can:
1. Export fixtures from premierleague.com or ESPN
2. Use a browser extension to scrape table data
3. Fill in the template manually

For football-data.org API:
- Get free API key at: https://www.football-data.org/
- Add to script: self.headers['X-Auth-Token'] = 'YOUR_KEY'
- 10 requests/minute limit (enough for this)
    """)


if __name__ == '__main__':
    main()
