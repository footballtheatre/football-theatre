#!/usr/bin/env python3
"""
English Football Theatre - YouTube Video Collector
===================================================

This script searches YouTube for Premier League match highlights and builds
a database of available videos with geo-blocking information.

Requirements:
    pip install google-api-python-client python-dateutil --break-system-packages

Usage:
    1. Get a YouTube API key from Google Cloud Console
    2. Set your API key: export YOUTUBE_API_KEY="your-key-here"
    3. Run: python collect_videos.py

API Costs:
    - Free tier: 10,000 units/day
    - Each search: ~100 units
    - This script: ~100 searches/day max (configurable)
"""

import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import sys

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("ERROR: Missing required libraries.")
    print("Install with: pip install google-api-python-client --break-system-packages")
    sys.exit(1)


class YouTubeVideoCollector:
    """Collects Premier League match videos from YouTube."""
    
    def __init__(self, api_key: str):
        """Initialize with YouTube API key."""
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.quota_used = 0
        self.daily_limit = 10000  # YouTube API daily quota
        self.searches_today = 0
        self.max_searches_per_day = 95  # Conservative limit
        
        # Official channels (prioritize these)
        self.official_channels = {
            'Premier League': 'UCG5qGWdu8nIRZqJ_GgDwQ-w',
            'Sky Sports Football': 'UCNAf1k0yIjyGu5T8OqIaLOA', 
            'NBC Sports': 'UCqZQlzSHbVJrwrn5XvzrzcA',
            'beIN SPORTS': 'UCcyzVqzlwU0n0pjgTZJsY5Q',
            'BT Sport': 'UCg6dYGxN6TIY0hDVxJvL9Kw',
        }
        
        # Known geo-blocking patterns
        self.geo_patterns = {
            'Sky Sports': ['US', 'CA'],
            'NBC Sports': ['GB', 'IE'],
            'BT Sport': ['US', 'CA'],
            'beIN SPORTS': [],  # Usually region-specific
        }

        # Non-English channel name fragments (case-insensitive)
        self.non_english_channels = [
            'telemundo', 'espn deportes', 'tudn', 'univision', 'bein sports arabic',
            'movistar', 'dazn espanol', 'canal+', 'sky italia', 'sportv',
            'canal do futebol', 'eleven sports portugal', 'eleven sports pl',
        ]

        # Non-English title keywords that signal foreign-language content
        self.non_english_title_terms = [
            'resumen', 'todos los goles', 'en vivo', 'destacados', 'goles',
            'melhores momentos', 'resumo', 'zusammenfassung', 'tore',
            'buts', 'résumé', 'maç özeti', 'gollar',
        ]

        # Known-good broadcaster channels — get a strong score boost so they
        # always outrank random reupload accounts (matched as substrings, lowercase).
        self.broadcaster_whitelist = {
            'nbc sports', 'sky sports', 'sky sports football', 'premier league',
            'tnt sports', 'amazon prime video sport', 'bein sports', 'bt sport',
            'channel 4 sport', 'channel 4', 'itv sport',
        }

        # Official club channel name fragments — preferred over reuploads but
        # ranked below major broadcasters (matched as substrings, lowercase).
        self.club_channel_whitelist = {
            'arsenal', 'liverpool', 'chelsea', 'manchester city', 'manchester united',
            'man city', 'man utd', 'man united', 'tottenham hotspur', 'tottenham hotspur fc',
            'newcastle united', 'aston villa', 'west ham united',
            'brighton & hove albion', 'brentford fc', 'fulham fc',
            'crystal palace', 'wolverhampton wanderers',
            'everton', 'nottingham forest', 'luton town', 'burnley fc',
            'sheffield united', 'afc bournemouth', 'leicester city',
            'ipswich town', 'southampton fc',
        }

        # Fragments in channel names that signal reupload / low-quality accounts
        # (matched as substrings, lowercase).
        self.reupload_channel_patterns = [
            'cyber', 'highlights hd', 'sir-',
            'gameeworld', 'understand facts', 'sports pulse',
        ]
    
    def search_match_videos(self, home: str, away: str, date: str, 
                           score: Optional[str] = None) -> List[Dict]:
        """
        Search for videos of a specific match using multiple strategies.
        
        Args:
            home: Home team name
            away: Away team name  
            date: Match date (YYYY-MM-DD)
            score: Optional score (e.g., "2-1")
            
        Returns:
            List of video metadata dictionaries
        """
        if self.searches_today >= self.max_searches_per_day:
            print(f"⚠️  Daily search limit reached ({self.max_searches_per_day})")
            return []
        
        videos = []
        seen_ids = set()
        
        # Parse date for search queries
        match_date = datetime.strptime(date, '%Y-%m-%d')
        year = match_date.year
        month = match_date.strftime('%B')
        
        # Multiple search strategies (ordered by priority)
        search_queries = [
            # Strategy 1: Exact match with date
            f"{home} {away} {date} highlights",
            
            # Strategy 2: With score (if available)
            f"{home} {score} {away} {year}" if score else None,
            
            # Strategy 3: Premier League specific
            f"{home} vs {away} Premier League {year}",
            
            # Strategy 4: Extended highlights
            f"{home} {away} extended highlights {month} {year}",
            
            # Strategy 5: Reverse team order (catches both naming conventions)
            f"{away} {home} {year} Premier League",
        ]
        
        # Remove None values
        search_queries = [q for q in search_queries if q]
        
        # Date window: match day through 14 days after (catches same-day and delayed uploads)
        published_after = match_date.strftime('%Y-%m-%dT00:00:00Z')
        published_before = (match_date + timedelta(days=14)).strftime('%Y-%m-%dT00:00:00Z')

        for query in search_queries[:3]:  # Limit to first 3 strategies to save quota
            try:
                results = self._youtube_search(
                    query, max_results=5,
                    published_after=published_after,
                    published_before=published_before,
                )
                self.searches_today += 1
                self.quota_used += 100  # Each search costs ~100 units
                
                for item in results:
                    video_id = item['id']['videoId']
                    
                    # Skip duplicates
                    if video_id in seen_ids:
                        continue
                    seen_ids.add(video_id)
                    
                    # Extract metadata
                    video_data = self._extract_video_metadata(item, home, away, date)
                    
                    if video_data:
                        videos.append(video_data)
                
                # Rate limiting: be nice to the API
                time.sleep(0.5)
                
            except HttpError as e:
                print(f"❌ YouTube API error: {e}")
                if e.resp.status == 403:
                    print("⚠️  Quota exceeded or API key invalid")
                    break
            except Exception as e:
                print(f"❌ Error searching '{query}': {e}")
        
        # Sort by relevance/quality
        videos = self._rank_videos(videos, home, away, score)
        
        return videos[:5]  # Return top 5 videos max per match
    
    def _youtube_search(self, query: str, max_results: int = 10,
                        published_after: Optional[str] = None,
                        published_before: Optional[str] = None) -> List[Dict]:
        """Execute YouTube search API call."""
        params = dict(
            part='snippet',
            q=query,
            type='video',
            maxResults=max_results,
            order='relevance',
            videoDuration='medium',  # 4-20 mins (typical highlight length)
            regionCode='US',  # Default search region
        )
        if published_after:
            params['publishedAfter'] = published_after
        if published_before:
            params['publishedBefore'] = published_before

        request = self.youtube.search().list(**params)
        response = request.execute()
        return response.get('items', [])
    
    def _extract_video_metadata(self, item: Dict, home: str, away: str, 
                                date: str) -> Optional[Dict]:
        """Extract and structure video metadata."""
        try:
            snippet = item['snippet']
            video_id = item['id']['videoId']
            title = snippet['title']
            channel = snippet['channelTitle']
            
            # Filter out irrelevant videos
            if not self._is_relevant_video(title, home, away):
                return None
            
            # Determine if official channel (broadcaster whitelist or club channel)
            channel_lower = channel.lower()
            is_official = (
                any(wl in channel_lower for wl in self.broadcaster_whitelist)
                or any(club in channel_lower for club in self.club_channel_whitelist)
            )
            
            # Determine geo-blocking (heuristic based on channel)
            geo_blocked = self._get_geo_blocking(channel)
            
            published_at = snippet['publishedAt']
            return {
                'videoId': video_id,
                'title': title,
                'channel': channel,
                'channelId': snippet['channelId'],
                'publishedAt': published_at,
                'thumbnail': snippet['thumbnails']['high']['url'],
                'description': snippet.get('description', '')[:200],
                'isOfficial': is_official,
                'geoBlocked': geo_blocked,
                'relevanceScore': self._calculate_relevance(
                    title, channel, home, away,
                    published_at=published_at, match_date=date,
                ),
            }
            
        except Exception as e:
            print(f"⚠️  Error extracting metadata: {e}")
            return None
    
    def _is_relevant_video(self, title: str, home: str, away: str) -> bool:
        """Check if video title is relevant to the match."""
        title_lower = title.lower()
        home_lower = home.lower()
        away_lower = away.lower()
        
        # Must contain at least one team name
        has_team = home_lower in title_lower or away_lower in title_lower
        
        # Filter out common false positives
        excluded_terms = [
            'fifa', 'pes', 'fm24', 'career mode', 'prediction', 'preview',
            'from the stands', 'fan cam', 'phone footage',
        ]
        has_excluded = any(term in title_lower for term in excluded_terms)
        
        # Prefer highlight keywords
        highlight_terms = ['highlights', 'goals', 'extended', 'all goals', 'match']
        has_highlights = any(term in title_lower for term in highlight_terms)
        
        return has_team and not has_excluded and has_highlights
    
    def _get_geo_blocking(self, channel: str) -> List[str]:
        """Determine likely geo-blocking based on channel."""
        for pattern, blocked_regions in self.geo_patterns.items():
            if pattern in channel:
                return blocked_regions
        return []  # Assume global if unknown
    
    def _calculate_relevance(self, title: str, channel: str,
                             home: str, away: str,
                             published_at: Optional[str] = None,
                             match_date: Optional[str] = None) -> float:
        """Calculate relevance score (0-1) for ranking."""
        score = 0.5  # Base score

        title_lower = title.lower()
        channel_lower = channel.lower()

        # Boost for both team names
        if home.lower() in title_lower and away.lower() in title_lower:
            score += 0.2

        # Channel quality scoring — three tiers so that good sources always
        # outrank reupload channels when both are available for the same match.
        is_broadcaster = any(wl in channel_lower for wl in self.broadcaster_whitelist)
        is_club_channel = any(club in channel_lower for club in self.club_channel_whitelist)

        if is_broadcaster:
            score += 0.3  # Major broadcaster — always ranks above reuploads
        elif is_club_channel:
            score += 0.2  # Official club channel — preferred over reuploads
        else:
            # Penalise channels that look like random reupload accounts.
            # Checks are cumulative so a channel can incur multiple penalties.
            if self._is_allcaps_channel(channel):
                score -= 0.35
            if any(pat in channel_lower for pat in self.reupload_channel_patterns):
                score -= 0.3

        # Boost for highlight keywords
        if 'extended' in title_lower:
            score += 0.1
        if 'full highlights' in title_lower:
            score += 0.1

        # Penalty for club-specific highlights
        if 'official' in title_lower and (home.lower() in title_lower or away.lower() in title_lower):
            score -= 0.05  # Might be one-sided

        # Penalty for non-English channels
        if any(ne in channel_lower for ne in self.non_english_channels):
            score -= 0.3

        # Penalty for non-English title keywords
        if any(term in title_lower for term in self.non_english_title_terms):
            score -= 0.2

        # Boost for videos published close to the match date (within 3 days = fresher/more likely correct game)
        if published_at and match_date:
            try:
                pub_dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                match_dt = datetime.strptime(match_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                days_after = (pub_dt - match_dt).days
                if 0 <= days_after <= 3:
                    score += 0.1  # Published quickly after the match — likely the right game
                elif days_after > 7:
                    score -= 0.05  # Late upload — slightly less confident
            except (ValueError, TypeError):
                pass

        # Unknown channels (not broadcaster or club whitelist) are capped at 0.65
        # so they can never outrank official sources regardless of other bonuses.
        if not is_broadcaster and not is_club_channel:
            return max(0.0, min(score, 0.65))
        return max(0.0, min(score, 1.0))
    
    def _is_allcaps_channel(self, channel: str) -> bool:
        """Return True if the channel name is entirely uppercase (reupload signal).

        Legitimate acronyms like 'NBC', 'BT', or 'TNT' appear inside mixed-case
        names ('NBC Sports', 'BT Sport') so they are not caught here.  A channel
        like 'CYBER HIGHLIGHTS HD' has no lowercase letters at all and is flagged.
        The five-letter minimum avoids penalising very short acronym-only names.
        """
        letters = ''.join(c for c in channel if c.isalpha())
        return len(letters) > 5 and letters == letters.upper()

    def _rank_videos(self, videos: List[Dict], home: str, away: str,
                    score: Optional[str]) -> List[Dict]:
        """Rank videos by relevance and quality."""
        # Sort by relevance score (descending)
        return sorted(videos, key=lambda v: v.get('relevanceScore', 0), reverse=True)
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed video information (duration, stats, etc.)."""
        try:
            request = self.youtube.videos().list(
                part='contentDetails,statistics',
                id=video_id
            )
            response = request.execute()
            
            if response['items']:
                item = response['items'][0]
                return {
                    'duration': item['contentDetails']['duration'],
                    'viewCount': item['statistics'].get('viewCount', 0),
                    'likeCount': item['statistics'].get('likeCount', 0),
                }
            
        except Exception as e:
            print(f"⚠️  Error getting video details: {e}")
        
        return None
    
    def save_results(self, results: Dict, output_file: str):
        """Save collected videos to JSON file."""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Saved results to {output_file}")
    
    def print_stats(self):
        """Print collection statistics."""
        print("\n" + "="*60)
        print("COLLECTION STATISTICS")
        print("="*60)
        print(f"Searches performed: {self.searches_today}")
        print(f"Estimated quota used: {self.quota_used} / {self.daily_limit}")
        print(f"Remaining quota: {self.daily_limit - self.quota_used}")
        print(f"Remaining searches (estimated): {(self.daily_limit - self.quota_used) // 100}")
        print("="*60 + "\n")


def load_fixtures(fixtures_file: str) -> Dict:
    """Load fixtures from JSON file."""
    with open(fixtures_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def process_season(collector: YouTubeVideoCollector, fixtures_file: str, 
                   output_file: str, max_matches: Optional[int] = None):
    """
    Process all fixtures for a season.
    
    Args:
        collector: YouTubeVideoCollector instance
        fixtures_file: Path to fixtures JSON
        output_file: Path to save results
        max_matches: Optional limit on number of matches to process
    """
    print("📂 Loading fixtures...")
    data = load_fixtures(fixtures_file)

    results = {
        'season': data.get('season', 'Unknown'),
        'processedAt': datetime.now().isoformat(),
        'gameweeks': []
    }
    
    total_matches = sum(len(gw['fixtures']) for gw in data.get('gameweeks', []))
    processed_count = 0
    videos_found = 0
    
    print(f"\n🎯 Processing {total_matches} matches...")
    print(f"⚠️  Limited to {collector.max_searches_per_day} searches/day\n")
    
    for gameweek in data.get('gameweeks', []):
        gw_number = gameweek['gameweek']
        print(f"\n{'='*60}")
        print(f"GAMEWEEK {gw_number}")
        print(f"{'='*60}\n")
        
        gw_result = {
            'gameweek': gw_number,
            'dates': gameweek.get('dates', ''),
            'fixtures': []
        }
        
        for fixture in gameweek['fixtures']:
            if max_matches and processed_count >= max_matches:
                print(f"\n⚠️  Reached max matches limit ({max_matches})")
                break
            
            home = fixture['home']
            away = fixture['away']
            score = fixture.get('score', '')
            date = fixture['date']
            
            print(f"🔍 Searching: {home} vs {away} ({score}) - {date}")
            
            # Search for videos
            videos = collector.search_match_videos(home, away, date, score)

            # Preserve Sky Sports videos from the input fixture (added by enrich_sky.py).
            # These are always kept at the front, regardless of whether a previous
            # output file exists.
            sky_videos = [v for v in fixture.get('videos', []) if 'Sky Sports' in v.get('channel', '')]
            if sky_videos:
                sky_ids = {v['videoId'] for v in sky_videos}
                merged = sky_videos + [v for v in videos if v['videoId'] not in sky_ids]
                merged.sort(key=lambda v: v.get('relevanceScore', 0), reverse=True)
                videos = merged[:5]

            fixture_result = {
                **fixture,
                'videos': videos,
                'videoCount': len(videos)
            }
            
            gw_result['fixtures'].append(fixture_result)
            
            processed_count += 1
            videos_found += len(videos)
            
            print(f"   ✅ Found {len(videos)} videos")
            
            # Check if we're approaching quota limit
            if collector.searches_today >= collector.max_searches_per_day:
                print("\n⚠️  Daily search limit reached!")
                break
            
            # Progress update
            if processed_count % 10 == 0:
                collector.print_stats()
        
        results['gameweeks'].append(gw_result)
        
        if collector.searches_today >= collector.max_searches_per_day:
            break
    
    # Save results
    collector.save_results(results, output_file)
    
    # Final stats
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Matches processed: {processed_count} / {total_matches}")
    print(f"Total videos found: {videos_found}")
    print(f"Average videos/match: {videos_found/processed_count:.1f}" if processed_count > 0 else 0)
    collector.print_stats()


def main():
    """Main entry point."""
    print("""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║        English Football Theatre - Video Collector           ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Get API key
    api_key = os.environ.get('YOUTUBE_API_KEY')
    
    if not api_key:
        print("❌ ERROR: YOUTUBE_API_KEY environment variable not set")
        print("\nTo get an API key:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project")
        print("3. Enable YouTube Data API v3")
        print("4. Create credentials (API key)")
        print("5. Set it: export YOUTUBE_API_KEY='your-key-here'")
        sys.exit(1)
    
    # Initialize collector
    collector = YouTubeVideoCollector(api_key)
    
    # Get input/output files
    fixtures_file = input("📂 Fixtures JSON file [data/2024-25-fixtures-sample.json]: ").strip()
    if not fixtures_file:
        fixtures_file = 'data/2024-25-fixtures-sample.json'
    
    output_file = input("💾 Output file [data/2024-25-with-videos.json]: ").strip()
    if not output_file:
        output_file = 'data/2024-25-with-videos.json'
    
    max_matches = input("🎯 Max matches to process (blank for all): ").strip()
    max_matches = int(max_matches) if max_matches else None
    
    # Confirm
    print(f"\n📋 Configuration:")
    print(f"   Input: {fixtures_file}")
    print(f"   Output: {output_file}")
    print(f"   Max matches: {max_matches or 'All'}")
    print(f"   API Key: {'*' * 20}{api_key[-4:]}")
    
    confirm = input("\n▶️  Start processing? [y/N]: ").strip().lower()
    
    if confirm != 'y':
        print("❌ Cancelled")
        sys.exit(0)
    
    # Process
    try:
        process_season(collector, fixtures_file, output_file, max_matches)
        print("\n✅ Complete!")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        print("Partial results may be saved")
    except FileNotFoundError:
        print(f"\n❌ ERROR: File not found: {fixtures_file}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
