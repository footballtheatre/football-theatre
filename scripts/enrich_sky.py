#!/usr/bin/env python3
"""
Sky Sports Playlist Enricher
=============================
Fetches all videos from the Sky Sports YouTube playlist and matches
them to fixtures in the 2024-25 season data by parsing team names
from video titles.

Requirements:
    pip install google-api-python-client --break-system-packages

Usage:
    export YOUTUBE_API_KEY="your-key-here"
    python scripts/enrich_sky.py
"""

import os
import sys
import json
import time
from datetime import date, datetime, timedelta
from typing import Dict, FrozenSet, List, Optional, Tuple

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("ERROR: Missing required libraries.")
    print("Install with: pip install google-api-python-client --break-system-packages")
    sys.exit(1)


PLAYLIST_ID = "PLISuFiQTdKDWc1PjlgqIAm1Bzc38MoLa6"
FIXTURES_FILE = "data/2024-25-fixtures-full.json"
OUTPUT_FILE = "data/2024-25-fixtures-sky-enriched.json"

# Maps lowercase title fragments → canonical fixture team name.
# Sorted by length at runtime so longer aliases take priority (e.g.
# "manchester city" matches before "man city" or "city").
TEAM_ALIASES: Dict[str, str] = {
    "arsenal": "Arsenal",
    "aston villa": "Aston Villa",
    "villa": "Aston Villa",
    "bournemouth": "Bournemouth",
    "afc bournemouth": "Bournemouth",
    "brentford": "Brentford",
    "brighton & hove albion": "Brighton",
    "brighton and hove albion": "Brighton",
    "brighton": "Brighton",
    "chelsea": "Chelsea",
    "crystal palace": "Crystal Palace",
    "palace": "Crystal Palace",
    "everton": "Everton",
    "fulham": "Fulham",
    "ipswich town": "Ipswich",
    "ipswich": "Ipswich",
    "leicester city": "Leicester",
    "leicester": "Leicester",
    "liverpool": "Liverpool",
    "manchester city": "Manchester City",
    "man city": "Manchester City",
    "manchester united": "Manchester United",
    "man united": "Manchester United",
    "man utd": "Manchester United",
    "newcastle united": "Newcastle United",
    "newcastle": "Newcastle United",
    "nottingham forest": "Nottingham Forest",
    "nott'm forest": "Nottingham Forest",
    "nottm forest": "Nottingham Forest",
    "n.forest": "Nottingham Forest",
    "n forest": "Nottingham Forest",
    "forest": "Nottingham Forest",
    "southampton": "Southampton",
    "saints": "Southampton",
    "tottenham hotspur": "Tottenham",
    "tottenham": "Tottenham",
    "spurs": "Tottenham",
    "west ham united": "West Ham",
    "west ham": "West Ham",
    "wolverhampton wanderers": "Wolves",
    "wolverhampton": "Wolves",
    "wolves": "Wolves",
}

# Pre-sorted aliases: longest first so more specific names match before short ones.
_SORTED_ALIASES = sorted(TEAM_ALIASES.keys(), key=len, reverse=True)


def fetch_playlist_videos(youtube, playlist_id: str) -> List[Dict]:
    """Fetch all videos from a YouTube playlist, handling pagination."""
    videos = []
    page_token = None
    page = 1

    while True:
        print(f"  Fetching page {page}...")
        params: Dict = dict(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
        )
        if page_token:
            params["pageToken"] = page_token

        try:
            response = youtube.playlistItems().list(**params).execute()
        except HttpError as e:
            print(f"ERROR: YouTube API error: {e}")
            if e.resp.status == 403:
                print("Quota exceeded or API key invalid.")
            break

        for item in response.get("items", []):
            snippet = item["snippet"]

            # Skip deleted/private videos
            if snippet.get("title") in ("Deleted video", "Private video"):
                continue

            video_id = snippet.get("resourceId", {}).get("videoId")
            if not video_id:
                continue

            thumbnails = snippet.get("thumbnails", {})
            thumbnail_url = (
                thumbnails.get("high", thumbnails.get("medium", thumbnails.get("default", {}))).get("url", "")
            )

            videos.append({
                "videoId": video_id,
                "title": snippet["title"],
                "channel": "Sky Sports",
                "channelId": snippet.get("channelId", ""),
                "publishedAt": snippet.get("publishedAt", ""),
                "thumbnail": thumbnail_url,
                "description": snippet.get("description", "")[:200],
                "type": "official",
                "relevanceScore": 0.95,
                "geoBlocked": ["US", "CA"],
            })

        page_token = response.get("nextPageToken")
        if not page_token:
            break

        page += 1
        time.sleep(0.2)  # Stay well within rate limits

    return videos


def extract_teams_from_title(title: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Scan a video title for two Premier League team names.

    Returns (canonical_team1, canonical_team2) in order of appearance,
    or (None, None) if fewer than two teams are found.
    """
    title_lower = title.lower()
    found: List[str] = []

    for alias in _SORTED_ALIASES:
        canonical = TEAM_ALIASES[alias]
        if canonical not in found and alias in title_lower:
            found.append(canonical)
        if len(found) == 2:
            break

    if len(found) >= 2:
        return found[0], found[1]
    return None, None


def parse_date(date_str: str) -> Optional[date]:
    """Parse an ISO 8601 date or datetime string to a date object."""
    if not date_str:
        return None
    try:
        # Handle both "2024-08-16" and "2024-08-16T15:30:00Z"
        return datetime.fromisoformat(date_str.rstrip("Z").split("T")[0]).date()
    except (ValueError, AttributeError):
        return None


def build_fixture_index(gameweeks: List[Dict]) -> Dict[FrozenSet, List[Tuple[date, Dict]]]:
    """Build a lookup dict: frozenset({home, away}) → list of (date, fixture)."""
    index: Dict[FrozenSet, List[Tuple[date, Dict]]] = {}
    for gw in gameweeks:
        for fixture in gw["fixtures"]:
            key = frozenset([fixture["home"], fixture["away"]])
            fixture_date = parse_date(fixture.get("date", ""))
            if key not in index:
                index[key] = []
            index[key].append((fixture_date, fixture))
    return index


def find_fixture(
    index: Dict[FrozenSet, List[Tuple[date, Dict]]],
    team1: str,
    team2: str,
    published_at: str,
    window_days: int = 3,
) -> Optional[Dict]:
    """Find the fixture for a team pair whose date is within window_days of the video publish date."""
    key = frozenset([team1, team2])
    candidates = index.get(key)
    if not candidates:
        return None

    video_date = parse_date(published_at)

    for fixture_date, fixture in candidates:
        if video_date is None or fixture_date is None:
            # Can't date-match — fall back to first candidate
            return fixture
        if abs((video_date - fixture_date).days) <= window_days:
            return fixture

    return None


def main() -> None:
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        print("ERROR: YOUTUBE_API_KEY environment variable not set.")
        sys.exit(1)

    print(f"Loading fixtures from {FIXTURES_FILE}...")
    with open(FIXTURES_FILE, "r", encoding="utf-8") as f:
        fixtures_data = json.load(f)

    youtube = build("youtube", "v3", developerKey=api_key)

    print(f"\nFetching videos from playlist {PLAYLIST_ID}...")
    videos = fetch_playlist_videos(youtube, PLAYLIST_ID)
    print(f"Fetched {len(videos)} videos.\n")

    fixture_index = build_fixture_index(fixtures_data["gameweeks"])

    matched = 0
    unmatched: List[str] = []

    for video in videos:
        team1, team2 = extract_teams_from_title(video["title"])
        if not team1 or not team2:
            unmatched.append(video["title"])
            continue

        fixture = find_fixture(fixture_index, team1, team2, video["publishedAt"])
        if fixture is None:
            unmatched.append(video["title"])
            continue

        if "videos" not in fixture:
            fixture["videos"] = []

        # Insert Sky Sports entry at the front
        fixture["videos"].insert(0, video)
        matched += 1

    print(f"Matched : {matched} / {len(videos)} videos")
    if unmatched:
        print(f"Unmatched ({len(unmatched)}):")
        for title in unmatched:
            print(f"  - {title}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(fixtures_data, f, indent=2, ensure_ascii=False)

    print(f"\nSaved enriched fixtures to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
