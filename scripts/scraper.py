"""
NHL Data Scraper
Fetches standings, skater stats, goalie stats, team advanced stats,
and roster data from the official NHL API.
Saves everything as JSON files in /data/ for the dashboard to read.
"""

import requests
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

BASE     = "https://api-web.nhle.com/v1"
STATS    = "https://api.nhle.com/stats/rest/en"
SEASON   = "20252026"

HEADERS = {"User-Agent": "NHL-Analytics-Dashboard/1.0", "Accept": "application/json"}

TEAMS = [
    "ANA","BOS","BUF","CGY","CAR","CHI","COL","CBJ","DAL","DET",
    "EDM","FLA","LAK","MIN","MTL","NSH","NJD","NYI","NYR","OTT",
    "PHI","PIT","SEA","SJS","STL","TBL","TOR","UTA","VAN","VGK","WSH","WPG"
]

# ── HTTP helper ────────────────────────────────────────────────────────────
def get(url: str, params: dict = None, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            log.warning(f"HTTP {r.status_code} on {url}")
            if r.status_code == 429:          # rate limited
                time.sleep(10)
            break
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return None

def save(name: str, data) -> None:
    path = DATA_DIR / f"{name}.json"
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    log.info(f"Saved {path} ({path.stat().st_size // 1024} KB)")

# ── STANDINGS ─────────────────────────────────────────────────────────────
def fetch_standings() -> dict:
    log.info("Fetching standings...")
    data = get(f"{BASE}/standings/now")
    if not data:
        return {}

    teams = {}
    for t in data.get("standings", []):
        ab = (t.get("teamAbbrev") or {}).get("default", "")
        if not ab:
            continue
        gp = t.get("gamesPlayed", 1) or 1
        teams[ab] = {
            "abbrev":      ab,
            "name":        (t.get("teamName") or {}).get("default", ab),
            "conference":  t.get("conferenceName", ""),
            "division":    t.get("divisionName", ""),
            "gp":          gp,
            "w":           t.get("wins", 0),
            "l":           t.get("losses", 0),
            "otl":         t.get("otLosses", 0),
            "pts":         t.get("points", 0),
            "pts_pct":     round(t.get("pointPctg", 0), 4),
            "gf":          t.get("goalFor", 0),
            "ga":          t.get("goalAgainst", 0),
            "gf_pg":       round(t.get("goalFor", 0) / gp, 3),
            "ga_pg":       round(t.get("goalAgainst", 0) / gp, 3),
            "home_w":      t.get("homeWins", 0),
            "home_l":      t.get("homeLosses", 0),
            "road_w":      t.get("roadWins", 0),
            "road_l":      t.get("roadLosses", 0),
            "l10_w":       t.get("l10Wins", 0),
            "l10_l":       t.get("l10Losses", 0),
            "streak":      t.get("streakCode", ""),
            "streak_n":    t.get("streakCount", 0),
            "reg_wins":    t.get("regulationWins", 0),
        }

    result = {"updated": datetime.now(timezone.utc).isoformat(), "teams": teams}
    save("standings", result)
    log.info(f"Standings: {len(teams)} teams")
    return result

# ── SKATER STATS ──────────────────────────────────────────────────────────
def fetch_skater_stats() -> list:
    log.info("Fetching skater stats...")
    all_players = []
    start = 0
    limit = 100

    while True:
        data = get(f"{STATS}/skater/summary", params={
            "isAggregate":      "false",
            "isGame":           "false",
            "factCayenneExp":   "gamesPlayed>=5",
            "cayenneExp":       f"gameTypeId=2 and seasonId={SEASON}",
            "start":            start,
            "limit":            limit,
            "sort":             '[{"property":"points","direction":"DESC"}]',
        })
        if not data or "data" not in data:
            break
        chunk = data["data"]
        if not chunk:
            break
        all_players.extend(chunk)
        log.info(f"  Skaters fetched: {len(all_players)}")
        if len(chunk) < limit:
            break
        start += limit
        time.sleep(0.5)

    # Fetch advanced stats and merge
    adv_data = get(f"{STATS}/skater/advanced", params={
        "isAggregate": "false",
        "isGame":      "false",
        "factCayenneExp": "gamesPlayed>=5",
        "cayenneExp":  f"gameTypeId=2 and seasonId={SEASON}",
        "start":       0,
        "limit":       1000,
    })
    if adv_data and "data" in adv_data:
        adv_map = {p["playerId"]: p for p in adv_data["data"]}
        for p in all_players:
            adv = adv_map.get(p.get("playerId"), {})
            p["cfPct"]               = adv.get("corsiForPct", adv.get("cfPct"))
            p["ffPct"]               = adv.get("fenwickForPct", adv.get("ffPct"))
            p["xgfPct"]              = adv.get("xGoalsForPct")
            p["hdcfPct"]             = adv.get("highDangerChancesForPct")
            p["offensiveZoneStartPct"] = adv.get("offensiveZoneStartPct")
            p["gameScore"]           = adv.get("gameScore")
        log.info(f"  Advanced stats merged for {len(adv_map)} players")

    result = {"updated": datetime.now(timezone.utc).isoformat(), "players": all_players}
    save("skaters", result)
    log.info(f"Skaters total: {len(all_players)}")
    return all_players

# ── GOALIE STATS ──────────────────────────────────────────────────────────
def fetch_goalie_stats() -> list:
    log.info("Fetching goalie stats...")
    data = get(f"{STATS}/goalie/summary", params={
        "isAggregate": "false",
        "isGame":      "false",
        "factCayenneExp": "gamesPlayed>=5",
        "cayenneExp":  f"gameTypeId=2 and seasonId={SEASON}",
        "start":       0,
        "limit":       200,
        "sort":        '[{"property":"wins","direction":"DESC"}]',
    })
    goalies = data.get("data", []) if data else []

    # Fetch advanced goalie stats
    adv = get(f"{STATS}/goalie/advanced", params={
        "isAggregate": "false",
        "isGame":      "false",
        "factCayenneExp": "gamesPlayed>=5",
        "cayenneExp":  f"gameTypeId=2 and seasonId={SEASON}",
        "start": 0, "limit": 200,
    })
    if adv and "data" in adv:
        adv_map = {g["playerId"]: g for g in adv["data"]}
        for g in goalies:
            a = adv_map.get(g.get("playerId"), {})
            g["gsaa"]          = a.get("goalsForAboveAverage")
            g["highDangerSvPct"] = a.get("highDangerSavePct")
            g["medDangerSvPct"]  = a.get("mediumDangerSavePct")
            g["lowDangerSvPct"]  = a.get("lowDangerSavePct")
            g["qualityStartPct"] = a.get("qualityStartPct")

    result = {"updated": datetime.now(timezone.utc).isoformat(), "goalies": goalies}
    save("goalies", result)
    log.info(f"Goalies: {len(goalies)}")
    return goalies

# ── TEAM ADVANCED STATS ───────────────────────────────────────────────────
def fetch_team_stats() -> dict:
    log.info("Fetching team advanced stats...")
    endpoints = {
        "summary":    f"{STATS}/team/summary",
        "advanced":   f"{STATS}/team/advanced",
        "percentages": f"{STATS}/team/percentages",
        "shooting":   f"{STATS}/team/shooting",
    }
    common_params = {
        "isAggregate": "false",
        "isGame":      "false",
        "cayenneExp":  f"gameTypeId=2 and seasonId={SEASON}",
        "start": 0, "limit": 50,
    }

    team_data = {}
    for key, url in endpoints.items():
        data = get(url, params=common_params)
        if data and "data" in data:
            for t in data["data"]:
                tid = t.get("teamId") or t.get("teamAbbrev", "")
                if tid not in team_data:
                    team_data[tid] = {}
                team_data[tid].update(t)
            log.info(f"  Team {key}: {len(data['data'])} records")
        time.sleep(0.3)

    result = {"updated": datetime.now(timezone.utc).isoformat(), "teams": team_data}
    save("team_advanced", result)
    return team_data

# ── PER-TEAM ROSTER + GAME LOG ────────────────────────────────────────────
def fetch_rosters() -> dict:
    log.info("Fetching all team rosters...")
    rosters = {}
    for ab in TEAMS:
        data = get(f"{BASE}/roster/{ab}/current")
        if data:
            players = []
            for section in ["forwards", "defensemen", "goalies"]:
                for p in data.get(section, []):
                    players.append({
                        "id":       p.get("id"),
                        "name":     (p.get("firstName") or {}).get("default","") + " " + (p.get("lastName") or {}).get("default",""),
                        "number":   p.get("sweaterNumber"),
                        "pos":      p.get("positionCode", section[0].upper()),
                        "shoots":   p.get("shootsCatches"),
                        "height":   p.get("heightInInches"),
                        "weight":   p.get("weightInPounds"),
                        "born":     p.get("birthDate"),
                        "country":  p.get("birthCountry"),
                    })
            rosters[ab] = players
            log.info(f"  {ab}: {len(players)} players")
        time.sleep(0.25)

    result = {"updated": datetime.now(timezone.utc).isoformat(), "rosters": rosters}
    save("rosters", result)
    return rosters

# ── LINE COMBINATIONS ─────────────────────────────────────────────────────
def fetch_line_combos() -> dict:
    """Fetch forward line and defense pair stats for all teams."""
    log.info("Fetching line combinations...")
    lines = {}

    for ab in TEAMS:
        # Forward lines (groups of 3)
        fwd_data = get(f"{STATS}/skater/faceoffwins", params={
            "isAggregate": "false",
            "isGame":      "false",
            "cayenneExp":  f"gameTypeId=2 and seasonId={SEASON} and teamAbbrev='{ab}'",
            "start": 0, "limit": 100,
        })

        # Use the on-ice combinations endpoint
        combo_data = get(f"{STATS}/skater/onIce", params={
            "isAggregate": "false",
            "isGame":      "false",
            "factCayenneExp": "timeOnIce>=300",
            "cayenneExp":  f"gameTypeId=2 and seasonId={SEASON} and teamAbbrev='{ab}'",
            "start": 0, "limit": 100,
            "sort": '[{"property":"timeOnIce","direction":"DESC"}]',
        })

        if combo_data and "data" in combo_data:
            lines[ab] = combo_data["data"]
        else:
            lines[ab] = []

        time.sleep(0.3)

    result = {"updated": datetime.now(timezone.utc).isoformat(), "lines": lines}
    save("lines", result)
    log.info(f"Line combos: {len(lines)} teams")
    return lines

# ── UPCOMING GAMES ────────────────────────────────────────────────────────
def fetch_schedule(days: int = 7) -> list:
    log.info(f"Fetching schedule ({days} days)...")
    from datetime import timedelta, date
    games = []
    today = date.today()

    for i in range(days):
        d = (today + timedelta(days=i)).isoformat()
        data = get(f"{BASE}/schedule/{d}")
        if not data:
            continue
        for week in data.get("gameWeek", []):
            for g in week.get("games", []):
                if g.get("gameType") != 2:
                    continue
                home = g.get("homeTeam", {})
                away = g.get("awayTeam", {})
                games.append({
                    "id":        g.get("id"),
                    "date":      g.get("gameDate"),
                    "time_utc":  g.get("startTimeUTC"),
                    "home":      home.get("abbrev"),
                    "away":      away.get("abbrev"),
                    "home_name": (home.get("commonName") or {}).get("default", ""),
                    "away_name": (away.get("commonName") or {}).get("default", ""),
                    "venue":     (g.get("venue") or {}).get("default", ""),
                    "state":     g.get("gameState", "FUT"),
                    "home_score": home.get("score"),
                    "away_score": away.get("score"),
                })
        time.sleep(0.2)

    result = {"updated": datetime.now(timezone.utc).isoformat(), "games": games}
    save("schedule", result)
    log.info(f"Schedule: {len(games)} games")
    return games

# ── META / LAST UPDATED ───────────────────────────────────────────────────
def save_meta(counts: dict) -> None:
    meta = {
        "updated":   datetime.now(timezone.utc).isoformat(),
        "season":    SEASON,
        "counts":    counts,
        "endpoints": {
            "standings":     "data/standings.json",
            "skaters":       "data/skaters.json",
            "goalies":       "data/goalies.json",
            "team_advanced": "data/team_advanced.json",
            "rosters":       "data/rosters.json",
            "lines":         "data/lines.json",
            "schedule":      "data/schedule.json",
        }
    }
    save("meta", meta)
    log.info(f"Meta saved: {meta}")

# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("NHL Data Scraper Starting")
    log.info("=" * 60)

    counts = {}

    standings = fetch_standings()
    counts["teams"] = len(standings.get("teams", {}))

    skaters = fetch_skater_stats()
    counts["skaters"] = len(skaters)

    goalies = fetch_goalie_stats()
    counts["goalies"] = len(goalies)

    fetch_team_stats()
    counts["team_advanced"] = len(TEAMS)

    rosters = fetch_rosters()
    counts["rosters"] = len(rosters)

    schedule = fetch_schedule(days=7)
    counts["upcoming_games"] = len(schedule)

    # Lines take longer — run last
    fetch_line_combos()
    counts["line_combos"] = len(TEAMS)

    save_meta(counts)

    log.info("=" * 60)
    log.info(f"Scrape complete: {counts}")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
