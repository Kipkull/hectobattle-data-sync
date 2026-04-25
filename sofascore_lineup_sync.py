"""GitHub Actions cron — pull Sofascore lineups for open matches and push to backend.

R137 Phase 2: Sofascore 双源策略, 错峰 Livescore 30 min, 互为 fallback.
腾讯云出口 IP 段被 sofascore 拦 (HTTP 403); GH Actions runner IP 池干净.
本脚本完全独立 (只 stdlib + httpx), 不 import backend/* 任何模块.

Workflow:
  1. GET  {API_BASE_URL}/api/admin/sync/sofascore-candidates
       → list of open matches with home/away_en + sofascore_event_id
  2. For matches missing sofascore_event_id:
       sofascore mobile API by date → fuzzy match home+away → POST patch-sofascore-event-id
  3. For each match with sofascore_event_id:
       sofascore lineup API → parse home.players + away.players (minutesPlayed > 0)
       → POST lineups-sofascore-push
  4. 输出统计行供 GH log 检阅.

Required ENV:
  API_BASE_URL  — e.g. https://hectobattle-api-244173-4-xxxxxxxxxx.sh.run.tcloudbase.com
  ADMIN_TOKEN   — must equal backend settings.ADMIN_TOKEN

Optional ENV:
  LIMIT             — debug; only process first N matches (default all)
  DRY_RUN           — '1' = no push, just log (default 0)
  REQUEST_DELAY_S   — sofascore inter-request delay seconds (default 2.0, ToS)

Exit codes: 0 always (let GH workflow run regardless; status reflected in logs).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("sofascore_sync")


# ── Config ──────────────────────────────────────────────────────────────

API_BASE_URL = os.environ.get("API_BASE_URL", "").rstrip("/")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
LIMIT = int(os.environ.get("LIMIT", "0") or "0")  # 0 = no limit
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
REQUEST_DELAY_S = float(os.environ.get("REQUEST_DELAY_S", "2.0"))

SOFASCORE_BASE = "https://api.sofascore.com/api/v1"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT_S = 30.0

# Sofascore returns positions as single letters: G/D/M/F
POSITION_MAP: dict[str, str | None] = {
    "G": "Goalkeeper",
    "D": "Defender",
    "M": "Midfielder",
    "F": "Forward",
}

NOISE_WORDS = {
    "fc", "cf", "sc", "afc", "club", "de", "la", "le", "the",
    "united", "city", "real", "sv", "vfl", "vfb", "tsg", "rcd", "rc",
    "us", "ss", "ac", "as", "ogc", "ssc", "1899", "1846",
    "1909", "1910", "1913", "04",
}


# ── HTTP helpers ────────────────────────────────────────────────────────


def _backend_headers() -> dict[str, str]:
    return {
        "X-Admin-Token": ADMIN_TOKEN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _sofascore_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
    }


# ── Backend client ──────────────────────────────────────────────────────


async def fetch_open_matches(client: httpx.AsyncClient) -> list[dict]:
    url = f"{API_BASE_URL}/api/admin/sync/sofascore-candidates"
    resp = await client.get(url, headers=_backend_headers(), timeout=TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(
            f"unexpected /sofascore-candidates payload type: {type(data).__name__}"
        )
    return data


async def patch_event_id(
    client: httpx.AsyncClient, match_id: int, event_id: str,
) -> None:
    url = f"{API_BASE_URL}/api/admin/sync/patch-sofascore-event-id"
    body = {"match_id": match_id, "sofascore_event_id": event_id}
    if DRY_RUN:
        logger.info("[DRY_RUN] would patch match=%d eid=%s", match_id, event_id)
        return
    resp = await client.post(
        url, headers=_backend_headers(), content=json.dumps(body), timeout=TIMEOUT_S,
    )
    resp.raise_for_status()


async def push_lineup(
    client: httpx.AsyncClient, match_id: int, lineup: dict,
) -> dict:
    url = f"{API_BASE_URL}/api/admin/sync/lineups-sofascore-push"
    body = {"match_id": match_id, "lineup": lineup}
    if DRY_RUN:
        logger.info(
            "[DRY_RUN] would push match=%d players=H%d/A%d",
            match_id,
            len(lineup.get("home_players") or []),
            len(lineup.get("away_players") or []),
        )
        return {"dry_run": True}
    resp = await client.post(
        url, headers=_backend_headers(), content=json.dumps(body), timeout=TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()


# ── Sofascore client ────────────────────────────────────────────────────


async def get_events_by_date(
    client: httpx.AsyncClient, date_str: str,
) -> list[dict] | None:
    """date_str: 'YYYY-MM-DD'. Returns events list or None on 404."""
    url = f"{SOFASCORE_BASE}/sport/football/scheduled-events/{date_str}"
    resp = await client.get(url, headers=_sofascore_headers(), timeout=TIMEOUT_S)
    await asyncio.sleep(REQUEST_DELAY_S)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise RuntimeError(f"sofascore date HTTP {resp.status_code} for {date_str}")
    payload = resp.json()
    if not isinstance(payload, dict):
        return []
    return payload.get("events") or []


async def get_lineup(
    client: httpx.AsyncClient, event_id: str,
) -> dict | None:
    """Returns parsed lineup dict, or None if no lineup published yet."""
    eid = str(event_id).strip()
    if not eid.isdigit():
        raise ValueError(f"invalid eid {eid!r}")

    url = f"{SOFASCORE_BASE}/event/{eid}/lineups"
    resp = await client.get(url, headers=_sofascore_headers(), timeout=TIMEOUT_S)
    await asyncio.sleep(REQUEST_DELAY_S)

    if resp.status_code == 404:
        return None  # lineup not published yet
    if resp.status_code != 200:
        raise RuntimeError(f"sofascore lineup HTTP {resp.status_code} for eid={eid}")

    payload = resp.json()
    return _parse_lineup_payload(payload, eid)


# ── Parse helpers ───────────────────────────────────────────────────────


def _parse_lineup_payload(payload: dict, eid: str) -> dict | None:
    if not isinstance(payload, dict):
        return None
    home = payload.get("home") or {}
    away = payload.get("away") or {}
    home_raw = home.get("players") or []
    away_raw = away.get("players") or []
    if not home_raw and not away_raw:
        return None

    home_players: list[dict] = []
    away_players: list[dict] = []
    for raw in home_raw:
        e = _parse_player(raw, "home")
        if e:
            home_players.append(e)
    for raw in away_raw:
        e = _parse_player(raw, "away")
        if e:
            away_players.append(e)

    return {
        "sofascore_event_id": str(eid),
        "home_players": home_players,
        "away_players": away_players,
    }


def _parse_player(raw: dict, team_side: str) -> dict | None:
    """Sofascore player entry → unified PlayerEntry.

    Filter rule: minutesPlayed > 0 (substitute=true 替补也算, 只要上场).
    """
    player = raw.get("player") or {}
    statistics = raw.get("statistics") or {}

    name = (player.get("name") or "").strip()
    if not name:
        return None

    minutes_raw = statistics.get("minutesPlayed")
    try:
        minutes_played = int(minutes_raw) if minutes_raw is not None else 0
    except (TypeError, ValueError):
        minutes_played = 0
    if minutes_played <= 0:
        return None

    pid = player.get("id")
    try:
        pid_int = int(pid) if pid is not None else None
    except (TypeError, ValueError):
        pid_int = None

    raw_pos = (player.get("position") or "").strip().upper()
    position = POSITION_MAP.get(raw_pos)
    if position is None and raw_pos:
        position = raw_pos.title()

    number = raw.get("shirtNumber") or player.get("shirtNumber") or player.get("jerseyNumber")
    try:
        shirt = int(number) if number not in (None, "") else None
    except (TypeError, ValueError):
        shirt = None

    is_starter = not bool(raw.get("substitute"))

    return {
        "name": name,
        "sofascore_player_id": pid_int,
        "number": shirt,
        "position": position,
        "minutes_played": minutes_played,
        "is_starter": is_starter,
        "team_side": team_side,
    }


def _normalize_team(name: str) -> set[str]:
    if not name:
        return set()
    words = (
        name.lower().replace("-", " ").replace(".", " ").replace("'", "").split()
    )
    return {w for w in words if w not in NOISE_WORDS and len(w) >= 3}


def _team_match(a: set[str], b: set[str]) -> bool:
    if not a or not b:
        return False
    overlap = a & b
    if not overlap:
        return False
    return len(overlap) >= min(len(a), len(b))


# ── Phase 1: backfill sofascore_event_id ────────────────────────────────


async def backfill_event_ids(
    client: httpx.AsyncClient, matches: list[dict],
) -> tuple[int, int]:
    """Returns (backfilled_count, attempted)."""
    missing = [m for m in matches if not m.get("sofascore_event_id")]
    if not missing:
        logger.info("backfill: nothing to do (all open matches already have eid)")
        return (0, 0)

    groups: dict[str, list[dict]] = defaultdict(list)
    for m in missing:
        d = m.get("match_date")
        if not d:
            continue
        groups[d].append(m)

    backfilled = 0
    attempted = 0

    for date_str, group in groups.items():
        try:
            events = await get_events_by_date(client, date_str)
        except Exception as e:
            logger.warning("sofascore date=%s fetch error: %s", date_str, e)
            continue
        if not events:
            logger.warning("sofascore date=%s no events", date_str)
            continue

        # Build event pool for this date
        event_pool: list[tuple[set[str], set[str], str]] = []
        for ev in events:
            home = ev.get("homeTeam") or {}
            away = ev.get("awayTeam") or {}
            eid = ev.get("id")
            if not eid:
                continue
            hn = _normalize_team(home.get("name") or "")
            an = _normalize_team(away.get("name") or "")
            # also include shortName if available
            hs = _normalize_team(home.get("shortName") or "")
            as_ = _normalize_team(away.get("shortName") or "")
            hn_combined = hn | hs
            an_combined = an | as_
            if hn_combined and an_combined:
                event_pool.append((hn_combined, an_combined, str(eid)))

        for m in group:
            attempted += 1
            home = m.get("home_team_en") or m.get("home_team") or ""
            away = m.get("away_team_en") or m.get("away_team") or ""
            hn = _normalize_team(home)
            an = _normalize_team(away)
            matched_eid: str | None = None
            for ev_h, ev_a, eid in event_pool:
                if _team_match(hn, ev_h) and _team_match(an, ev_a):
                    matched_eid = eid
                    break
            if not matched_eid:
                logger.info(
                    "backfill: no match date=%s '%s' vs '%s' in %d events",
                    date_str, home, away, len(event_pool),
                )
                continue
            try:
                await patch_event_id(client, m["id"], matched_eid)
                m["sofascore_event_id"] = matched_eid
                backfilled += 1
                logger.info(
                    "backfill: match=%d '%s' vs '%s' eid=%s",
                    m["id"], home, away, matched_eid,
                )
            except Exception as e:
                logger.warning("patch eid match=%d failed: %s", m["id"], e)

    return (backfilled, attempted)


# ── main ────────────────────────────────────────────────────────────────


async def main() -> int:
    if not API_BASE_URL or not ADMIN_TOKEN:
        logger.error("API_BASE_URL and ADMIN_TOKEN env vars are required")
        return 0

    logger.info(
        "config: api=%s dry_run=%s limit=%d delay=%.1fs",
        API_BASE_URL, DRY_RUN, LIMIT, REQUEST_DELAY_S,
    )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            matches = await fetch_open_matches(client)
        except Exception as e:
            logger.exception("fetch_open_matches failed: %s", e)
            return 0
        logger.info("got %d open matches", len(matches))

        if LIMIT > 0:
            matches = matches[:LIMIT]
            logger.info("LIMIT=%d → 截断到 %d 条", LIMIT, len(matches))

        try:
            backfilled, attempted = await backfill_event_ids(client, matches)
            logger.info(
                "backfill phase: %d/%d matches got eid (skipped %d already-set)",
                backfilled, attempted, len(matches) - attempted,
            )
        except Exception as e:
            logger.exception("backfill phase fatal: %s", e)

        successes = 0
        no_lineup = 0
        failures = 0
        skipped = 0
        for m in matches:
            eid = m.get("sofascore_event_id")
            if not eid:
                skipped += 1
                continue

            try:
                lineup = await get_lineup(client, eid)
            except Exception as e:
                logger.warning(
                    "match=%d eid=%s lineup fetch error: %s", m["id"], eid, e,
                )
                failures += 1
                continue

            if not lineup:
                logger.info("match=%d eid=%s: no lineup yet", m["id"], eid)
                no_lineup += 1
                continue

            try:
                resp = await push_lineup(client, m["id"], lineup)
            except httpx.HTTPStatusError as e:
                body_excerpt = ""
                try:
                    body_excerpt = e.response.text[:300]
                except Exception:
                    pass
                logger.warning(
                    "match=%d push HTTP %s: %s",
                    m["id"], e.response.status_code, body_excerpt,
                )
                failures += 1
                continue
            except Exception as e:
                logger.warning("match=%d push error: %s", m["id"], e)
                failures += 1
                continue

            items = resp.get("items_synced") if isinstance(resp, dict) else None
            logger.info(
                "match=%d eid=%s pushed: items=%s", m["id"], eid, items,
            )
            successes += 1

        total = len(matches)
        logger.info(
            "DONE: total=%d push_ok=%d no_lineup=%d failures=%d skipped(no-eid)=%d",
            total, successes, no_lineup, failures, skipped,
        )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
