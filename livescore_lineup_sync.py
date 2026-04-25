"""GitHub Actions cron — pull Livescore lineups for open matches and push to backend.

R137 路径 B (最终稳定方案).
腾讯云出口 IP 段被 livescore 主站拦; GH Actions runner IP 池干净.
本脚本完全独立 (只 stdlib + httpx), 不 import backend/* 任何模块.

Workflow:
  1. GET  {API_BASE_URL}/api/admin/sync/livescore-candidates
       → list of open matches with home/away_en + livescore_event_id
  2. For matches missing livescore_event_id:
       livescore mobile API by date → fuzzy match home+away → POST patch-livescore-event-id
  3. For each match with livescore_event_id:
       livescore Next.js prefetch URL → parse → POST lineups-livescore-push
  4. 输出统计行供 GH log 检阅.

Required ENV:
  API_BASE_URL  — e.g. https://hectobattle-api-244173-4-xxxxxxxxxx.sh.run.tcloudbase.com
  ADMIN_TOKEN   — must equal backend settings.ADMIN_TOKEN

Optional ENV:
  LIMIT             — debug 用; 只处理前 N 个 match (默认全部)
  DRY_RUN           — '1' 时不调用 push 端点 (默认 0)
  REQUEST_DELAY_S   — 每次 livescore 请求间延时秒数 (默认 1.5)

Exit codes: 0 always (let GH workflow run regardless; status reflected in logs).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from typing import Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("livescore_sync")


# ── Config ──────────────────────────────────────────────────────────────

API_BASE_URL = os.environ.get("API_BASE_URL", "").rstrip("/")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
LIMIT = int(os.environ.get("LIMIT", "0") or "0")  # 0 = no limit
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
REQUEST_DELAY_S = float(os.environ.get("REQUEST_DELAY_S", "1.5"))

LIVESCORE_BASE = "https://www.livescore.com"
LIVESCORE_MOBILE = "https://prod-public-api.livescore.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT_S = 20.0
BUILD_ID_RE = re.compile(r'"buildId":"([A-Za-z0-9_-]{8,40})"')

# Position map mirrors backend.crawler.sources.livescore._POSITION_MAP
POSITION_MAP: dict[str, str | None] = {
    "GOALKEEPER": "Goalkeeper",
    "DEFENDER": "Defender",
    "MIDFIELDER": "Midfielder",
    "FORWARD": "Forward",
    "ATTACKER": "Forward",
    "SUBSTITUTE_PLAYER": None,
}

NOISE_WORDS = {
    "fc", "cf", "sc", "afc", "club", "de", "la", "le", "the",
    "united", "city", "real", "sv", "vfl", "vfb", "tsg", "rcd", "rc",
    "us", "ss", "ac", "as", "ogc", "ssc",
}


# ── HTTP helpers ────────────────────────────────────────────────────────


def _backend_headers() -> dict[str, str]:
    return {
        "X-Admin-Token": ADMIN_TOKEN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _livescore_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }


# ── Backend client ──────────────────────────────────────────────────────


async def fetch_open_matches(client: httpx.AsyncClient) -> list[dict]:
    url = f"{API_BASE_URL}/api/admin/sync/livescore-candidates"
    resp = await client.get(url, headers=_backend_headers(), timeout=TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected /livescore-candidates payload type: {type(data).__name__}")
    return data


async def patch_event_id(
    client: httpx.AsyncClient, match_id: int, event_id: str,
) -> None:
    url = f"{API_BASE_URL}/api/admin/sync/patch-livescore-event-id"
    body = {"match_id": match_id, "livescore_event_id": event_id}
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
    url = f"{API_BASE_URL}/api/admin/sync/lineups-livescore-push"
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


# ── Livescore client (stateless, per-process buildId cache) ────────────


_build_id_cache: tuple[str, float] | None = None
_BUILD_ID_TTL = 3600.0


async def get_build_id(
    client: httpx.AsyncClient, *, force_refresh: bool = False,
) -> str:
    global _build_id_cache
    now = time.monotonic()
    if not force_refresh and _build_id_cache is not None:
        bid, ts = _build_id_cache
        if now - ts < _BUILD_ID_TTL:
            return bid
    resp = await client.get(
        f"{LIVESCORE_BASE}/en/", headers=_livescore_headers(), timeout=TIMEOUT_S,
    )
    await asyncio.sleep(REQUEST_DELAY_S)
    if resp.status_code != 200:
        raise RuntimeError(f"buildId fetch HTTP {resp.status_code}")
    m = BUILD_ID_RE.search(resp.text)
    if not m:
        raise RuntimeError("buildId not found in homepage HTML")
    bid = m.group(1)
    _build_id_cache = (bid, now)
    logger.info("livescore buildId refreshed: %s", bid)
    return bid


async def get_fixtures_by_date(
    client: httpx.AsyncClient, date_str: str,
) -> dict | None:
    """date_str: 'YYYY-MM-DD'."""
    compact = date_str.replace("-", "")
    if len(compact) != 8 or not compact.isdigit():
        raise ValueError(f"invalid date {date_str!r}")
    url = f"{LIVESCORE_MOBILE}/v1/api/app/date/soccer/{compact}/0"
    resp = await client.get(url, headers=_livescore_headers(), timeout=TIMEOUT_S)
    await asyncio.sleep(REQUEST_DELAY_S)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise RuntimeError(f"mobile date HTTP {resp.status_code} for {compact}")
    return resp.json()


async def get_lineup(
    client: httpx.AsyncClient,
    event_id: str,
    *,
    country_slug: str = "x",
    league_slug: str = "y",
) -> dict | None:
    """Returns parsed lineup dict, or None if no lineup section yet.

    Raises on HTTP error.
    """
    eid = str(event_id).strip()
    if not eid.isdigit():
        raise ValueError(f"invalid eid {eid!r}")
    country = (country_slug or "x").strip("/ ") or "x"
    league = (league_slug or "y").strip("/ ") or "y"
    match_slug = "x-vs-y"

    async def _fetch() -> httpx.Response:
        bid = await get_build_id(client)
        url = (
            f"{LIVESCORE_BASE}/_next/data/{bid}/en/football/"
            f"{country}/{league}/{match_slug}/{eid}/lineups.json"
        )
        return await client.get(url, headers=_livescore_headers(), timeout=TIMEOUT_S)

    resp = await _fetch()
    await asyncio.sleep(REQUEST_DELAY_S)

    # buildId 过期 → 强刷一次
    if resp.status_code == 404:
        await get_build_id(client, force_refresh=True)
        resp = await _fetch()
        await asyncio.sleep(REQUEST_DELAY_S)

    if resp.status_code == 404:
        return None  # eid 无效或 lineup 未公布
    if resp.status_code != 200:
        raise RuntimeError(f"livescore HTTP {resp.status_code} for eid={eid}")

    payload = resp.json()
    return _parse_lineup_payload(payload, eid)


# ── Parse helpers (mirror livescore.py) ─────────────────────────────────


def _parse_lineup_payload(payload: dict, eid: str) -> dict | None:
    pp = (payload or {}).get("pageProps") or {}
    initial = pp.get("initialEventData") or {}
    event = initial.get("event") or {}
    lineups = event.get("lineups") or {}
    if not lineups:
        return None
    home_starters = lineups.get("homeStarters") or []
    away_starters = lineups.get("awayStarters") or []
    home_subs = lineups.get("homeSubs") or []
    away_subs = lineups.get("awaySubs") or []
    if not home_starters and not away_starters:
        return None

    full_time = 90
    home_players: list[dict] = []
    away_players: list[dict] = []
    for p in home_starters:
        e = _parse_player(p, "home", True, full_time)
        if e:
            home_players.append(e)
    for p in away_starters:
        e = _parse_player(p, "away", True, full_time)
        if e:
            away_players.append(e)
    for p in home_subs:
        e = _parse_player(p, "home", False, full_time)
        if e:
            home_players.append(e)
    for p in away_subs:
        e = _parse_player(p, "away", False, full_time)
        if e:
            away_players.append(e)

    return {
        "livescore_event_id": str(eid),
        "home_team": event.get("homeName"),
        "away_team": event.get("awayName"),
        "match_status": event.get("status") or "",
        "home_players": home_players,
        "away_players": away_players,
        "raw_full_time": full_time,
    }


def _parse_player(
    p: dict, team_side: str, is_starter: bool, full_time: int,
) -> dict | None:
    name = (p.get("name") or "").strip()
    if not name:
        return None
    pid = p.get("playerId") or p.get("id")
    try:
        pid_int = int(pid) if pid is not None else None
    except (TypeError, ValueError):
        pid_int = None

    raw_pos = p.get("position") or ""
    position = POSITION_MAP.get(raw_pos)
    if position is None and raw_pos and raw_pos != "SUBSTITUTE_PLAYER":
        position = raw_pos.title()

    number = p.get("number")
    try:
        shirt = int(number) if number not in (None, "") else None
    except (TypeError, ValueError):
        shirt = None

    sub = p.get("sub") or {}
    sub_type = (sub.get("subType") or "").strip()
    sub_time_raw = sub.get("time")
    try:
        sub_time = int(sub_time_raw) if sub_time_raw not in (None, "") else None
    except (TypeError, ValueError):
        sub_time = None

    if is_starter:
        if sub_type == "FootballSubOut" and sub_time is not None:
            minutes_played = max(sub_time, 0)
        else:
            minutes_played = full_time
    else:
        if sub_type != "FootballSubIn" or sub_time is None:
            return None
        minutes_played = max(full_time - sub_time, 0)

    return {
        "name": name,
        "livescore_player_id": pid_int,
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


# ── Phase 1: backfill livescore_event_id ────────────────────────────────


async def backfill_event_ids(
    client: httpx.AsyncClient, matches: list[dict],
) -> tuple[int, int, dict[str, str]]:
    """Returns (backfilled_count, attempted, eid_slug_map).

    eid_slug_map[eid] = "country_slug/league_slug" — 副产物, lineup 阶段直接复用,
    省一次 mobile API 调用.
    """
    missing = [m for m in matches if not m.get("livescore_event_id")]
    if not missing:
        logger.info("backfill: nothing to do (all open matches already have eid)")
        return (0, 0, {})

    # 按日期分组 → 每个日期 1 次 mobile API
    groups: dict[str, list[dict]] = defaultdict(list)
    for m in missing:
        d = m.get("match_date")
        if not d:
            continue
        groups[d].append(m)

    backfilled = 0
    attempted = 0
    eid_slugs: dict[str, str] = {}

    for date_str, group in groups.items():
        try:
            payload = await get_fixtures_by_date(client, date_str)
        except Exception as e:
            logger.warning("mobile date=%s fetch error: %s", date_str, e)
            continue
        if not payload:
            logger.warning("mobile date=%s no payload", date_str)
            continue

        # 摊平 events
        event_pool: list[tuple[set[str], set[str], str, str, str]] = []
        for stage in payload.get("Stages") or []:
            ccd = (stage.get("Ccd") or "").lower()
            scd = (stage.get("Scd") or stage.get("CompUrlName") or "").lower()
            for ev in stage.get("Events") or []:
                t1 = (ev.get("T1") or [{}])[0]
                t2 = (ev.get("T2") or [{}])[0]
                eid = ev.get("Eid")
                if not eid:
                    continue
                hn = _normalize_team(t1.get("Nm") or "")
                an = _normalize_team(t2.get("Nm") or "")
                if hn and an:
                    event_pool.append((hn, an, str(eid), ccd, scd))

        for m in group:
            attempted += 1
            home = m.get("home_team_en") or m.get("home_team") or ""
            away = m.get("away_team_en") or m.get("away_team") or ""
            hn = _normalize_team(home)
            an = _normalize_team(away)
            matched: tuple[str, str, str] | None = None
            for ev_h, ev_a, eid, ccd, scd in event_pool:
                if _team_match(hn, ev_h) and _team_match(an, ev_a):
                    matched = (eid, ccd, scd)
                    break
            if not matched:
                logger.info(
                    "backfill: no match date=%s '%s' vs '%s' in %d events",
                    date_str, home, away, len(event_pool),
                )
                continue
            eid, ccd, scd = matched
            try:
                await patch_event_id(client, m["id"], eid)
                m["livescore_event_id"] = eid  # 内存同步
                if ccd and scd:
                    eid_slugs[eid] = f"{ccd}/{scd}"
                backfilled += 1
                logger.info(
                    "backfill: match=%d '%s' vs '%s' eid=%s slug=%s/%s",
                    m["id"], home, away, eid, ccd, scd,
                )
            except Exception as e:
                logger.warning("patch eid match=%d failed: %s", m["id"], e)

    return (backfilled, attempted, eid_slugs)


# ── Phase 2: fetch + push lineups ───────────────────────────────────────


async def resolve_slug_for_eid(
    client: httpx.AsyncClient, match: dict,
) -> tuple[str, str] | None:
    """对已有 eid 但缺 slug 信息的 match, 通过 mobile API 反查 slug."""
    date_str = match.get("match_date")
    target_eid = str(match.get("livescore_event_id") or "")
    home = match.get("home_team_en") or match.get("home_team") or ""
    away = match.get("away_team_en") or match.get("away_team") or ""
    if not date_str or not target_eid:
        return None
    try:
        payload = await get_fixtures_by_date(client, date_str)
    except Exception as e:
        logger.warning("slug-resolve mobile date=%s error: %s", date_str, e)
        return None
    if not payload:
        return None
    for stage in payload.get("Stages") or []:
        ccd = (stage.get("Ccd") or "").lower()
        scd = (stage.get("Scd") or stage.get("CompUrlName") or "").lower()
        for ev in stage.get("Events") or []:
            if str(ev.get("Eid") or "") == target_eid:
                if ccd and scd:
                    return (ccd, scd)
        # fallback: fuzzy 队名 (eid 不一致时也接受)
        hn = _normalize_team(home)
        an = _normalize_team(away)
        for ev in stage.get("Events") or []:
            t1 = (ev.get("T1") or [{}])[0]
            t2 = (ev.get("T2") or [{}])[0]
            if _team_match(hn, _normalize_team(t1.get("Nm") or "")) and _team_match(
                an, _normalize_team(t2.get("Nm") or "")
            ):
                if ccd and scd:
                    return (ccd, scd)
    return None


async def main() -> int:
    if not API_BASE_URL or not ADMIN_TOKEN:
        logger.error("API_BASE_URL and ADMIN_TOKEN env vars are required")
        return 0  # 不让 GH 报红, log 已显式输出

    logger.info(
        "config: api=%s dry_run=%s limit=%d delay=%.1fs",
        API_BASE_URL, DRY_RUN, LIMIT, REQUEST_DELAY_S,
    )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # 1. open matches
        try:
            matches = await fetch_open_matches(client)
        except Exception as e:
            logger.exception("fetch_open_matches failed: %s", e)
            return 0
        logger.info("got %d open matches", len(matches))

        if LIMIT > 0:
            matches = matches[:LIMIT]
            logger.info("LIMIT=%d → 截断到 %d 条", LIMIT, len(matches))

        # 2. backfill missing eid
        try:
            backfilled, attempted, eid_slugs = await backfill_event_ids(client, matches)
            logger.info(
                "backfill phase: %d/%d matches got eid (skipped %d already-set)",
                backfilled, attempted, len(matches) - attempted,
            )
        except Exception as e:
            logger.exception("backfill phase fatal: %s", e)
            eid_slugs = {}

        # 3. lineup pull + push
        successes = 0
        no_lineup = 0
        failures = 0
        skipped = 0
        for m in matches:
            eid = m.get("livescore_event_id")
            if not eid:
                skipped += 1
                continue

            slug = eid_slugs.get(eid)
            if not slug:
                resolved = await resolve_slug_for_eid(client, m)
                if not resolved:
                    logger.info(
                        "match=%d eid=%s: no slug resolved, skip",
                        m["id"], eid,
                    )
                    failures += 1
                    continue
                slug = f"{resolved[0]}/{resolved[1]}"
                eid_slugs[eid] = slug

            country_slug, league_slug = slug.split("/", 1)

            try:
                lineup = await get_lineup(
                    client, eid,
                    country_slug=country_slug,
                    league_slug=league_slug,
                )
            except Exception as e:
                logger.warning("match=%d eid=%s lineup fetch error: %s", m["id"], eid, e)
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
                "match=%d eid=%s pushed: items=%s",
                m["id"], eid, items,
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
