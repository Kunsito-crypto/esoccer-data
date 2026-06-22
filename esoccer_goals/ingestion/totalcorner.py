"""
Ingesta de datos desde TotalCorner para Esoccer Battle - 8 mins play (liga 12995).

ENDPOINTS GRATUITOS (sin token, verificados 2026-06-16):
  /league/view/12995
    → Lista todos los jugadores de la liga (36 jugadores activos)
    → URL player: /esoccer-player/{name}/12995
  /esoccer-player/{name}/12995
    → Tabla de partidos con data-match_id, fecha, equipos (player en paréntesis)
    → Scores están bloqueados (fa-lock) en esta página
  /match/ajax_get_history/{match_id}
    → Historial de eventos: scores HT y FT ("X-Y score at the end of Second Half")
    → Goles y corners minuto a minuto — GRATIS
  /match/odds-handicap/{match_id}
    → Historial completo de cuotas:
        - Asian Handicap (tablas 0-1)
        - 1X2 (tabla 3)
        - Over/Under FT (tabla 5): Over, line, Under, timestamp por snapshot
        - Over/Under HT (tabla 6)
    → La fila más antigua (score='0 - 0', time='0') = CUOTA DE CIERRE pre-partido
    → ~40-50 snapshots por partido — GRATIS

ENDPOINTS DE API (requieren TC_API_TOKEN = VIP):
  GET /v1/league/schedule/{league_id}?token=TOKEN&type=ended&columns=goalLine
  GET /v1/match/odds/{match_id}?token=TOKEN&columns=goalList
  → Más estructurado, paginado, con campo `start` en Unix timestamp
  → Sin token: {"success":0,"error":"TOKEN_ERROR","msg":"token not provided"}
"""

import re
import time
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

from config.settings import (
    TC_BASE_SITE, TC_BASE_API, TC_API_TOKEN,
    LEAGUE_ID_TC, REQUEST_DELAY, REQUEST_TIMEOUT, HEADERS,
)
from storage.database import (
    upsert_player, upsert_match, upsert_odds_snapshot,
)

log = logging.getLogger(__name__)

PLAYER_URL_RE = re.compile(r"/esoccer-player/([^/]+)/\d+")
MATCH_ID_RE   = re.compile(r'data-match_id=["\'](\d+)["\']')
SCORE_HT_RE   = re.compile(r"(\d+)-(\d+)\s+score at the end of First Half", re.I)
SCORE_FT_RE   = re.compile(r"(\d+)-(\d+)\s+score at the end of Second Half", re.I)
# Busca el primer (jugador) en el string — sin ancla $ porque la celda puede tener
# texto adicional pegado (stats/score) en filas con datos completos (15 celdas).
PLAYER_RE     = re.compile(r"\(([^)]+)\)")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None,
         extra_headers: dict = None) -> Optional[requests.Response]:
    h = dict(HEADERS)
    if extra_headers:
        h.update(extra_headers)
    try:
        resp = requests.get(url, params=params, headers=h, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        log.warning("HTTP error %s: %s", url, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Utilidades de parseo
# ─────────────────────────────────────────────────────────────────────────────

def _parse_player(team_str: str) -> tuple[str, str]:
    """
    "Real Madrid (hotShot)" → ("Real Madrid", "hotShot")
    Si no hay paréntesis → (team_str, "unknown")
    """
    m = PLAYER_RE.search(team_str.strip())
    if m:
        player = m.group(1).strip()
        team = team_str[: team_str.find("(")].strip()
        return team, player
    return team_str.strip(), "unknown"


def _normalize_dt(raw: str, year: int = None) -> Optional[str]:
    """
    Acepta: "2026-06-16 11:38:52", "06-16 11:38", "MM/DD HH:MM"
    Devuelve ISO-8601 UTC string (TotalCorner usa hora UK ≈ UTC para esoccer).
    """
    raw = raw.strip()
    yr = year or datetime.now(timezone.utc).year
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%m-%d %H:%M",
        "%m/%d %H:%M",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=yr)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None


def _parse_goal_line(raw: str) -> Optional[float]:
    """
    "5.5,6.0" → 5.5 (toma la primera)
    "6" → 6.0
    "5" → 5.0
    """
    if not raw:
        return None
    parts = raw.split(",")
    try:
        return float(parts[0].strip())
    except (ValueError, IndexError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Descubrir jugadores de la liga (público, sin token)
# ─────────────────────────────────────────────────────────────────────────────

def discover_players(league_id: int = None) -> list[str]:
    """
    Scrapea la página de la liga y extrae todos los nombres de jugadores.
    """
    lid = league_id or LEAGUE_ID_TC
    url = f"{TC_BASE_SITE}/league/view/{lid}"
    resp = _get(url)
    if resp is None:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    players = [
        m.group(1)
        for a in soup.find_all("a", href=PLAYER_URL_RE)
        for m in [PLAYER_URL_RE.search(a["href"])] if m
    ]
    players = list(dict.fromkeys(players))  # dedup conservando orden
    log.info("Jugadores descubiertos: %d → %s", len(players), players)
    return players


# ─────────────────────────────────────────────────────────────────────────────
# Partidos de un jugador (lista de match IDs)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_player_match_ids(player_name: str, max_pages: int = 1, league_id: int = None) -> list[dict]:
    """
    Scrapea /esoccer-player/{name}/12995[?page=N].
    La página muestra ~40 partidos; hay hasta 9+ páginas por jugador.

    max_pages=1 → solo la primera página (modo rápido, ~40 partidos)
    max_pages=9 → hasta 9 páginas (~360 partidos, historia profunda)

    Devuelve lista de dicts: {match_id, date_raw, team_home, player_home,
                              team_away, player_away, status}
    Score NO disponible aquí (requiere VIP en esta tabla).
    """
    results = []

    lid = league_id or LEAGUE_ID_TC
    for page_num in range(1, max_pages + 1):
        # TotalCorner usa path-based pagination: /page:N  (no query param ?page=N)
        if page_num == 1:
            url = f"{TC_BASE_SITE}/esoccer-player/{player_name}/{lid}"
        else:
            url = f"{TC_BASE_SITE}/esoccer-player/{player_name}/{lid}/page:{page_num}"
        resp = _get(url)
        if resp is None:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        if len(tables) < 2:
            break

        rows = tables[1].find_all("tr")[1:]  # skip header
        page_results = []
        for row in rows:
            mid = row.get("data-match_id")
            if not mid:
                continue
            cells = row.find_all(["td", "th"])
            if len(cells) < 5:
                continue
            date_raw  = cells[0].get_text(strip=True)
            status    = cells[1].get_text(strip=True)
            home_raw  = cells[2].get_text(strip=True)
            away_raw  = cells[4].get_text(strip=True)

            team_h, player_h = _parse_player(home_raw)
            team_a, player_a = _parse_player(away_raw)

            page_results.append({
                "match_id":    int(mid),
                "date_raw":    date_raw,
                "team_home":   team_h,
                "player_home": player_h,
                "team_away":   team_a,
                "player_away": player_a,
                "status":      status,
            })

        if not page_results:
            break  # no more pages

        results.extend(page_results)

        # Si la página tiene menos de 35 registros, probablemente es la última
        if len(page_results) < 35:
            break

        if page_num < max_pages:
            time.sleep(REQUEST_DELAY)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Score HT/FT vía ajax_get_history (público, sin token)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_match_scores(match_id: int) -> dict:
    """
    GET /match/ajax_get_history/{match_id}
    Extrae score HT ("X-Y score at the end of First Half") y
    score FT ("X-Y score at the end of Second Half").
    También extrae fecha exacta del partido de la página de detalle.
    Devuelve: {goals_home, goals_away, ht_goals_home, ht_goals_away, match_dt_utc}
    """
    url = f"{TC_BASE_SITE}/match/ajax_get_history/{match_id}"
    resp = _get(url, extra_headers={
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{TC_BASE_SITE}/match/corner-stats/{match_id}",
    })
    if resp is None:
        return {}

    text = resp.text
    data = {}

    # Score FT
    m_ft = SCORE_FT_RE.search(text)
    if m_ft:
        data["goals_home"] = int(m_ft.group(1))
        data["goals_away"] = int(m_ft.group(2))

    # Score HT
    m_ht = SCORE_HT_RE.search(text)
    if m_ht:
        data["ht_goals_home"] = int(m_ht.group(1))
        data["ht_goals_away"] = int(m_ht.group(2))

    # Fecha exacta (la página live-stats tiene el datetime completo)
    dt_resp = _get(f"{TC_BASE_SITE}/match/live-stats/{match_id}")
    if dt_resp:
        dt_pattern = re.compile(r"20\d\d-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}")
        m_dt = dt_pattern.search(dt_resp.text)
        if m_dt:
            data["match_dt_utc"] = _normalize_dt(m_dt.group(0))

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Cuotas O/U vía odds-handicap (público, sin token)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_match_odds(match_id: int) -> list[dict]:
    """
    GET /match/odds-handicap/{match_id}
    Extrae historial de cuotas Over/Under FT.

    Estructura de la tabla 5 (Over/Under FT):
      col 0: minuto ("00 '", "01 '", ... "06 '", "half", "0")
      col 1: score en ese instante ("0 - 0", "1 - 0", ...)
      col 2: precio Over (e.g., "1.850")
      col 3: línea de goles (e.g., "5.5,6.0" o "6.5")
      col 4: precio Under (e.g., "1.850")
      col 5: timestamp (e.g., "06-16 11:38")

    Ordenado del más reciente (top) al más antiguo (bottom).
    Fila más antigua con minute='0' y score='0 - 0' = cuota pre-partido
      → la usamos como CLOSING LINE (es la última actualización pre-partido).

    Devuelve lista de dicts de snapshots para odds_snapshots.
    """
    url = f"{TC_BASE_SITE}/match/odds-handicap/{match_id}"
    resp = _get(url, extra_headers={
        "Referer": f"{TC_BASE_SITE}/match/corner-stats/{match_id}",
    })
    if resp is None:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")

    # Tabla 5 = Over/Under Full Time
    if len(tables) < 6:
        log.debug("match %d: menos de 6 tablas en odds-handicap", match_id)
        return []

    ou_table = tables[5]
    headers = [th.get_text(strip=True) for th in ou_table.find_all("th")]
    if "Over" not in headers:
        log.debug("match %d: tabla 5 no es O/U (headers=%s)", match_id, headers)
        return []

    rows = ou_table.find_all("tr")[1:]  # skip header
    if not rows:
        return []

    # Identificar cuáles filas son pre-partido (score = "0 - 0" y minuto = '0')
    # y cuáles son cuota de apertura / cierre
    snapshots = []
    pre_match_rows = []
    year = datetime.now(timezone.utc).year

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 6:
            continue
        minute_raw, score_raw, over_raw, line_raw, under_raw, ts_raw = cells[:6]

        try:
            price_over  = float(over_raw)
            price_under = float(under_raw)
        except ValueError:
            continue

        goal_line = _parse_goal_line(line_raw)
        if goal_line is None:
            continue

        ts_utc = _normalize_dt(ts_raw, year=year)

        # Pre-partido: score "0 - 0" y minuto indica 0 minutos jugados.
        # TotalCorner usa distintos formatos: "0", "0'", "0 '", "00'", "00 '".
        is_pre = (
            score_raw.strip() == "0 - 0"
            and bool(re.match(r"^0+\s*'?$", minute_raw.strip()))
        )

        snap = {
            "match_id":    match_id,
            "snap_type":   "unknown",  # se reclasifica abajo
            "goal_line":   goal_line,
            "price_over":  price_over,
            "price_under": price_under,
            "snap_ts_utc": ts_utc,
            "source":      "tc_odds_page",
            "_is_pre":     is_pre,
            "_minute_raw": minute_raw,
            "_score_raw":  score_raw,
        }
        snapshots.append(snap)
        if is_pre:
            pre_match_rows.append(snap)

    # Clasificar: la tabla está ordenada newest→oldest (top=newest, bottom=oldest)
    # El snapshot más antiguo (último) es el de APERTURA del mercado.
    # El snapshot pre-partido más reciente (el primero de los pre_match_rows) = CIERRE.
    if pre_match_rows:
        # La lista original está newest→oldest; pre_match_rows mantiene ese orden
        # → pre_match_rows[0] = el más reciente pre-partido = CLOSING LINE
        pre_match_rows[0]["snap_type"] = "close"
        if len(pre_match_rows) > 1:
            pre_match_rows[-1]["snap_type"] = "open"
        for snap in pre_match_rows[1:-1]:
            snap["snap_type"] = "inplay"  # pre-match intermediate (raro)
    elif snapshots:
        # Fallback: buscar snapshots con score "0 - 0" aunque el minuto no sea exactamente "0"
        # El más reciente de esos es la mejor aproximación a la cuota de cierre.
        zero_score = [s for s in snapshots
                      if s.get("_score_raw", "").strip() == "0 - 0"]
        if zero_score:
            zero_score[0]["snap_type"] = "close"   # newest (tabla newest→oldest)
            if len(zero_score) > 1:
                zero_score[-1]["snap_type"] = "open"
        else:
            # Sin ninguna fila pre-partido: el snapshot más antiguo como open
            snapshots[-1]["snap_type"] = "open"

    # Limpiar campos internos antes de devolver
    for s in snapshots:
        s.pop("_is_pre", None)
        s.pop("_minute_raw", None)
        s.pop("_score_raw", None)

    return snapshots


# ─────────────────────────────────────────────────────────────────────────────
# Ingesta completa de un partido
# ─────────────────────────────────────────────────────────────────────────────

def ingest_match(match_id: int, base_info: dict,
                 conn: sqlite3.Connection) -> tuple[bool, int]:
    """
    Ingesta completa de un partido:
      1. Scores vía ajax_get_history (gratis)
      2. Fecha exacta vía live-stats (gratis)
      3. Cuotas O/U vía odds-handicap (gratis)

    base_info necesita: date_raw, team_home, player_home, team_away, player_away

    Devuelve (match_is_new, odds_added).
    """
    # Skip si ya tenemos resultado Y cuota de cierre — evita HTTP innecesario en re-runs
    _existing = conn.execute(
        "SELECT goals_home FROM matches WHERE id=? AND goals_home IS NOT NULL",
        (match_id,),
    ).fetchone()
    _close = conn.execute(
        "SELECT id FROM odds_snapshots WHERE match_id=? AND snap_type='close' LIMIT 1",
        (match_id,),
    ).fetchone()
    if _existing and _close:
        return False, 0

    # Scores
    time.sleep(REQUEST_DELAY)
    scores = fetch_match_scores(match_id)

    # Jugadores
    ph_id = upsert_player(conn, base_info.get("player_home", "unknown"))
    pa_id = upsert_player(conn, base_info.get("player_away", "unknown"))

    # Fecha: preferir la exacta de live-stats, si no la del player_page
    match_dt = scores.get("match_dt_utc") or _normalize_dt(
        base_info.get("date_raw", ""), year=datetime.now(timezone.utc).year
    ) or ""

    match_row = {
        "id":            match_id,
        "match_dt_utc":  match_dt,
        "player_home_id": ph_id,
        "player_away_id": pa_id,
        "team_home":     base_info.get("team_home", ""),
        "team_away":     base_info.get("team_away", ""),
        "goals_home":    scores.get("goals_home"),
        "goals_away":    scores.get("goals_away"),
        "ht_goals_home": scores.get("ht_goals_home"),
        "ht_goals_away": scores.get("ht_goals_away"),
        "source":        "tc_scrape",
    }
    _, is_new = upsert_match(conn, match_row)

    # Cuotas O/U
    time.sleep(REQUEST_DELAY)
    odds_snaps = fetch_match_odds(match_id)
    odds_added = 0
    for snap in odds_snaps:
        if upsert_odds_snapshot(conn, snap):
            odds_added += 1

    conn.commit()
    log.debug("match %d: new=%s, scores=%s-%s, odds=%d",
              match_id, is_new,
              scores.get("goals_home"), scores.get("goals_away"), odds_added)
    return is_new, odds_added


# ─────────────────────────────────────────────────────────────────────────────
# Orquestación principal
# ─────────────────────────────────────────────────────────────────────────────

def scrape_league_all_players(conn: sqlite3.Connection,
                               extra_players: list[str] = None,
                               max_pages: int = 1,
                               league_id: int = None) -> dict:
    """
    Pipeline completo de scraping gratuito:
      1. Descubrir jugadores de la liga
      2. Para cada jugador: obtener lista de match IDs
      3. Para cada match ID: score + cuotas O/U
    """
    players = discover_players(league_id=league_id)
    if extra_players:
        players = list(dict.fromkeys(players + extra_players))

    # Colectar todos los match IDs únicos con su info base
    all_matches: dict[int, dict] = {}
    for player in players:
        log.info("Jugador: %s", player)
        time.sleep(REQUEST_DELAY)
        entries = fetch_player_match_ids(player, max_pages=max_pages, league_id=league_id)
        for e in entries:
            mid = e["match_id"]
            if mid not in all_matches:
                all_matches[mid] = e
            else:
                # Actualizar con info de jugador si mejora lo que ya tenemos
                ex = all_matches[mid]
                if ex["player_home"] == "unknown" and e["player_home"] != "unknown":
                    ex["player_home"] = e["player_home"]
                    ex["team_home"]   = e["team_home"]
                if ex["player_away"] == "unknown" and e["player_away"] != "unknown":
                    ex["player_away"] = e["player_away"]
                    ex["team_away"]   = e["team_away"]
            # Solo incluir partidos terminados
            # (status = "Full" o cualquier texto que no sea un número de minuto)
        log.info("  → %d partidos en lista (acumulado total: %d)",
                 len(entries), len(all_matches))

    log.info("Total match IDs únicos: %d", len(all_matches))

    matches_new = 0
    odds_new = 0
    errors = 0

    for i, (mid, base_info) in enumerate(all_matches.items()):
        # Solo procesar partidos completados
        status = base_info.get("status", "Full")
        if not (status == "Full" or "full" in status.lower()):
            continue

        try:
            is_new, odds_added = ingest_match(mid, base_info, conn)
            if is_new:
                matches_new += 1
            odds_new += odds_added
        except Exception as exc:
            log.error("Error en match %d: %s", mid, exc)
            errors += 1

        if (i + 1) % 20 == 0:
            log.info("Progreso: %d/%d matches procesados (nuevos=%d, odds=%d)",
                     i + 1, len(all_matches), matches_new, odds_new)

    return {
        "matches_new": matches_new,
        "odds_new": odds_new,
        "errors": errors,
        "players_processed": len(players),
        "match_ids_found": len(all_matches),
    }


# ─────────────────────────────────────────────────────────────────────────────
# API TotalCorner (requiere TC_API_TOKEN)
# ─────────────────────────────────────────────────────────────────────────────

def api_available() -> bool:
    return bool(TC_API_TOKEN)


def api_ingest_with_odds(conn: sqlite3.Connection, pages: int = 5) -> dict:
    """
    Ingesta vía API TotalCorner (más estructurada, paginada):
      GET /v1/league/schedule/12995?type=ended&columns=goalLine
      GET /v1/match/odds/{match_id}?columns=goalList

    La cuota de cierre del API viene en goalList:
      [status, line, price_over, price_under, timestamp_unix, goals_current]
    El último elemento con status=0 (pre-partido) = closing line.

    Requiere TC_API_TOKEN (VIP). Sin token no funciona.
    """
    if not api_available():
        log.error("TC_API_TOKEN no configurado. Usa scraping gratuito en su lugar.")
        return {"matches_new": 0, "odds_new": 0, "errors": 1}

    matches_new = 0
    odds_new = 0
    errors = 0

    for page in range(1, pages + 1):
        log.info("API: página %d/%d", page, pages)
        resp = _get(
            f"{TC_BASE_API}/league/schedule/{LEAGUE_ID_TC}",
            params={"token": TC_API_TOKEN, "type": "ended",
                    "columns": "goalLine", "page": page},
        )
        if resp is None:
            errors += 1
            break

        data = resp.json()
        if not data.get("success"):
            log.error("API error: %s", data)
            errors += 1
            break

        raw_matches = data.get("data", [])
        if not raw_matches:
            break

        for rm in raw_matches:
            mid = rm.get("id")
            if not mid:
                continue

            team_h_raw = rm.get("h", "")
            team_a_raw = rm.get("a", "")
            team_h, player_h = _parse_player(team_h_raw)
            team_a, player_a = _parse_player(team_a_raw)

            ph_id = upsert_player(conn, player_h)
            pa_id = upsert_player(conn, player_a)

            # start puede ser unix timestamp o string
            start_raw = str(rm.get("start", ""))
            try:
                # Unix timestamp
                dt = datetime.utcfromtimestamp(int(start_raw))
                match_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
            except (ValueError, TypeError):
                match_dt = _normalize_dt(start_raw) or ""

            match_row = {
                "id":            mid,
                "match_dt_utc":  match_dt,
                "player_home_id": ph_id,
                "player_away_id": pa_id,
                "team_home":     team_h,
                "team_away":     team_a,
                "goals_home":    rm.get("hg"),
                "goals_away":    rm.get("ag"),
                "source":        "tc_api",
            }
            _, is_new = upsert_match(conn, match_row)
            if is_new:
                matches_new += 1

            # goalList = historial de cuotas pre-partido e inplay
            time.sleep(REQUEST_DELAY)
            odds_resp = _get(
                f"{TC_BASE_API}/match/odds/{mid}",
                params={"token": TC_API_TOKEN, "columns": "goalList"},
            )
            if odds_resp:
                od = odds_resp.json()
                if od.get("success"):
                    goal_list = od.get("data", {}).get("goalList", [])
                    snaps = _parse_api_goal_list(mid, goal_list)
                    for snap in snaps:
                        if upsert_odds_snapshot(conn, snap):
                            odds_new += 1

            conn.commit()
        time.sleep(REQUEST_DELAY)

    return {"matches_new": matches_new, "odds_new": odds_new, "errors": errors}


def _parse_api_goal_list(match_id: int, goal_list: list) -> list[dict]:
    """
    Formato goalList de la API TC:
      [match_status, goal_line, price_over, price_under, ts_unix, goals_current]
    match_status: 0=pre-match, 1=first half, 2=HT, 3=second half, 5=ended
    Cuota de CIERRE = último elemento con status=0.
    """
    if not goal_list:
        return []

    pre_match = [e for e in goal_list if isinstance(e, list) and len(e) >= 5 and e[0] == 0]
    snapshots = []
    for i, entry in enumerate(pre_match):
        status, line, p_over, p_under, ts_raw = entry[:5]
        try:
            ts_utc = datetime.utcfromtimestamp(int(ts_raw)).strftime("%Y-%m-%dT%H:%M:%S")
        except (ValueError, TypeError):
            ts_utc = None
        snap_type = "close" if i == len(pre_match) - 1 else ("open" if i == 0 else "inplay")
        snapshots.append({
            "match_id":    match_id,
            "snap_type":   snap_type,
            "goal_line":   float(line),
            "price_over":  float(p_over) if p_over else None,
            "price_under": float(p_under) if p_under else None,
            "snap_ts_utc": ts_utc,
            "source":      "tc_api_odds",
        })
    return snapshots
