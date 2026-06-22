"""
bsports_scraper.py — Backfill desde bsportsfan.com (~110k partidos, Ene 1 → hoy)

Ligas soportadas:
  22614  Esoccer Battle 8-min  (~55k partidos)
  23114  Esoccer GT Leagues 12-min (~55k partidos)

Fase 1: raspa páginas de lista → match_id, dt, equipos, jugadores, FT scores
Fase 2: raspa páginas de detalle → ht_h, ht_a (marcador de descanso)

La BD resultante (bsports.db) NO tiene odds. Se usa para enriquecer
las features H2H y de jugador de los modelos que sí tienen odds (TotalCorner).

Uso:
  python bsports_scraper.py --phase 1                         # raspa 8-min (default)
  python bsports_scraper.py --phase 1 --league-id 23114      # raspa 12-min GT Leagues
  python bsports_scraper.py --phase 1 --start-page 500       # reanuda desde pág 500
  python bsports_scraper.py --phase 2 --batch 15000          # rellena HT en tanda
  python bsports_scraper.py --phase 1 --dry-run 5            # prueba con 5 páginas
"""
from __future__ import annotations

import argparse
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: pip install requests beautifulsoup4 lxml")
    sys.exit(1)

ROOT     = Path(__file__).parent
DATA_DIR = ROOT / "data"
DB_PATH  = DATA_DIR / "bsports.db"

BASE_URL = "https://es.bsportsfan.com"

# Ligas conocidas: {league_id: (slug_url, etiqueta_corta)}
KNOWN_LEAGUES: dict[int, tuple[str, str]] = {
    22614: ("esoccer-battle--8-mins-play",       "8min"),
    23114: ("esoccer-gt-leagues--12-mins-play",  "12min"),
}
DEFAULT_LEAGUE_ID = 22614

MADRID_TZ = ZoneInfo("Europe/Madrid")
UTC_TZ    = ZoneInfo("UTC")

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer":         f"{BASE_URL}/",
}

MAX_RETRIES  = 3
RETRY_DELAY  = 5.0
LIST_DELAY   = (1.5, 2.8)   # (min, max) segundos entre páginas de lista
DETAIL_DELAY = (0.9, 1.6)   # segundos entre páginas de detalle


# ── BD ─────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS bsports_matches (
    match_id     INTEGER PRIMARY KEY,
    league_id    INTEGER NOT NULL DEFAULT 22614,
    dt           TEXT NOT NULL,
    home_team    TEXT NOT NULL,
    home_player  TEXT NOT NULL,
    away_team    TEXT NOT NULL,
    away_player  TEXT NOT NULL,
    ft_h         INTEGER NOT NULL,
    ft_a         INTEGER NOT NULL,
    ft_total     INTEGER NOT NULL,
    ht_h         INTEGER,
    ht_a         INTEGER,
    ht_total     INTEGER,
    detail_done  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_bsm_dt     ON bsports_matches(dt);
CREATE INDEX IF NOT EXISTS idx_bsm_pair   ON bsports_matches(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_bsm_league ON bsports_matches(league_id);

CREATE TABLE IF NOT EXISTS _progress (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    # Migración: añadir league_id si la BD ya existía sin esa columna
    cols = {r[1] for r in conn.execute("PRAGMA table_info(bsports_matches)")}
    if "league_id" not in cols:
        conn.execute(
            "ALTER TABLE bsports_matches ADD COLUMN league_id INTEGER NOT NULL DEFAULT 22614"
        )
        conn.commit()
    return conn


def get_progress(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute("SELECT value FROM _progress WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_progress(conn: sqlite3.Connection, key: str, value: str):
    conn.execute("INSERT OR REPLACE INTO _progress VALUES (?,?)", (key, str(value)))
    conn.commit()


def count_matches(conn: sqlite3.Connection, league_id: int | None = None) -> int:
    if league_id is None:
        return conn.execute("SELECT COUNT(*) FROM bsports_matches").fetchone()[0]
    return conn.execute(
        "SELECT COUNT(*) FROM bsports_matches WHERE league_id=?", (league_id,)
    ).fetchone()[0]


def count_pending_detail(conn: sqlite3.Connection, league_id: int | None = None) -> int:
    if league_id is None:
        return conn.execute(
            "SELECT COUNT(*) FROM bsports_matches WHERE detail_done=0"
        ).fetchone()[0]
    return conn.execute(
        "SELECT COUNT(*) FROM bsports_matches WHERE detail_done=0 AND league_id=?",
        (league_id,)
    ).fetchone()[0]


# ── HTTP ───────────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch(session: requests.Session, url: str) -> str | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 503):
                wait = RETRY_DELAY * attempt
                print(f"    [{r.status_code}] esperando {wait:.0f}s…")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            print(f"    HTTP {r.status_code} para {url}")
            return None
        except requests.RequestException as e:
            print(f"    Error red (intento {attempt}): {e}")
            time.sleep(RETRY_DELAY)
    return None


# ── PARSEO - LISTA ─────────────────────────────────────────────────────────────

_SCORE_HREF_RE  = re.compile(r"/soccer/r/(\d+)/")
_TEAM_HREF_RE   = re.compile(r"/soccer/t/\d+/")
_SCORE_TEXT_RE  = re.compile(r"^(\d+)-(\d+)$")
_DT_RE          = re.compile(r"(\d{2}/\d{2})\s+(\d{2}:\d{2})")
_TEAM_PLAYER_RE = re.compile(r"^(.+?)\s*\(([^)]+)\)\s*$")
_PAGES_RE       = re.compile(r"Página\s+\d+\s+de\s+(\d+)")


class _YearTracker:
    """Infiere el año correcto mientras se raspan páginas de más reciente a más antigua.

    El sitio muestra fechas como "MM/DD" sin año. Como las páginas van de la
    más nueva (p.1) a la más antigua (p.N), el mes solo puede disminuir o
    quedarse igual dentro del mismo año. Cuando el mes SUBE (p.ej. pasamos de
    enero=1 a diciembre=12), hemos cruzado un límite de año hacia atrás.
    """
    def __init__(self) -> None:
        now = datetime.now(tz=UTC_TZ)
        self.year        = now.year
        self._last_month = now.month

    def resolve(self, month: int, day: int, hour: int, minute: int) -> datetime:
        if month > self._last_month:
            self.year -= 1
        self._last_month = month
        return datetime(self.year, month, day, hour, minute, tzinfo=MADRID_TZ)


def _dt_to_utc_iso(date_str: str, time_str: str, tracker: _YearTracker) -> str:
    month, day   = map(int, date_str.split("/"))
    hour, minute = map(int, time_str.split(":"))
    dt_local = tracker.resolve(month, day, hour, minute)
    return dt_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:00")


def _split_team(raw: str) -> tuple[str, str]:
    m = _TEAM_PLAYER_RE.match(raw)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return raw.strip(), ""


def parse_list_page(html: str, tracker: _YearTracker) -> tuple[list[dict], int | None]:
    """
    Devuelve (lista_de_partidos, total_pages).
    Cada partido: {match_id, dt, home_team, home_player, away_team, away_player, ft_h, ft_a}
    """
    soup    = BeautifulSoup(html, "lxml")
    results = []
    seen    = set()

    # Total páginas (para saber cuándo parar)
    body_text   = soup.get_text(" ")
    pages_m     = _PAGES_RE.search(body_text)
    total_pages = int(pages_m.group(1)) if pages_m else None

    for score_link in soup.find_all("a", href=_SCORE_HREF_RE):
        href = score_link.get("href", "")
        mid_m = _SCORE_HREF_RE.search(href)
        if not mid_m:
            continue
        match_id = int(mid_m.group(1))
        if match_id in seen:
            continue
        seen.add(match_id)

        score_text = score_link.get_text(strip=True)
        sc_m = _SCORE_TEXT_RE.match(score_text)
        if not sc_m:
            continue
        ft_h, ft_a = int(sc_m.group(1)), int(sc_m.group(2))

        # Buscar contenedor con ≥2 enlaces de equipo
        container  = score_link.parent
        team_links = []
        for _ in range(12):
            if container is None:
                break
            team_links = container.find_all("a", href=_TEAM_HREF_RE)
            if len(team_links) >= 2:
                break
            container = container.parent

        if len(team_links) < 2:
            continue

        home_team, home_player = _split_team(team_links[0].get_text(strip=True))
        away_team, away_player = _split_team(team_links[1].get_text(strip=True))

        # Fecha/hora — buscar en el contenedor y hasta 3 niveles arriba
        dt_str   = ""
        time_str = ""
        node = container
        for _ in range(4):
            if node is None:
                break
            dt_m = _DT_RE.search(node.get_text(" ", strip=True))
            if dt_m:
                dt_str   = dt_m.group(1)
                time_str = dt_m.group(2)
                break
            node = node.parent

        if not dt_str:
            continue

        results.append({
            "match_id":    match_id,
            "dt":          _dt_to_utc_iso(dt_str, time_str, tracker),
            "home_team":   home_team,
            "home_player": home_player,
            "away_team":   away_team,
            "away_player": away_player,
            "ft_h":        ft_h,
            "ft_a":        ft_a,
        })

    return results, total_pages


# ── PARSEO - DETALLE ───────────────────────────────────────────────────────────

_HT_EVENT_RE = re.compile(r"Score After First Half\s*[-–]\s*(\d+)-(\d+)", re.I)
_HT_TABLE_RE = re.compile(r"HT\s+FT\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)")


def parse_detail_page(html: str) -> tuple[int | None, int | None]:
    text = BeautifulSoup(html, "lxml").get_text(" ")

    # Prioridad 1: "Score After First Half - X-Y"
    m = _HT_EVENT_RE.search(text)
    if m:
        return int(m.group(1)), int(m.group(2))

    # Prioridad 2: tabla HT/FT — "HT FT / h1 h2 / a1 a2"
    m2 = _HT_TABLE_RE.search(text)
    if m2:
        return int(m2.group(1)), int(m2.group(3))

    return None, None


# ── FASE 1 — Lista de resultados ───────────────────────────────────────────────

def phase1(conn: sqlite3.Connection, session: requests.Session,
           start_page: int, dry_run: int | None, delay: tuple[float, float],
           league_id: int = DEFAULT_LEAGUE_ID) -> None:

    slug, label = KNOWN_LEAGUES.get(league_id, ("x", str(league_id)))
    list_url    = f"{BASE_URL}/soccer/le/{league_id}/{slug}/p.{{page}}"
    prog_page   = f"phase1_last_page_{league_id}"
    prog_done   = f"phase1_done_{league_id}"

    current_page       = start_page
    total_pages        = None
    inserted           = 0
    completed          = False
    consecutive_errors = 0
    MAX_CONSECUTIVE    = 20
    tracker            = _YearTracker()

    print(f"\n=== FASE 1 [{label} / {league_id}] — desde página {start_page} ===")

    while True:
        if dry_run is not None and (current_page - start_page) >= dry_run:
            print(f"  Dry-run: parado tras {dry_run} páginas.")
            break
        if total_pages is not None and current_page > total_pages:
            print(f"  Página {current_page} > total {total_pages}. Fase 1 completa.")
            completed = True
            break

        url = list_url.format(page=current_page)
        print(f"  [{current_page}/{total_pages or '?'}] {url}", end=" … ", flush=True)

        html = fetch(session, url)
        if html is None:
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE:
                print(f"  {consecutive_errors} errores consecutivos — abortando (sitio bloqueado?).")
                break
            print("ERROR, saltando.")
            current_page += 1
            continue
        consecutive_errors = 0

        matches, tp = parse_list_page(html, tracker)
        if tp:
            total_pages = tp
        if not matches:
            print("0 partidos (¿última página?).")
            if current_page > (total_pages or current_page):
                break
            current_page += 1
            continue

        # Insertar en BD (incluyendo league_id)
        batch_inserted = 0
        for r in matches:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO bsports_matches
                        (match_id, league_id, dt, home_team, home_player,
                         away_team, away_player, ft_h, ft_a, ft_total)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (r["match_id"], league_id, r["dt"],
                      r["home_team"], r["home_player"],
                      r["away_team"], r["away_player"],
                      r["ft_h"], r["ft_a"], r["ft_h"] + r["ft_a"]))
                batch_inserted += conn.execute("SELECT changes()").fetchone()[0]
            except sqlite3.Error as e:
                print(f"    DB error: {e}")

        inserted += batch_inserted
        conn.commit()
        set_progress(conn, prog_page, str(current_page))

        total_league = count_matches(conn, league_id)
        print(f"{len(matches)} partidos, +{batch_inserted} nuevos (total liga: {total_league:,})")

        current_page += 1
        time.sleep(random.uniform(*delay))

    total = count_matches(conn)
    print(f"\n  Fase 1 terminada. Total BD global: {total:,} partidos.")
    if completed:
        set_progress(conn, prog_done, "1")
    else:
        print(f"  (Fase 1 NO marcada como completa — terminó por dry-run o errores.)")


# ── FASE 2 — Detalles HT ──────────────────────────────────────────────────────

def phase2(conn: sqlite3.Connection, session: requests.Session,
           batch: int, delay: tuple[float, float],
           league_id: int | None = None) -> None:

    _, label = KNOWN_LEAGUES.get(league_id or DEFAULT_LEAGUE_ID, ("x", str(league_id)))
    tag      = f"[{label} / {league_id}]" if league_id else "[todas las ligas]"

    pending = count_pending_detail(conn, league_id)
    total   = count_matches(conn, league_id)
    print(f"\n=== FASE 2 {tag} — Detalles HT (pendientes: {pending:,}/{total:,}) ===")

    if pending == 0:
        print("  Nada pendiente. Fase 2 completa.")
        return

    # Prioridad: matches más recientes primero (más valiosos para modelos actuales)
    if league_id is not None:
        rows = conn.execute("""
            SELECT match_id FROM bsports_matches
            WHERE detail_done = 0 AND league_id = ?
            ORDER BY dt DESC
            LIMIT ?
        """, (league_id, batch)).fetchall()
    else:
        rows = conn.execute("""
            SELECT match_id FROM bsports_matches
            WHERE detail_done = 0
            ORDER BY dt DESC
            LIMIT ?
        """, (batch,)).fetchall()

    done_ok = 0
    done_nf = 0

    for i, (match_id,) in enumerate(rows, 1):
        url  = f"{BASE_URL}/soccer/r/{match_id}/x"
        html = fetch(session, url)

        if html is None:
            conn.execute(
                "UPDATE bsports_matches SET detail_done=-1 WHERE match_id=?",
                (match_id,)
            )
        else:
            ht_h, ht_a = parse_detail_page(html)
            if ht_h is not None:
                conn.execute("""
                    UPDATE bsports_matches
                    SET ht_h=?, ht_a=?, ht_total=?, detail_done=1
                    WHERE match_id=?
                """, (ht_h, ht_a, ht_h + ht_a, match_id))
                done_ok += 1
            else:
                conn.execute(
                    "UPDATE bsports_matches SET detail_done=2 WHERE match_id=?",
                    (match_id,)
                )
                done_nf += 1

        if i % 100 == 0:
            conn.commit()
            remaining_est = (pending - i) * (sum(delay) / 2)
            print(f"  {i}/{len(rows)} ({done_ok} HT ok, {done_nf} sin HT) "
                  f"— est. {remaining_est/3600:.1f}h restantes")

    conn.commit()

    still_pending = count_pending_detail(conn)
    print(f"\n  Fase 2 tanda completa. HT ok={done_ok}, sin HT={done_nf}. "
          f"Pendientes restantes: {still_pending:,}")


# ── RESUMEN BD ─────────────────────────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection) -> None:
    total   = count_matches(conn)
    with_ht = conn.execute(
        "SELECT COUNT(*) FROM bsports_matches WHERE ht_h IS NOT NULL"
    ).fetchone()[0]
    oldest  = conn.execute("SELECT MIN(dt) FROM bsports_matches").fetchone()[0]
    newest  = conn.execute("SELECT MAX(dt) FROM bsports_matches").fetchone()[0]

    print(f"\nEstado bsports.db (GLOBAL):")
    print(f"  Total partidos:  {total:,}")
    print(f"  Con HT score:    {with_ht:,} ({100*with_ht/max(total,1):.1f}%)")
    print(f"  Pendiente HT:    {count_pending_detail(conn):,}")
    print(f"  Rango fechas:    {oldest} .. {newest}")

    # Desglose por liga
    for lid, (_, label) in KNOWN_LEAGUES.items():
        n       = count_matches(conn, lid)
        pend    = count_pending_detail(conn, lid)
        p1done  = get_progress(conn, f"phase1_done_{lid}", "0")
        p1page  = get_progress(conn, f"phase1_last_page_{lid}", "0")
        print(f"\n  Liga {lid} [{label}]:  {n:,} partidos  |  HT pendientes: {pend:,}"
              f"  |  Fase1 done={p1done} (última pág: {p1page})")


# ── CLI ─────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="bsportsfan.com scraper")
    parser.add_argument("--phase",      type=int, required=True, choices=[1, 2])
    parser.add_argument("--league-id",  type=int, default=DEFAULT_LEAGUE_ID,
                        choices=list(KNOWN_LEAGUES.keys()),
                        help=f"ID de liga (default: {DEFAULT_LEAGUE_ID} = 8-min Batalla)")
    parser.add_argument("--start-page", type=int, default=None)
    parser.add_argument("--batch",      type=int, default=15000)
    parser.add_argument("--dry-run",    type=int, default=None,
                        help="Fase 1: parar tras N páginas (prueba)")
    parser.add_argument("--db",         default=str(DB_PATH))
    parser.add_argument("--stats",      action="store_true")
    args = parser.parse_args()

    db_path   = Path(args.db)
    league_id = args.league_id
    conn      = init_db(db_path)
    _, label  = KNOWN_LEAGUES.get(league_id, ("x", str(league_id)))
    print(f"BD: {db_path}  |  Liga: {league_id} [{label}]")

    if args.stats:
        show_stats(conn)
        return

    session = make_session()

    if args.phase == 1:
        # Reanuda desde donde se quedó (por liga), o desde --start-page, o desde 1
        prog_key = f"phase1_last_page_{league_id}"
        if args.start_page:
            start = args.start_page
        else:
            last  = get_progress(conn, prog_key)
            start = int(last) + 1 if last else 1
        phase1(conn, session, start_page=start,
               dry_run=args.dry_run, delay=LIST_DELAY, league_id=league_id)
        show_stats(conn)

    elif args.phase == 2:
        phase2(conn, session, batch=args.batch, delay=DETAIL_DELAY, league_id=league_id)
        show_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
