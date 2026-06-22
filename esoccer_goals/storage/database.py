"""
Esquema SQLite y capa de acceso.

Tablas:
  players        - jugadores normalizados (la unidad real, no el equipo)
  matches        - partidos completados con resultado
  odds_snapshots - cuotas O/U de goles con timestamp (open / close / inplay)
"""

import sqlite3
import logging
from pathlib import Path

log = logging.getLogger(__name__)


DDL = """
-- ── Jugadores ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS players (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical     TEXT NOT NULL UNIQUE,  -- nombre normalizado (minúsculas, sin espacios)
    display_name  TEXT NOT NULL,         -- nombre tal como aparece en la fuente
    created_at    TEXT NOT NULL DEFAULT (datetime('now','utc'))
);

-- ── Partidos ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matches (
    id              INTEGER PRIMARY KEY,  -- match_id de TotalCorner
    league_id       INTEGER NOT NULL DEFAULT 12995,
    match_dt_utc    TEXT NOT NULL,        -- ISO-8601 UTC  e.g. "2026-06-16T03:04:12"
    player_home_id  INTEGER NOT NULL REFERENCES players(id),
    player_away_id  INTEGER NOT NULL REFERENCES players(id),
    team_home       TEXT NOT NULL,        -- nombre del equipo controlado (decorativo)
    team_away       TEXT NOT NULL,
    goals_home      INTEGER,             -- NULL si partido no terminado
    goals_away      INTEGER,
    ht_goals_home   INTEGER,             -- goles primer tiempo
    ht_goals_away   INTEGER,
    status          TEXT NOT NULL DEFAULT 'ended',  -- ended | live | pending
    source          TEXT NOT NULL DEFAULT 'tc_scrape',
    ingested_at     TEXT NOT NULL DEFAULT (datetime('now','utc'))
);

CREATE INDEX IF NOT EXISTS idx_matches_dt ON matches(match_dt_utc);
CREATE INDEX IF NOT EXISTS idx_matches_phid ON matches(player_home_id);
CREATE INDEX IF NOT EXISTS idx_matches_paid ON matches(player_away_id);

-- ── Cuotas O/U de goles ─────────────────────────────────────────────────────
-- Una fila por snapshot de cuota: open, close, o inplay.
-- La cuota de CIERRE es el snapshot con snap_type='close' (más cercano al saque).
-- Si solo hay un valor (línea sin precio), price_over/under quedan NULL.
CREATE TABLE IF NOT EXISTS odds_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id     INTEGER NOT NULL REFERENCES matches(id),
    snap_type    TEXT NOT NULL CHECK(snap_type IN ('open','close','inplay','unknown')),
    goal_line    REAL NOT NULL,          -- e.g. 5.5
    price_over   REAL,                  -- cuota decimal para Over (NULL si no disponible sin token)
    price_under  REAL,                  -- cuota decimal para Under
    snap_ts_utc  TEXT,                  -- timestamp del snapshot (ISO UTC, NULL si no disponible)
    source       TEXT NOT NULL DEFAULT 'tc_scrape',
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now','utc'))
);

CREATE INDEX IF NOT EXISTS idx_odds_match ON odds_snapshots(match_id, snap_type);

-- ── Log de ingesta ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ingest_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at      TEXT NOT NULL DEFAULT (datetime('now','utc')),
    source      TEXT NOT NULL,
    matches_new INTEGER NOT NULL DEFAULT 0,
    odds_new    INTEGER NOT NULL DEFAULT 0,
    errors      INTEGER NOT NULL DEFAULT 0,
    notes       TEXT
);
"""


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = get_connection(db_path)
    conn.executescript(DDL)
    conn.commit()
    log.info("DB inicializada en %s", db_path)
    return conn


# ── Operaciones de escritura ─────────────────────────────────────────────────

def upsert_player(conn: sqlite3.Connection, display_name: str) -> int:
    """Inserta o recupera jugador; devuelve su id."""
    canonical = display_name.lower().strip().replace(" ", "_")
    conn.execute(
        "INSERT OR IGNORE INTO players (canonical, display_name) VALUES (?, ?)",
        (canonical, display_name),
    )
    row = conn.execute(
        "SELECT id FROM players WHERE canonical = ?", (canonical,)
    ).fetchone()
    return row["id"]


def upsert_match(conn: sqlite3.Connection, m: dict) -> tuple[int, bool]:
    """
    Inserta o actualiza un partido. Devuelve (match_id, is_new).
    m debe tener: id, match_dt_utc, player_home_id, player_away_id,
                  team_home, team_away, goals_home, goals_away,
                  ht_goals_home (opt), ht_goals_away (opt), source (opt)
    """
    existing = conn.execute(
        "SELECT id FROM matches WHERE id = ?", (m["id"],)
    ).fetchone()

    if existing:
        conn.execute(
            """UPDATE matches SET goals_home=?, goals_away=?,
               ht_goals_home=?, ht_goals_away=?, status='ended'
               WHERE id=?""",
            (m.get("goals_home"), m.get("goals_away"),
             m.get("ht_goals_home"), m.get("ht_goals_away"), m["id"]),
        )
        return m["id"], False
    else:
        conn.execute(
            """INSERT INTO matches
               (id, league_id, match_dt_utc, player_home_id, player_away_id,
                team_home, team_away, goals_home, goals_away,
                ht_goals_home, ht_goals_away, status, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                m["id"], m.get("league_id", 12995), m["match_dt_utc"],
                m["player_home_id"], m["player_away_id"],
                m["team_home"], m["team_away"],
                m.get("goals_home"), m.get("goals_away"),
                m.get("ht_goals_home"), m.get("ht_goals_away"),
                m.get("status", "ended"), m.get("source", "tc_scrape"),
            ),
        )
        return m["id"], True


def upsert_odds_snapshot(conn: sqlite3.Connection, o: dict) -> bool:
    """
    Inserta snapshot de cuota si no existe ya para (match_id, snap_type, goal_line).
    Devuelve True si fue inserción nueva.
    """
    existing = conn.execute(
        """SELECT id FROM odds_snapshots
           WHERE match_id=? AND snap_type=? AND goal_line=?""",
        (o["match_id"], o["snap_type"], o["goal_line"]),
    ).fetchone()
    if existing:
        return False
    conn.execute(
        """INSERT INTO odds_snapshots
           (match_id, snap_type, goal_line, price_over, price_under,
            snap_ts_utc, source)
           VALUES (?,?,?,?,?,?,?)""",
        (
            o["match_id"], o["snap_type"], o["goal_line"],
            o.get("price_over"), o.get("price_under"),
            o.get("snap_ts_utc"), o.get("source", "tc_scrape"),
        ),
    )
    return True


def log_run(conn: sqlite3.Connection, source: str,
            matches_new: int, odds_new: int, errors: int, notes: str = ""):
    conn.execute(
        """INSERT INTO ingest_log (source, matches_new, odds_new, errors, notes)
           VALUES (?,?,?,?,?)""",
        (source, matches_new, odds_new, errors, notes),
    )
    conn.commit()


# ── Consultas de calidad ─────────────────────────────────────────────────────

def quality_report(conn: sqlite3.Connection) -> dict:
    total_matches = conn.execute(
        "SELECT COUNT(*) FROM matches WHERE status='ended'"
    ).fetchone()[0]

    with_result = conn.execute(
        "SELECT COUNT(*) FROM matches WHERE goals_home IS NOT NULL AND goals_away IS NOT NULL"
    ).fetchone()[0]

    with_any_odds = conn.execute(
        "SELECT COUNT(DISTINCT match_id) FROM odds_snapshots"
    ).fetchone()[0]

    with_close_odds = conn.execute(
        "SELECT COUNT(DISTINCT match_id) FROM odds_snapshots WHERE snap_type='close'"
    ).fetchone()[0]

    with_close_price = conn.execute(
        """SELECT COUNT(DISTINCT match_id) FROM odds_snapshots
           WHERE snap_type='close' AND price_over IS NOT NULL"""
    ).fetchone()[0]

    date_range = conn.execute(
        "SELECT MIN(match_dt_utc), MAX(match_dt_utc) FROM matches"
    ).fetchone()

    total_players = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]

    return {
        "total_matches_ended": total_matches,
        "matches_with_result": with_result,
        "matches_with_any_odds": with_any_odds,
        "matches_with_close_line_only": with_close_odds,
        "matches_with_close_price_and_ts": with_close_price,
        "closing_line_coverage_pct": (
            round(with_close_price / with_result * 100, 1) if with_result else 0
        ),
        "date_min": date_range[0],
        "date_max": date_range[1],
        "total_players": total_players,
    }
