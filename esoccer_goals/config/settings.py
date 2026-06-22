"""
Configuración central del proyecto esoccer_goals.
Tarea 1: Esoccer Battle - 8 mins play (TotalCorner league_id 12995).
"""

# ── Liga objetivo ────────────────────────────────────────────────────────────
LEAGUE_ID_TC = 12995          # TotalCorner — 8 mins play
LEAGUE_ID_BETS = 22614        # BetsAPI  (sport_id=1 = Soccer, esoccer incluido)
LEAGUE_NAME = "Esoccer Battle - 8 mins play"

# Liga alternativa — 12 mins play
LEAGUE_12_ID_TC  = 12985
LEAGUE_12_NAME   = "Esoccer GT Leagues - 12 mins play"

# ── Rutas ────────────────────────────────────────────────────────────────────
import os, pathlib

BASE_DIR    = pathlib.Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "data"
LOG_DIR     = BASE_DIR / "logs"
DB_PATH     = DATA_DIR / "esoccer.db"          # liga 8 min
DB_PATH_12  = DATA_DIR / "esoccer_12min.db"   # liga 12 min

# ── TotalCorner ──────────────────────────────────────────────────────────────
TC_BASE_SITE  = "https://www.totalcorner.com"
TC_BASE_API   = "https://api.totalcorner.com/v1"

# Token VIP necesario para el endpoint API.
# Sin token: scraping de páginas públicas (resultados OK; cuotas O/U cerradas NO).
# Para obtener token: https://www.totalcorner.com/page/api  → contactar [email protected]
TC_API_TOKEN  = os.environ.get("TC_API_TOKEN", "")   # vacío = modo scraping

# ── BetsAPI ──────────────────────────────────────────────────────────────────
# Servicio de pago (desde $10/mes). sport_id=1 para fútbol/esoccer.
# Documentación: https://betsapi.com/docs/
BETS_BASE_API = "https://api.b365api.com/v3"
BETS_API_TOKEN = os.environ.get("BETS_API_TOKEN", "")   # vacío = no disponible

# ── Scraping ─────────────────────────────────────────────────────────────────
# Jugadores conocidos de la liga (se amplían en ejecución)
KNOWN_PLAYERS = [
    "hotShot", "Kray", "RossFCDK", "OG", "Kodak",
    "Wboy", "Hotshot",
]

KNOWN_PLAYERS_12 = [
    "Fox", "Hulk", "Kratos", "Jose", "Lio", "Banega", "David", "Rossi",
    "Jack", "Baba", "Arthur", "Crysis", "Shaolin", "Lucas", "Vendetta",
    "Furious", "Viper", "Kangal", "Fred", "Habibi", "Tifosi", "Delpiero",
    "Sensei", "Professor", "Razvan",
]

REQUEST_DELAY = 1.5     # segundos entre peticiones (respetar el servidor)
REQUEST_TIMEOUT = 15    # segundos

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Calidad de datos ─────────────────────────────────────────────────────────
# Número mínimo de partidos históricos a capturar en la carga inicial
MIN_MATCHES_TARGET = 200
