"""
main.py — Punto de entrada para la ingesta de datos esoccer_goals.

Uso:
  python main.py              # Scraping gratuito (resultados + goal_line parcial)
  python main.py --api        # API TotalCorner (requiere TC_API_TOKEN en env)
  python main.py --report     # Solo imprime reporte de calidad de la BD actual
  python main.py --pages N    # Número de páginas de API a descargar (default: 5)

Variables de entorno:
  TC_API_TOKEN    Token VIP de TotalCorner (para modo --api)
  BETS_API_TOKEN  Token BetsAPI (no implementado en Tarea 1, solo documentado)
"""

import argparse
import logging
import sys
from pathlib import Path

# Añadir raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import DB_PATH, DB_PATH_12, LOG_DIR, LEAGUE_12_ID_TC, KNOWN_PLAYERS_12
from storage.database import init_db, log_run, quality_report
from ingestion.totalcorner import (
    api_available, scrape_league_all_players, api_ingest_with_odds,
)


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "esoccer.log"
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[
            logging.FileHandler(str(log_file), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def print_report(report: dict):
    print("\n" + "=" * 60)
    print("REPORTE DE CALIDAD — esoccer_goals Tarea 1")
    print("=" * 60)
    print(f"  Partidos terminados en BD:          {report['total_matches_ended']:>6}")
    print(f"  Partidos con resultado (goles):     {report['matches_with_result']:>6}")
    print(f"  Partidos con ALGUNA cuota O/U:      {report['matches_with_any_odds']:>6}")
    print(f"  Partidos con línea de cierre:       {report['matches_with_close_line_only']:>6}")
    print(f"  Partidos con cierre + precio + ts:  {report['matches_with_close_price_and_ts']:>6}")
    print(f"  COBERTURA CUOTA DE CIERRE (%):      {report['closing_line_coverage_pct']:>5.1f}%")
    print(f"  Jugadores normalizados:             {report['total_players']:>6}")
    print(f"  Rango temporal: {report['date_min']}  →  {report['date_max']}")
    print("=" * 60)

    # Diagnóstico clave
    cov = report["closing_line_coverage_pct"]
    if cov == 0:
        print("\n⚠️  DIAGNÓSTICO: Cuota de cierre con precio y timestamp = 0%")
        print("   El scraping gratuito no proporciona precios/timestamps de cuotas.")
        print("   Para obtenerlos se necesita:")
        print("   a) Token VIP de TotalCorner → set TC_API_TOKEN y usar --api")
        print("   b) Token BetsAPI ($10/mes) → ver ingestion/betsapi_notes.md")
        print("   Recomendación: solicitar token TC o contratar BetsAPI básico.")
    elif cov < 50:
        print(f"\n⚠️  DIAGNÓSTICO: Cobertura de cierre baja ({cov}%).")
        print("   Puede deberse a partidos sin cuota en la fuente o a gaps históricos.")
    else:
        print(f"\n✅  Cobertura de cuota de cierre aceptable ({cov}%).")


def main():
    setup_logging()
    log = logging.getLogger("main")

    parser = argparse.ArgumentParser(description="Ingesta datos esoccer Tarea 1")
    parser.add_argument("--api", action="store_true",
                        help="Usar API TotalCorner (requiere TC_API_TOKEN)")
    parser.add_argument("--report", action="store_true",
                        help="Solo mostrar reporte de calidad")
    parser.add_argument("--pages", type=int, default=5,
                        help="Páginas de API a descargar (default 5 = ~150 partidos)")
    parser.add_argument("--max-pages", type=int, default=1,
                        help="Páginas por jugador en scraping (default 1 = ~40 partidos recientes; "
                             "usar 15 para backfill historico completo)")
    parser.add_argument("--league", type=int, default=8, choices=[8, 12],
                        help="Liga a scrapear: 8 (8 mins, default) o 12 (12 mins)")
    args = parser.parse_args()

    if args.league == 12:
        db_path    = DB_PATH_12
        league_id  = LEAGUE_12_ID_TC
        extra_pl   = KNOWN_PLAYERS_12
    else:
        db_path    = DB_PATH
        league_id  = None   # usa el default de settings (12995)
        extra_pl   = None

    conn = init_db(db_path)
    log.info("BD: %s", DB_PATH)

    if args.report:
        report = quality_report(conn)
        print_report(report)
        return

    if args.api:
        if not api_available():
            log.error("TC_API_TOKEN no encontrado en variables de entorno.")
            print("Configura TC_API_TOKEN=<tu_token> antes de usar --api")
            sys.exit(1)
        log.info("Modo API TotalCorner (páginas=%d)", args.pages)
        result = api_ingest_with_odds(conn, pages=args.pages)
        source = "tc_api"
    else:
        log.info("Modo scraping gratuito (liga=%d min, max_pages=%d)", args.league, args.max_pages)
        result = scrape_league_all_players(conn, extra_players=extra_pl,
                                           max_pages=args.max_pages, league_id=league_id)
        source = "tc_scrape"

    log_run(
        conn, source,
        matches_new=result.get("matches_new", 0),
        odds_new=result.get("odds_new", 0),
        errors=result.get("errors", 0),
    )

    log.info("Resultado: %s", result)
    report = quality_report(conn)
    print_report(report)


if __name__ == "__main__":
    main()
