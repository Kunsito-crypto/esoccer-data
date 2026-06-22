"""
bsports_feature_builder.py — Features walk-forward sobre bsports.db con Polars

Genera bsports_features.csv (~55k filas) con:
  Labels:   over_55, over_65, over_75, over_85
  H2H:      h2h_count, h2h_avg, h2h_std, h2h_o65, h2h_streak_3/5/10, h2h_ht_ratio
  Jugador:  player_home_avg, player_home_o65, player_away_avg, player_away_o65
  Tiempo:   hour_utc

Todas las features son walk-forward estrictas (shift(1) → sin leakage del partido actual).
Sin odds — para enriquecer features H2H/player de los modelos con odds de TotalCorner.

Uso:
  python bsports_feature_builder.py                     # defaults
  python bsports_feature_builder.py --db data/bsports.db --out data/bsports_features.csv
  python bsports_feature_builder.py --sample 10         # imprime 10 filas extra
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import polars as pl
except ImportError:
    print("ERROR: pip install 'polars>=1.0.0'")
    sys.exit(1)

ROOT     = Path(__file__).parent
DATA_DIR = ROOT / "data"
DB_PATH  = DATA_DIR / "bsports.db"
OUT_CSV  = DATA_DIR / "bsports_features.csv"


# ── CARGA ─────────────────────────────────────────────────────────────────────

def load_db(db_path: Path) -> pl.DataFrame:
    conn   = sqlite3.connect(str(db_path))
    cursor = conn.execute("""
        SELECT
            match_id,
            COALESCE(league_id, 22614) AS league_id,
            dt,
            home_team,
            home_player,
            away_team,
            away_player,
            ft_h,
            ft_a,
            COALESCE(ft_total, ft_h + ft_a) AS ft_total,
            ht_h,
            ht_a,
            ht_total
        FROM bsports_matches
        WHERE ft_h IS NOT NULL AND ft_a IS NOT NULL
        ORDER BY dt
    """)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    conn.close()

    if not rows:
        print("ERROR: bsports.db está vacío. Ejecuta primero bsports_scraper.py --phase 1")
        sys.exit(1)

    return pl.DataFrame({col: [r[i] for r in rows] for i, col in enumerate(cols)})


# ── FEATURES ─────────────────────────────────────────────────────────────────

def build_features(df: pl.DataFrame) -> pl.DataFrame:

    # Nombres combinados (formato TotalCorner: "Equipo (jugador)")
    df = df.with_columns([
        (pl.col("home_team") + pl.lit(" (") + pl.col("home_player") + pl.lit(")"))
            .alias("home"),
        (pl.col("away_team") + pl.lit(" (") + pl.col("away_player") + pl.lit(")"))
            .alias("away"),
    ])

    # Clave de par normalizada: min|max para que A vs B = B vs A
    df = df.with_columns([
        pl.when(pl.col("home_team") < pl.col("away_team"))
          .then(pl.col("home_team") + pl.lit("|") + pl.col("away_team"))
          .otherwise(pl.col("away_team") + pl.lit("|") + pl.col("home_team"))
          .alias("pair_key"),
    ])

    # Etiquetas binarias (multiples líneas)
    df = df.with_columns([
        (pl.col("ft_total") > 5.5).cast(pl.Int8).alias("over_55"),
        (pl.col("ft_total") > 6.5).cast(pl.Int8).alias("over_65"),
        (pl.col("ft_total") > 7.5).cast(pl.Int8).alias("over_75"),
        (pl.col("ft_total") > 8.5).cast(pl.Int8).alias("over_85"),
    ])

    # Ratio HT/FT (para h2h_ht_ratio)
    df = df.with_columns([
        pl.when((pl.col("ht_total").is_not_null()) & (pl.col("ft_total") > 0))
          .then(pl.col("ht_total").cast(pl.Float32) / pl.col("ft_total").cast(pl.Float32))
          .otherwise(None)
          .alias("_ht_ratio"),
    ])

    # Hora UTC
    df = df.with_columns([
        pl.col("dt").str.slice(11, 2).cast(pl.Int32).alias("hour_utc"),
    ])

    # ── PASO 1: Acumuladores H2H (ordenados por par+tiempo) ─────────────────
    df = df.sort(["pair_key", "dt"]).with_columns([
        # Número de partidos previos del par (0 = primera vez que se enfrentan)
        (pl.lit(1).cast(pl.Int32).cum_sum().over("pair_key") - 1)
            .alias("h2h_count"),

        # Suma acumulada de goles totales del par (solo partidos anteriores)
        pl.col("ft_total").cast(pl.Float64).shift(1).cum_sum()
            .over("pair_key").alias("_h2h_sum"),

        # Número de partidos previos con datos (para dividir)
        pl.col("ft_total").is_not_null().cast(pl.Int32).shift(1).cum_sum()
            .over("pair_key").alias("_h2h_n"),

        # Suma acumulada Over6.5 (para tasa acumulada)
        pl.col("over_65").cast(pl.Float64).shift(1).cum_sum()
            .over("pair_key").alias("_h2h_over65_sum"),

        # Ventanas rodantes
        pl.col("over_65").cast(pl.Float32).shift(1)
            .rolling_mean(window_size=3, min_samples=1).over("pair_key")
            .alias("h2h_streak_3"),
        pl.col("over_65").cast(pl.Float32).shift(1)
            .rolling_mean(window_size=5, min_samples=1).over("pair_key")
            .alias("h2h_streak_5"),
        pl.col("over_65").cast(pl.Float32).shift(1)
            .rolling_mean(window_size=10, min_samples=1).over("pair_key")
            .alias("h2h_streak_10"),

        # Std rodante (últimas 10)
        pl.col("ft_total").cast(pl.Float64).shift(1)
            .rolling_std(window_size=10, min_samples=2).over("pair_key")
            .alias("h2h_std"),

        # Ratio HT/FT rodante (últimas 10, solo donde hay HT)
        pl.col("_ht_ratio").shift(1)
            .rolling_mean(window_size=10, min_samples=1).over("pair_key")
            .alias("h2h_ht_ratio"),
    ])

    # ── PASO 2: Derivar medias de los acumuladores ────────────────────────────
    df = df.with_columns([
        # Media acumulada de goles H2H
        pl.when(pl.col("_h2h_n") > 0)
          .then(pl.col("_h2h_sum") / pl.col("_h2h_n").cast(pl.Float64))
          .otherwise(None).alias("h2h_avg"),

        # Tasa Over6.5 acumulada
        pl.when(pl.col("_h2h_n") > 0)
          .then(pl.col("_h2h_over65_sum") / pl.col("_h2h_n").cast(pl.Float64))
          .otherwise(None).alias("h2h_o65"),

    ])

    # ── PLAYER FEATURES (rol home y rol away por separado) ───────────────────

    # Home player: media rodante de los últimos 20 partidos como local
    home_feats = (
        df.sort(["home_player", "dt"])
        .with_columns([
            pl.col("ft_total").cast(pl.Float32).shift(1)
              .rolling_mean(window_size=20, min_samples=3).over("home_player")
              .alias("player_home_avg"),
            pl.col("over_65").cast(pl.Float32).shift(1)
              .rolling_mean(window_size=20, min_samples=3).over("home_player")
              .alias("player_home_o65"),
        ])
        .select(["match_id", "player_home_avg", "player_home_o65"])
    )

    # Away player: media rodante de los últimos 20 partidos como visitante
    away_feats = (
        df.sort(["away_player", "dt"])
        .with_columns([
            pl.col("ft_total").cast(pl.Float32).shift(1)
              .rolling_mean(window_size=20, min_samples=3).over("away_player")
              .alias("player_away_avg"),
            pl.col("over_65").cast(pl.Float32).shift(1)
              .rolling_mean(window_size=20, min_samples=3).over("away_player")
              .alias("player_away_o65"),
        ])
        .select(["match_id", "player_away_avg", "player_away_o65"])
    )

    # Join por match_id (único por fila)
    df = df.join(home_feats, on="match_id", how="left")
    df = df.join(away_feats, on="match_id", how="left")

    # Ordenar por dt para salida final
    df = df.sort("dt")

    # Seleccionar y ordenar columnas de salida
    return df.select([
        "match_id", "league_id", "dt", "home", "away",
        "ft_h", "ft_a", "ft_total",
        "ht_h", "ht_a", "ht_total",
        "over_55", "over_65", "over_75", "over_85",
        "pair_key",
        "h2h_count", "h2h_avg", "h2h_std", "h2h_o65",
        "h2h_streak_3", "h2h_streak_5", "h2h_streak_10",
        "h2h_ht_ratio",
        "player_home_avg", "player_home_o65",
        "player_away_avg", "player_away_o65",
        "hour_utc",
    ])


# ── ESTADÍSTICAS DE COBERTURA ─────────────────────────────────────────────────

def coverage_stats(df: pl.DataFrame) -> None:
    n = len(df)
    print(f"\n  Filas totales:    {n:,}")
    cols_check = [
        ("h2h_avg",        "H2H avg (>0 previos)"),
        ("h2h_std",        "H2H std  (>1 previo) "),
        ("h2h_ht_ratio",   "H2H HT ratio         "),
        ("player_home_avg","Player home avg       "),
        ("player_away_avg","Player away avg       "),
    ]
    for col, label in cols_check:
        filled = df.filter(pl.col(col).is_not_null()).height
        print(f"  {label}: {filled:>7,} ({100*filled/n:.1f}%)")

    print(f"\n  Over rates:")
    for col in ["over_55", "over_65", "over_75", "over_85"]:
        rate = df[col].mean()
        print(f"    {col}: {100*rate:.1f}%")

    print(f"\n  H2H count distribución:")
    h_counts = df["h2h_count"]
    for threshold in [0, 5, 10, 20, 50]:
        frac = (h_counts >= threshold).sum() / n
        print(f"    >= {threshold:>3}:  {frac:.1%}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Polars feature builder para bsports.db")
    parser.add_argument("--db",     default=str(DB_PATH),  help="Ruta a bsports.db")
    parser.add_argument("--out",    default=str(OUT_CSV),  help="CSV de salida")
    parser.add_argument("--sample", type=int, default=0,   help="Imprimir N filas de muestra")
    args = parser.parse_args()

    db_path  = Path(args.db)
    out_path = Path(args.out)

    if not db_path.exists():
        print(f"ERROR: {db_path} no encontrado. Ejecuta bsports_scraper.py --phase 1 primero.")
        sys.exit(1)

    print(f"Cargando {db_path}…")
    df_raw = load_db(db_path)
    n_ht   = df_raw.filter(pl.col("ht_total").is_not_null()).height
    print(f"  {len(df_raw):,} partidos ({n_ht:,} con HT, {len(df_raw)-n_ht:,} sin HT)")

    print("Computando features (Polars)…")
    df_out = build_features(df_raw)

    coverage_stats(df_out)

    if args.sample > 0:
        print(f"\n--- Muestra ({args.sample} filas) ---")
        with pl.Config(tbl_cols=12, tbl_rows=args.sample, fmt_str_lengths=25):
            print(df_out.select([
                "dt", "home", "away", "ft_total",
                "h2h_count", "h2h_avg", "h2h_o65",
                "player_home_avg", "player_away_avg",
            ]).head(args.sample))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.write_csv(str(out_path))
    size_mb = out_path.stat().st_size / 1e6
    print(f"\nGuardado: {out_path}")
    print(f"  {len(df_out):,} filas × {len(df_out.columns)} columnas — {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
