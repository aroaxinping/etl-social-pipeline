"""Configuracion del pipeline ETL."""

from pathlib import Path

# ── Rutas ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"
TEMPLATES_DIR = BASE_DIR / "templates"

# CSVs de entrada — cambia estas rutas a tus archivos reales
TIKTOK_CSV = DATA_RAW / "tiktok_analytics.csv"
INSTAGRAM_CSV = DATA_RAW / "instagram_analytics.csv"

# ── Umbrales ───────────────────────────────────────────
ANOMALY_STD_THRESHOLD = 3  # posts con views > 3 std sobre la media
TOP_N_POSTS = 5

# ── Columnas esperadas (post-normalizacion) ────────────
UNIFIED_COLUMNS = [
    "date",
    "platform",
    "post_id",
    "description",
    "views",
    "likes",
    "comments",
    "shares",
    "engagement_rate",
]
