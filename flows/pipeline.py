"""
ETL Pipeline para Social Media Analytics — @aroaxinping
Orquestado con Prefect.
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape
from prefect import flow, get_run_logger, task

# Agregar el directorio raiz al path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))
import config


# ── Datos sinteticos (fallback) ────────────────────────────────────


def _generate_synthetic_tiktok(n: int = 50) -> pd.DataFrame:
    """Genera datos sinteticos de TikTok si no existen CSVs reales."""
    rng = np.random.default_rng(42)
    dates = pd.date_range(end=datetime.now(), periods=n, freq="D")
    topics = [
        "python tips", "data science", "sql hack", "pandas trick",
        "ml basics", "git workflow", "terminal setup", "vscode config",
        "jupyter hack", "api rest", "web scraping", "docker intro",
    ]
    return pd.DataFrame({
        "Date": dates,
        "Video ID": [f"tt_{i:04d}" for i in range(n)],
        "Description": rng.choice(topics, n),
        "Video Views": rng.integers(500, 150_000, n),
        "Likes": rng.integers(10, 8_000, n),
        "Comments": rng.integers(0, 500, n),
        "Shares": rng.integers(0, 2_000, n),
    })


def _generate_synthetic_instagram(n: int = 40) -> pd.DataFrame:
    """Genera datos sinteticos de Instagram si no existen CSVs reales."""
    rng = np.random.default_rng(99)
    dates = pd.date_range(end=datetime.now(), periods=n, freq="2D")
    topics = [
        "reel python", "carousel data", "reel sql", "story tech",
        "reel pandas", "carousel ml", "reel git", "story code",
        "reel api", "carousel analytics",
    ]
    return pd.DataFrame({
        "Date": dates,
        "Post ID": [f"ig_{i:04d}" for i in range(n)],
        "Caption": rng.choice(topics, n),
        "Impressions": rng.integers(300, 80_000, n),
        "Likes": rng.integers(5, 5_000, n),
        "Comments": rng.integers(0, 300, n),
        "Shares": rng.integers(0, 1_000, n),
    })


# ── Tasks ──────────────────────────────────────────────────────────


@task(retries=2, retry_delay_seconds=10)
def extract_tiktok() -> pd.DataFrame:
    """Extrae datos de TikTok desde CSV o genera sinteticos."""
    logger = get_run_logger()

    if config.TIKTOK_CSV.exists():
        logger.info(f"Leyendo TikTok CSV: {config.TIKTOK_CSV}")
        df = pd.read_csv(config.TIKTOK_CSV)
        logger.info(f"TikTok: {len(df)} filas cargadas")

        expected_cols = {"Date", "Video ID", "Description", "Video Views", "Likes", "Comments", "Shares"}
        missing = expected_cols - set(df.columns)
        if missing:
            logger.warning(f"Columnas faltantes en TikTok CSV: {missing}. Usando datos sinteticos.")
            df = _generate_synthetic_tiktok()
    else:
        logger.warning(f"CSV no encontrado: {config.TIKTOK_CSV} — usando datos sinteticos")
        df = _generate_synthetic_tiktok()
        logger.info(f"TikTok sintetico: {len(df)} filas generadas")

    return df


@task(retries=2, retry_delay_seconds=10)
def extract_instagram() -> pd.DataFrame:
    """Extrae datos de Instagram desde CSV o genera sinteticos."""
    logger = get_run_logger()

    if config.INSTAGRAM_CSV.exists():
        logger.info(f"Leyendo Instagram CSV: {config.INSTAGRAM_CSV}")
        df = pd.read_csv(config.INSTAGRAM_CSV)
        logger.info(f"Instagram: {len(df)} filas cargadas")

        expected_cols = {"Date", "Post ID", "Caption", "Impressions", "Likes", "Comments", "Shares"}
        missing = expected_cols - set(df.columns)
        if missing:
            logger.warning(f"Columnas faltantes en Instagram CSV: {missing}. Usando datos sinteticos.")
            df = _generate_synthetic_instagram()
    else:
        logger.warning(f"CSV no encontrado: {config.INSTAGRAM_CSV} — usando datos sinteticos")
        df = _generate_synthetic_instagram()
        logger.info(f"Instagram sintetico: {len(df)} filas generadas")

    return df


@task(retries=2, retry_delay_seconds=10)
def transform_unify(tiktok_df: pd.DataFrame, instagram_df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas y unifica ambas plataformas en un solo DataFrame."""
    logger = get_run_logger()

    # Normalizar TikTok
    tt = tiktok_df.rename(columns={
        "Date": "date",
        "Video ID": "post_id",
        "Description": "description",
        "Video Views": "views",
        "Likes": "likes",
        "Comments": "comments",
        "Shares": "shares",
    })
    tt["platform"] = "TikTok"

    # Normalizar Instagram
    ig = instagram_df.rename(columns={
        "Date": "date",
        "Post ID": "post_id",
        "Caption": "description",
        "Impressions": "views",
        "Likes": "likes",
        "Comments": "comments",
        "Shares": "shares",
    })
    ig["platform"] = "Instagram"

    # Unificar
    unified = pd.concat([tt, ig], ignore_index=True)
    unified["date"] = pd.to_datetime(unified["date"])
    unified = unified.sort_values("date", ascending=False).reset_index(drop=True)

    # Calcular engagement rate por post
    unified["engagement_rate"] = (
        (unified["likes"] + unified["comments"] + unified["shares"])
        / unified["views"].replace(0, np.nan)
        * 100
    ).fillna(0).round(2)

    # Mantener solo columnas esperadas
    unified = unified[config.UNIFIED_COLUMNS]

    logger.info(f"Unificado: {len(unified)} posts ({len(tt)} TikTok + {len(ig)} Instagram)")
    return unified


@task(retries=2, retry_delay_seconds=10)
def calculate_metrics(unified_df: pd.DataFrame) -> dict:
    """Calcula KPIs: engagement rate, viral rate, top topics, comparacion por plataforma."""
    logger = get_run_logger()

    total_posts = len(unified_df)
    total_views = unified_df["views"].sum()
    avg_engagement = unified_df["engagement_rate"].mean()

    # Viral rate: % de posts con views > 3 std sobre la media
    views_mean = unified_df["views"].mean()
    views_std = unified_df["views"].std()
    viral_threshold = views_mean + config.ANOMALY_STD_THRESHOLD * views_std
    viral_posts = (unified_df["views"] > viral_threshold).sum()
    viral_rate = (viral_posts / total_posts * 100) if total_posts > 0 else 0

    # Comparacion por plataforma
    platform_stats = unified_df.groupby("platform").agg(
        posts=("post_id", "count"),
        avg_views=("views", "mean"),
        avg_engagement=("engagement_rate", "mean"),
    ).reset_index()

    best_platform = platform_stats.loc[platform_stats["avg_engagement"].idxmax(), "platform"]

    platform_comparison = platform_stats.to_dict("records")

    metrics = {
        "total_posts": total_posts,
        "total_views": float(total_views),
        "avg_engagement_rate": float(avg_engagement),
        "viral_rate": float(viral_rate),
        "best_platform": best_platform,
        "platform_comparison": platform_comparison,
    }

    logger.info(
        f"Metricas: {total_posts} posts, {total_views:,.0f} views, "
        f"{avg_engagement:.2f}% engagement, {viral_rate:.2f}% viral rate"
    )
    return metrics


@task(retries=2, retry_delay_seconds=10)
def detect_anomalies(unified_df: pd.DataFrame) -> list[dict]:
    """Detecta posts con views > 3 std por encima de la media."""
    logger = get_run_logger()

    views_mean = unified_df["views"].mean()
    views_std = unified_df["views"].std()
    threshold = views_mean + config.ANOMALY_STD_THRESHOLD * views_std

    anomalies_df = unified_df[unified_df["views"] > threshold].copy()
    anomalies_df = anomalies_df.sort_values("views", ascending=False)

    anomalies = anomalies_df.to_dict("records")

    # Convertir timestamps a strings para el template
    for a in anomalies:
        if hasattr(a["date"], "strftime"):
            a["date"] = a["date"].strftime("%Y-%m-%d")

    logger.info(
        f"Anomalias: {len(anomalies)} posts con views > {threshold:,.0f} "
        f"(media={views_mean:,.0f}, std={views_std:,.0f})"
    )
    return anomalies


@task(retries=2, retry_delay_seconds=10)
def generate_report(metrics: dict, anomalies: list[dict], unified_df: pd.DataFrame) -> str:
    """Genera reporte HTML usando Jinja2."""
    logger = get_run_logger()

    env = Environment(
        loader=FileSystemLoader(str(config.TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("report.html")

    # Top N posts por views
    top_posts = unified_df.nlargest(config.TOP_N_POSTS, "views").to_dict("records")
    for post in top_posts:
        if hasattr(post["date"], "strftime"):
            post["date"] = post["date"].strftime("%Y-%m-%d")

    # Convertir metrics dict a objeto con atributos para el template
    class MetricsObj:
        pass

    m = MetricsObj()
    for k, v in metrics.items():
        setattr(m, k, v)

    html = template.render(
        report_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
        metrics=m,
        top_posts=top_posts,
        anomalies=anomalies,
    )

    logger.info("Reporte HTML generado")
    return html


@task(retries=2, retry_delay_seconds=10)
def save_outputs(unified_df: pd.DataFrame, report_html: str) -> None:
    """Guarda CSV procesado y reporte HTML."""
    logger = get_run_logger()

    # Crear directorios si no existen
    config.DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # Guardar CSV procesado
    csv_path = config.DATA_PROCESSED / f"unified_analytics_{timestamp}.csv"
    unified_df.to_csv(csv_path, index=False)
    logger.info(f"CSV guardado: {csv_path}")

    # Guardar reporte HTML
    report_path = config.REPORTS_DIR / f"report_{timestamp}.html"
    report_path.write_text(report_html, encoding="utf-8")
    logger.info(f"Reporte guardado: {report_path}")


# ── Flow principal ─────────────────────────────────────────────────


@flow(name="social-media-pipeline", log_prints=True)
def social_media_pipeline():
    """Pipeline ETL completo para analytics de redes sociales."""
    logger = get_run_logger()
    logger.info("Iniciando pipeline ETL — @aroaxinping")

    # 1. Extract (en paralelo)
    tiktok_future = extract_tiktok.submit()
    instagram_future = extract_instagram.submit()
    tiktok_df = tiktok_future.result()
    instagram_df = instagram_future.result()

    # 2. Transform
    unified_df = transform_unify(tiktok_df, instagram_df)

    # Guard: si no hay datos, salir
    if unified_df.empty:
        logger.warning("No hay datos despues de unificacion. Saltando metricas.")
        return

    # 3. Metrics + Anomalies (en paralelo)
    metrics_future = calculate_metrics.submit(unified_df)
    anomalies_future = detect_anomalies.submit(unified_df)
    metrics = metrics_future.result()
    anomalies = anomalies_future.result()

    # 4. Report
    report_html = generate_report(metrics, anomalies, unified_df)

    # 5. Save
    save_outputs(unified_df, report_html)

    logger.info("Pipeline completado")


if __name__ == "__main__":
    social_media_pipeline()
