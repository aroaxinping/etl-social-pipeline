# ETL Social Pipeline

Pipeline ETL automatizado para analytics de redes sociales (@aroaxinping). Extrae datos de TikTok e Instagram, unifica, calcula KPIs, detecta anomalias y genera reportes HTML.

## Por que este proyecto

Tener datos de analytics en CSVs separados no sirve de mucho si cada vez que quieres ver como va tu contenido tienes que abrir archivos manualmente. Un pipeline ETL automatiza todo: extraccion, transformacion, calculo de metricas y generacion de reportes. Ademas, detecta automaticamente posts con metricas anomalas.

Este proyecto usa Prefect como orquestador — puro Python, sin YAML, sin infraestructura extra.

## Que se aprende

| Concepto | Implementacion |
|---|---|
| ETL (Extract, Transform, Load) | 7 tasks separadas con responsabilidades claras |
| Orquestacion con Prefect | `@flow` y `@task` con retries, logging y ejecucion paralela |
| Ejecucion paralela | `.submit()` / `.result()` para correr extracts y metricas en paralelo |
| Unificacion de datos | Normalizar columnas de TikTok e Instagram en un schema comun |
| Deteccion de anomalias | Posts con views > 3 desviaciones estandar sobre la media |
| Generacion de reportes | Jinja2 template con dark theme, KPI cards y tablas |
| Validacion de datos | Comprobacion de columnas esperadas en CSVs + fallback a sintetico |

## Pipeline

```
Extract TikTok ──┐
                  ├──> Unify ──> Calculate Metrics ──┐
Extract Instagram ┘                                   ├──> Generate Report ──> Save
                                  Detect Anomalies ──┘
```

Los extracts corren en paralelo. Metrics y anomalias tambien.

## Uso

```bash
pip install -r requirements.txt

# Ejecutar el pipeline (genera datos sinteticos si no hay CSVs reales)
python flows/pipeline.py
```

Los reportes se guardan en `reports/` como HTML con timestamp.

## Estructura

```
flows/pipeline.py      — Flow principal con 7 tasks
config.py              — Rutas, umbrales, configuracion centralizada
templates/report.html  — Template del reporte (dark theme #0f0f0f)
data/raw/              — CSVs originales (gitignored)
data/processed/        — Datos unificados (gitignored)
reports/               — Reportes HTML generados (gitignored)
```

## Stack

Prefect · pandas · numpy · Jinja2
