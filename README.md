# geodata-quality-pipeline

**Automated geospatial data quality monitoring for humanitarian operations**  
PostGIS · Python · Streamlit · DAMA-DMBOK v2 · ISO 19115

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org)
[![PostGIS](https://img.shields.io/badge/PostGIS-15--3.3-336791.svg)](https://postgis.net)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B.svg)](https://streamlit.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Live Demo](https://img.shields.io/badge/Live%20Demo-Hugging%20Face-yellow)](https://huggingface.co/spaces/Lokozu/geodata-quality-pipeline)

---

## Overview

Humanitarian GIS operations depend on high-quality geospatial data — but data degrades silently. This pipeline continuously ingests data from humanitarian APIs (OCHA HDX, OpenStreetMap, ReliefWeb), runs automated quality checks across 6 DAMA-DMBOK quality dimensions, and surfaces failures in a real-time Streamlit dashboard.

**Who it's for:** GIS Data Governance teams, MEL data officers, field data coordinators.

---

## Architecture

```
                ┌─────────────────────────────────┐
                │        DATA SOURCES              │
                │  OCHA HDX · OSM · ReliefWeb      │
                └────────────┬────────────────────┘
                             │ pipeline/ingest.py
                             ▼
                ┌─────────────────────────────────┐
                │      PostGIS (Docker)            │
                │  admin_boundaries                │
                │  health_facilities               │
                │  disaster_events                 │
                └────────────┬────────────────────┘
                             │ pipeline/validators.py
                             ▼
                ┌─────────────────────────────────┐
                │    Quality Reports (JSON)        │
                │  6 dimensions · 15+ checks       │
                └────────────┬────────────────────┘
                             │
                             ▼
                ┌─────────────────────────────────┐
                │   Streamlit Dashboard            │
                │  Scores · Radar · Heatmap        │
                └─────────────────────────────────┘
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/sanyamsin/geodata-quality-pipeline.git
cd geodata-quality-pipeline
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
```

### 2. Start PostGIS

```bash
docker-compose up -d
```

PostGIS will be available at `localhost:5432`.  
pgAdmin UI at `http://localhost:5050` (admin@geodata.local / admin).

### 3. Run the quality pipeline

```bash
python pipeline/ingest.py
```

### 4. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Opens at `http://localhost:8501`.

---

## Quality Dimensions (DAMA-DMBOK v2 Ch. 13)

| Dimension | Checks | DAMA Reference |
|-----------|--------|----------------|
| Completeness | Null geometries, mandatory fields | DMBOK2 §13.2.1 |
| Validity | Invalid geometry, CRS compliance, coordinate bounds | DMBOK2 §13.2.2 |
| Consistency | Self-intersections, topology, zero-area polygons | DMBOK2 §13.2.3 |
| Uniqueness | Duplicate geometries and attributes | DMBOK2 §13.2.4 |
| Timeliness | Data freshness vs update frequency | DMBOK2 §13.2.5 |
| Accuracy | Coordinate precision, operational area bounds | DMBOK2 §13.2.6 |

---

## Data Sources

| Source | API | Domain | Update Freq |
|--------|-----|--------|-------------|
| OCHA HDX — Admin Boundaries | HDX CKAN API | DD01 | Quarterly |
| OpenStreetMap — Health Facilities | Overpass API | DD03 | Monthly |
| ReliefWeb — Disaster Events | ReliefWeb API v1 | DD05 | Daily |

---

## Related Projects

| Project | Description |
|---------|-------------|
| [humanitarian-gis-governance](https://github.com/sanyamsin/humanitarian-gis-governance) | DAMA-DMBOK v2 governance framework |
| [humanitarian-geo-catalogue](https://github.com/sanyamsin/humanitarian-geo-catalogue) | ISO 19115 data catalogue |

---

## Author

**Serge-Alain NYAMSIN** — GIS Data Governance & Humanitarian Data Engineering  
[github.com/sanyamsin](https://github.com/sanyamsin)

---

## License

MIT License
