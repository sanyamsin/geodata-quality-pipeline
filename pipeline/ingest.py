"""
pipeline/ingest.py
==================
Data ingestion from humanitarian APIs:
  - OCHA HDX (admin boundaries, CODs)
  - OpenStreetMap Overpass API (health facilities, infrastructure)
  - ReliefWeb API (disaster events)

Usage:
    from pipeline.ingest import HDXIngester, OverpassIngester, ReliefWebIngester
"""

import logging
import time
from datetime import datetime
from typing import Optional
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, shape
import yaml
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


# ── BASE INGESTER ─────────────────────────────────────────────────────────────

class BaseIngester:
    """Base class for all data ingesters."""

    def __init__(self, source_config: dict):
        self.config = source_config
        self.source_id = source_config["id"]
        self.source_name = source_config["name"]
        self.expected_crs = source_config.get("expected_crs", "EPSG:4326")
        self.domain = source_config.get("domain", "DD01")

    def fetch(self) -> Optional[gpd.GeoDataFrame]:
        raise NotImplementedError

    def _add_metadata(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Adds ingestion metadata to every GeoDataFrame."""
        gdf["source_id"] = self.source_id
        gdf["source_name"] = self.source_name
        gdf["domain"] = self.domain
        gdf["date_ingested"] = datetime.utcnow().isoformat()
        gdf["pipeline_version"] = "1.0"
        return gdf

    def _ensure_crs(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Ensures CRS matches expected CRS, reprojects if needed."""
        if gdf.crs is None:
            gdf = gdf.set_crs(self.expected_crs)
            logger.warning(f"{self.source_id}: No CRS found, set to {self.expected_crs}")
        elif gdf.crs.to_string() != self.expected_crs:
            logger.info(f"{self.source_id}: Reprojecting from {gdf.crs} to {self.expected_crs}")
            gdf = gdf.to_crs(self.expected_crs)
        return gdf


# ── HDX INGESTER ──────────────────────────────────────────────────────────────

class HDXIngester(BaseIngester):
    """
    Ingests geospatial data from OCHA Humanitarian Data Exchange (HDX).
    Uses the CKAN-compatible HDX API to discover and download datasets.

    HDX API docs: https://data.humdata.org/api/3/
    """

    HDX_API_BASE = "https://data.humdata.org/api/3/action"

    def fetch(self) -> Optional[gpd.GeoDataFrame]:
        logger.info(f"Fetching HDX dataset: {self.config['dataset_id']}")
        try:
            # Step 1: Get dataset metadata
            url = f"{self.HDX_API_BASE}/package_show"
            resp = requests.get(url, params={"id": self.config["dataset_id"]}, timeout=30)
            resp.raise_for_status()
            resources = resp.json()["result"]["resources"]

            # Step 2: Find the right resource
            resource_name = self.config.get("resource_name", "")
            resource_url = None
            for r in resources:
                if resource_name.lower() in r.get("name", "").lower():
                    resource_url = r["download_url"]
                    break

            if not resource_url:
                # Fallback: take first GeoJSON or Shapefile resource
                for r in resources:
                    fmt = r.get("format", "").lower()
                    if fmt in ("geojson", "shp", "shapefile", "zip"):
                        resource_url = r["download_url"]
                        break

            if not resource_url:
                logger.error(f"No suitable resource found for {self.config['dataset_id']}")
                return self._generate_sample_data("Polygon")

            # Step 3: Download and parse
            logger.info(f"Downloading from: {resource_url}")
            gdf = gpd.read_file(resource_url)
            gdf = self._ensure_crs(gdf)
            gdf = self._add_metadata(gdf)
            logger.info(f"HDX: Loaded {len(gdf)} features from {self.config['dataset_id']}")
            return gdf

        except Exception as e:
            logger.warning(f"HDX fetch failed for {self.source_id}: {e}. Using sample data.")
            return self._generate_sample_data("Polygon")

    def _generate_sample_data(self, geom_type: str) -> gpd.GeoDataFrame:
        """Generates realistic sample data when API is unavailable."""
        from shapely.geometry import box
        import numpy as np

        logger.info(f"Generating sample data for {self.source_id}")
        np.random.seed(42)

        # Sample admin boundaries (approximate RCA regions)
        regions = [
            {"name": "Bangui", "admin_level": 1, "geometry": box(18.4, 4.2, 18.7, 4.5)},
            {"name": "Ombella-M'Poko", "admin_level": 1, "geometry": box(17.5, 4.0, 19.5, 6.0)},
            {"name": "Lobaye", "admin_level": 1, "geometry": box(16.0, 3.5, 18.0, 5.5)},
            {"name": "Sangha-Mbaéré", "admin_level": 1, "geometry": box(15.0, 2.5, 17.0, 4.5)},
            {"name": "Mambéré-Kadéï", "admin_level": 1, "geometry": box(14.5, 3.5, 16.5, 6.0)},
        ]
        gdf = gpd.GeoDataFrame(regions, crs=self.expected_crs)
        gdf = self._add_metadata(gdf)
        gdf["data_source"] = "sample_data"
        return gdf


# ── OVERPASS (OSM) INGESTER ───────────────────────────────────────────────────

class OverpassIngester(BaseIngester):
    """
    Ingests geospatial data from OpenStreetMap via the Overpass API.
    Used for health facilities, water points, infrastructure.

    Overpass API: https://overpass-api.de/
    """

    OVERPASS_URL = "https://overpass-api.de/api/interpreter"

    def fetch(self) -> Optional[gpd.GeoDataFrame]:
        logger.info(f"Fetching OSM data: {self.source_id}")
        try:
            query = self._build_query()
            resp = requests.post(
                self.OVERPASS_URL,
                data={"data": query},
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()

            features = []
            for element in data.get("elements", []):
                if element["type"] == "node":
                    lat = element.get("lat")
                    lon = element.get("lon")
                    if lat and lon:
                        tags = element.get("tags", {})
                        features.append({
                            "osm_id": element["id"],
                            "name": tags.get("name", "Unknown"),
                            "amenity": tags.get("amenity", ""),
                            "healthcare": tags.get("healthcare", ""),
                            "operator": tags.get("operator", ""),
                            "geometry": Point(lon, lat)
                        })

            if not features:
                logger.warning(f"No features returned from Overpass for {self.source_id}")
                return self._generate_sample_data("Point")

            gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
            gdf = self._ensure_crs(gdf)
            gdf = self._add_metadata(gdf)
            logger.info(f"OSM: Loaded {len(gdf)} features")
            return gdf

        except Exception as e:
            logger.warning(f"Overpass fetch failed: {e}. Using sample data.")
            return self._generate_sample_data("Point")

    def _build_query(self) -> str:
        """Builds Overpass QL query from config."""
        raw = self.config.get("query", "node[amenity=hospital]")
        return f"[out:json][timeout:30];{raw};out body;"

    def _generate_sample_data(self, geom_type: str) -> gpd.GeoDataFrame:
        """Generates realistic sample health facility data."""
        import numpy as np
        np.random.seed(123)

        facilities = [
            {"name": "Hôpital Communautaire de Bangui", "amenity": "hospital", "operator": "MSF", "lat": 4.361, "lon": 18.555},
            {"name": "Centre de Santé de Bossangoa", "amenity": "clinic", "operator": "MOH", "lat": 6.489, "lon": 17.453},
            {"name": "Hôpital de Kaga-Bandoro", "amenity": "hospital", "operator": "MOH", "lat": 6.993, "lon": 19.183},
            {"name": "Centre Nutritionnel de Bambari", "amenity": "clinic", "operator": "ACF", "lat": 5.765, "lon": 20.672},
            {"name": "Poste de Santé de Ndélé", "amenity": "health_post", "operator": "MOH", "lat": 8.411, "lon": 20.651},
            {"name": "Hôpital de Bouar", "amenity": "hospital", "operator": "MOH", "lat": 5.925, "lon": 15.599},
            {"name": "Centre de Santé de Carnot", "amenity": "clinic", "operator": "CRF", "lat": 4.940, "lon": 15.878},
        ]
        rows = []
        for f in facilities:
            rows.append({
                "name": f["name"],
                "amenity": f["amenity"],
                "operator": f["operator"],
                "osm_id": np.random.randint(100000, 999999),
                "geometry": Point(f["lon"], f["lat"])
            })
        gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        gdf = self._add_metadata(gdf)
        gdf["data_source"] = "sample_data"
        return gdf


# ── RELIEFWEB INGESTER ────────────────────────────────────────────────────────

class ReliefWebIngester(BaseIngester):
    """
    Ingests disaster and crisis event data from the ReliefWeb API.
    Converts events to point geometries using country centroids.

    ReliefWeb API: https://apidoc.rwlabs.org/
    """

    COUNTRY_CENTROIDS = {
        "Central African Republic": (20.940, 6.611),
        "Mauritania": (-10.940, 20.265),
        "Senegal": (-14.452, 14.497),
        "Mali": (-1.981, 17.570),
        "Niger": (8.082, 17.607),
        "Burkina Faso": (-1.561, 12.364),
        "Chad": (18.732, 15.454),
    }

    def fetch(self) -> Optional[gpd.GeoDataFrame]:
        logger.info(f"Fetching ReliefWeb data: {self.source_id}")
        try:
            endpoint = self.config.get("endpoint", "https://api.reliefweb.int/v1/disasters")
            filters = self.config.get("filters", {})

            payload = {
                "appname": "humanitarian-gis-pipeline",
                "limit": 50,
                "fields": {"include": ["name", "date", "status", "type", "country", "glide"]},
                "filter": {
                    "operator": "AND",
                    "conditions": []
                }
            }

            countries = filters.get("country", [])
            if countries:
                payload["filter"]["conditions"].append({
                    "field": "country.name",
                    "value": countries,
                    "operator": "OR"
                })

            resp = requests.post(endpoint, json=payload, timeout=30)
            resp.raise_for_status()
            items = resp.json().get("data", [])

            rows = []
            for item in items:
                fields = item.get("fields", {})
                country_list = fields.get("country", [{}])
                country_name = country_list[0].get("name", "") if country_list else ""
                coords = self.COUNTRY_CENTROIDS.get(country_name, (0, 0))
                rows.append({
                    "event_id": item.get("id"),
                    "name": fields.get("name", "Unknown"),
                    "status": fields.get("status", ""),
                    "disaster_type": fields.get("type", [{}])[0].get("name", "") if fields.get("type") else "",
                    "country": country_name,
                    "date_event": fields.get("date", {}).get("created", ""),
                    "glide": fields.get("glide", ""),
                    "geometry": Point(coords[0], coords[1])
                })

            if not rows:
                return self._generate_sample_data("Point")

            gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
            gdf = self._add_metadata(gdf)
            logger.info(f"ReliefWeb: Loaded {len(gdf)} disaster events")
            return gdf

        except Exception as e:
            logger.warning(f"ReliefWeb fetch failed: {e}. Using sample data.")
            return self._generate_sample_data("Point")

    def _generate_sample_data(self, geom_type: str) -> gpd.GeoDataFrame:
        events = [
            {"name": "Floods — Bangui 2025", "status": "ongoing", "disaster_type": "Flood", "country": "Central African Republic"},
            {"name": "Drought — Southern Mauritania", "status": "alert", "disaster_type": "Drought", "country": "Mauritania"},
            {"name": "Conflict displacement — Bambari", "status": "ongoing", "disaster_type": "Complex Emergency", "country": "Central African Republic"},
            {"name": "Cholera outbreak — Saint-Louis", "status": "alert", "disaster_type": "Epidemic", "country": "Senegal"},
        ]
        rows = []
        for e in events:
            coords = self.COUNTRY_CENTROIDS.get(e["country"], (0, 0))
            rows.append({**e, "geometry": Point(coords[0], coords[1]), "event_id": None, "date_event": datetime.utcnow().isoformat(), "glide": ""})
        gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        gdf = self._add_metadata(gdf)
        gdf["data_source"] = "sample_data"
        return gdf


# ── INGESTER FACTORY ──────────────────────────────────────────────────────────

def get_ingester(source_config: dict) -> BaseIngester:
    """Returns the appropriate ingester based on source type."""
    ingester_map = {
        "hdx": HDXIngester,
        "overpass": OverpassIngester,
        "reliefweb": ReliefWebIngester,
    }
    source_type = source_config.get("type", "hdx")
    cls = ingester_map.get(source_type)
    if not cls:
        raise ValueError(f"Unknown source type: {source_type}")
    return cls(source_config)


def run_ingestion(config_path: str = "config/pipeline.yaml") -> dict:
    """Runs ingestion for all configured sources. Returns dict of GeoDataFrames."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    results = {}
    for source in config.get("sources", []):
        logger.info(f"--- Ingesting: {source['id']} ---")
        ingester = get_ingester(source)
        gdf = ingester.fetch()
        if gdf is not None and len(gdf) > 0:
            results[source["id"]] = gdf
            logger.info(f"✓ {source['id']}: {len(gdf)} features ingested")
        else:
            logger.error(f"✗ {source['id']}: ingestion returned empty dataset")

    return results


if __name__ == "__main__":
    results = run_ingestion()
    for source_id, gdf in results.items():
        print(f"\n{source_id}: {len(gdf)} features, CRS={gdf.crs}, columns={list(gdf.columns)}")
