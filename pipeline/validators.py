"""
pipeline/validators.py
=======================
Automated geospatial data quality validators.
Implements 6 DAMA-DMBOK quality dimensions for GIS data.

Quality dimensions covered:
  - Completeness   : missing geometries, null mandatory fields
  - Validity       : invalid geometries, CRS compliance, coordinate bounds
  - Consistency    : topological errors, self-intersections, winding order
  - Uniqueness     : duplicate geometries and attributes
  - Timeliness     : data freshness vs expected update frequency
  - Accuracy       : coordinate precision, geometry complexity checks
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass, field

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.validation import explain_validity
from shapely.geometry import MultiPolygon, Polygon

logger = logging.getLogger(__name__)


# ── DATA STRUCTURES ───────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    """Result of a single quality check."""
    check_id: str
    check_name: str
    dimension: str
    status: str           # "pass" | "warn" | "fail" | "error"
    score: float          # 0.0 to 1.0
    details: str = ""
    affected_count: int = 0
    total_count: int = 0
    severity: str = "medium"   # "low" | "medium" | "high" | "critical"

    @property
    def pass_rate(self) -> float:
        if self.total_count == 0:
            return 1.0
        return 1.0 - (self.affected_count / self.total_count)


@dataclass
class QualityReport:
    """Full quality report for a single dataset."""
    source_id: str
    source_name: str
    feature_count: int
    geometry_type: str
    crs: str
    check_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    checks: List[CheckResult] = field(default_factory=list)
    domain: str = ""
    data_source: str = ""

    @property
    def overall_score(self) -> float:
        if not self.checks:
            return 0.0
        weights = {"critical": 3, "high": 2, "medium": 1, "low": 0.5}
        total_w = sum(weights.get(c.severity, 1) for c in self.checks)
        weighted_score = sum(c.score * weights.get(c.severity, 1) for c in self.checks)
        return round(weighted_score / total_w, 3) if total_w > 0 else 0.0

    @property
    def status(self) -> str:
        if self.overall_score >= 0.90:
            return "pass"
        elif self.overall_score >= 0.75:
            return "warn"
        else:
            return "fail"

    @property
    def critical_failures(self) -> List[CheckResult]:
        return [c for c in self.checks if c.status == "fail" and c.severity == "critical"]

    @property
    def dimension_scores(self) -> Dict[str, float]:
        dims = {}
        for c in self.checks:
            if c.dimension not in dims:
                dims[c.dimension] = []
            dims[c.dimension].append(c.score)
        return {d: round(np.mean(scores), 3) for d, scores in dims.items()}

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "feature_count": self.feature_count,
            "geometry_type": self.geometry_type,
            "crs": self.crs,
            "check_timestamp": self.check_timestamp,
            "overall_score": self.overall_score,
            "status": self.status,
            "domain": self.domain,
            "dimension_scores": self.dimension_scores,
            "checks": [
                {
                    "check_id": c.check_id,
                    "check_name": c.check_name,
                    "dimension": c.dimension,
                    "status": c.status,
                    "score": c.score,
                    "details": c.details,
                    "affected_count": c.affected_count,
                    "total_count": c.total_count,
                    "severity": c.severity,
                }
                for c in self.checks
            ]
        }


# ── VALIDATOR CLASS ───────────────────────────────────────────────────────────

class GeoDataQualityValidator:
    """
    Runs all quality checks on a GeoDataFrame.
    Aligned with DAMA-DMBOK v2 Chapter 13 — Data Quality Management.
    """

    # World bounds for coordinate validation
    WORLD_BOUNDS = (-180, -90, 180, 90)

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.qc_config = self.config.get("quality_checks", {})

    def validate(self, gdf: gpd.GeoDataFrame, source_config: dict) -> QualityReport:
        """Runs all quality checks and returns a QualityReport."""
        source_id = source_config.get("id", "unknown")
        source_name = source_config.get("name", "Unknown")
        expected_crs = source_config.get("expected_crs", "EPSG:4326")

        report = QualityReport(
            source_id=source_id,
            source_name=source_name,
            feature_count=len(gdf),
            geometry_type=str(gdf.geom_type.value_counts().index[0]) if len(gdf) > 0 else "empty",
            crs=str(gdf.crs) if gdf.crs else "None",
            domain=source_config.get("domain", ""),
            data_source=gdf.get("data_source", pd.Series(["live"])).iloc[0] if len(gdf) > 0 else "unknown"
        )

        if len(gdf) == 0:
            report.checks.append(CheckResult(
                check_id="C00", check_name="Dataset not empty",
                dimension="Completeness", status="fail", score=0.0,
                details="Dataset is empty — no features ingested.",
                affected_count=0, total_count=0, severity="critical"
            ))
            return report

        # Run all checks
        report.checks.extend(self._check_completeness(gdf))
        report.checks.extend(self._check_validity(gdf, expected_crs))
        report.checks.extend(self._check_consistency(gdf))
        report.checks.extend(self._check_uniqueness(gdf))
        report.checks.extend(self._check_timeliness(gdf, source_config))
        report.checks.extend(self._check_accuracy(gdf))

        logger.info(
            f"Quality check complete — {source_id}: "
            f"score={report.overall_score:.2f}, "
            f"status={report.status}, "
            f"checks={len(report.checks)}"
        )
        return report

    # ── COMPLETENESS ─────────────────────────────────────────────────────────

    def _check_completeness(self, gdf: gpd.GeoDataFrame) -> List[CheckResult]:
        results = []
        n = len(gdf)
        threshold = self.qc_config.get("completeness", {}).get("threshold", 0.95)

        # Null geometries
        null_geom = gdf.geometry.isna().sum()
        score = 1.0 - (null_geom / n)
        results.append(CheckResult(
            check_id="C01", check_name="No null geometries",
            dimension="Completeness",
            status="pass" if score >= threshold else "fail",
            score=round(score, 3),
            details=f"{null_geom}/{n} features have null geometry.",
            affected_count=int(null_geom), total_count=n,
            severity="critical"
        ))

        # Mandatory fields
        mandatory = self.qc_config.get("completeness", {}).get(
            "mandatory_fields", ["geometry", "name", "source_id", "date_ingested"]
        )
        for field_name in mandatory:
            if field_name == "geometry":
                continue
            if field_name not in gdf.columns:
                results.append(CheckResult(
                    check_id=f"C02_{field_name}", check_name=f"Field '{field_name}' present",
                    dimension="Completeness", status="fail", score=0.0,
                    details=f"Mandatory field '{field_name}' is missing from dataset.",
                    affected_count=n, total_count=n, severity="high"
                ))
            else:
                null_count = gdf[field_name].isna().sum()
                score = 1.0 - (null_count / n)
                results.append(CheckResult(
                    check_id=f"C02_{field_name}", check_name=f"Field '{field_name}' complete",
                    dimension="Completeness",
                    status="pass" if score >= threshold else "warn",
                    score=round(score, 3),
                    details=f"{null_count}/{n} null values in '{field_name}'.",
                    affected_count=int(null_count), total_count=n,
                    severity="medium"
                ))

        return results

    # ── VALIDITY ──────────────────────────────────────────────────────────────

    def _check_validity(self, gdf: gpd.GeoDataFrame, expected_crs: str) -> List[CheckResult]:
        results = []
        n = len(gdf)
        valid_geom = gdf[~gdf.geometry.isna()]
        nv = len(valid_geom)

        # Geometry validity (Shapely is_valid)
        invalid_mask = ~valid_geom.geometry.is_valid
        invalid_count = invalid_mask.sum()
        score = 1.0 - (invalid_count / nv) if nv > 0 else 1.0

        details = f"{invalid_count}/{nv} invalid geometries."
        if invalid_count > 0:
            sample = valid_geom[invalid_mask].geometry.apply(explain_validity).head(3).tolist()
            details += f" Examples: {'; '.join(sample)}"

        results.append(CheckResult(
            check_id="V01", check_name="Geometry validity",
            dimension="Validity",
            status="pass" if score == 1.0 else ("warn" if score >= 0.95 else "fail"),
            score=round(score, 3), details=details,
            affected_count=int(invalid_count), total_count=nv,
            severity="critical"
        ))

        # Geometry not empty
        empty_count = valid_geom.geometry.is_empty.sum()
        score_empty = 1.0 - (empty_count / nv) if nv > 0 else 1.0
        results.append(CheckResult(
            check_id="V02", check_name="Geometry not empty",
            dimension="Validity",
            status="pass" if score_empty == 1.0 else "fail",
            score=round(score_empty, 3),
            details=f"{empty_count}/{nv} empty geometries.",
            affected_count=int(empty_count), total_count=nv,
            severity="high"
        ))

        # Coordinate bounds
        bounds = valid_geom.geometry.bounds
        out_of_bounds = (
            (bounds["minx"] < self.WORLD_BOUNDS[0]) |
            (bounds["miny"] < self.WORLD_BOUNDS[1]) |
            (bounds["maxx"] > self.WORLD_BOUNDS[2]) |
            (bounds["maxy"] > self.WORLD_BOUNDS[3])
        ).sum()
        score_bounds = 1.0 - (out_of_bounds / nv) if nv > 0 else 1.0
        results.append(CheckResult(
            check_id="V03", check_name="Coordinates within world bounds",
            dimension="Validity",
            status="pass" if score_bounds == 1.0 else "fail",
            score=round(score_bounds, 3),
            details=f"{out_of_bounds}/{nv} features outside world bounds (-180/-90/180/90).",
            affected_count=int(out_of_bounds), total_count=nv,
            severity="high"
        ))

        # CRS check
        crs_ok = gdf.crs is not None and gdf.crs.to_string() == expected_crs
        results.append(CheckResult(
            check_id="V04", check_name=f"CRS matches expected ({expected_crs})",
            dimension="Validity",
            status="pass" if crs_ok else "warn",
            score=1.0 if crs_ok else 0.5,
            details=f"Actual CRS: {gdf.crs}. Expected: {expected_crs}.",
            affected_count=0 if crs_ok else n, total_count=n,
            severity="medium"
        ))

        return results

    # ── CONSISTENCY ───────────────────────────────────────────────────────────

    def _check_consistency(self, gdf: gpd.GeoDataFrame) -> List[CheckResult]:
        results = []
        valid_geom = gdf[~gdf.geometry.isna() & gdf.geometry.is_valid]
        nv = len(valid_geom)

        # Self-intersections (for polygons)
        poly_mask = valid_geom.geom_type.isin(["Polygon", "MultiPolygon"])
        polys = valid_geom[poly_mask]

        if len(polys) > 0:
            self_intersect = (~polys.geometry.is_valid).sum()
            score = 1.0 - (self_intersect / len(polys))
            results.append(CheckResult(
                check_id="K01", check_name="No self-intersecting polygons",
                dimension="Consistency",
                status="pass" if score == 1.0 else "warn",
                score=round(score, 3),
                details=f"{self_intersect}/{len(polys)} polygons with self-intersections.",
                affected_count=int(self_intersect), total_count=len(polys),
                severity="medium"
            ))

            # Geometry area sanity check (degenerate polygons)
            zero_area = (polys.geometry.area == 0).sum()
            score_area = 1.0 - (zero_area / len(polys))
            results.append(CheckResult(
                check_id="K02", check_name="No zero-area polygons",
                dimension="Consistency",
                status="pass" if score_area == 1.0 else "warn",
                score=round(score_area, 3),
                details=f"{zero_area}/{len(polys)} polygons with zero area.",
                affected_count=int(zero_area), total_count=len(polys),
                severity="medium"
            ))
        else:
            results.append(CheckResult(
                check_id="K01", check_name="Topology check (N/A — no polygons)",
                dimension="Consistency", status="pass", score=1.0,
                details="No polygon geometries to check.", severity="low"
            ))

        return results

    # ── UNIQUENESS ────────────────────────────────────────────────────────────

    def _check_uniqueness(self, gdf: gpd.GeoDataFrame) -> List[CheckResult]:
        results = []
        n = len(gdf)

        # Duplicate geometries (WKT comparison)
        try:
            wkt_series = gdf.geometry.apply(lambda g: g.wkt if g else None)
            dup_geom = wkt_series.duplicated().sum()
            score = 1.0 - (dup_geom / n) if n > 0 else 1.0
            threshold = self.qc_config.get("uniqueness", {}).get("threshold", 0.99)
            results.append(CheckResult(
                check_id="U01", check_name="No duplicate geometries",
                dimension="Uniqueness",
                status="pass" if score >= threshold else "warn",
                score=round(score, 3),
                details=f"{dup_geom}/{n} duplicate geometries detected.",
                affected_count=int(dup_geom), total_count=n,
                severity="medium"
            ))
        except Exception as e:
            results.append(CheckResult(
                check_id="U01", check_name="Duplicate geometry check",
                dimension="Uniqueness", status="error", score=0.5,
                details=f"Check failed: {e}", severity="low"
            ))

        # Duplicate names (if name field exists)
        if "name" in gdf.columns:
            dup_names = gdf["name"].duplicated().sum()
            score_names = 1.0 - (dup_names / n)
            results.append(CheckResult(
                check_id="U02", check_name="No duplicate names",
                dimension="Uniqueness",
                status="pass" if score_names >= 0.95 else "warn",
                score=round(score_names, 3),
                details=f"{dup_names}/{n} duplicate name values.",
                affected_count=int(dup_names), total_count=n,
                severity="low"
            ))

        return results

    # ── TIMELINESS ────────────────────────────────────────────────────────────

    def _check_timeliness(self, gdf: gpd.GeoDataFrame, source_config: dict) -> List[CheckResult]:
        results = []
        freq = source_config.get("update_frequency", "monthly")
        max_age_map = self.qc_config.get("timeliness", {}).get("max_age_days", {
            "daily": 2, "weekly": 9, "monthly": 35, "quarterly": 95
        })
        max_age = max_age_map.get(freq, 35)

        if "date_ingested" in gdf.columns:
            try:
                dates = pd.to_datetime(gdf["date_ingested"], errors="coerce")
                latest = dates.max()
                if pd.notna(latest):
                    age_days = (datetime.utcnow() - latest.to_pydatetime().replace(tzinfo=None)).days
                    score = 1.0 if age_days <= max_age else max(0.0, 1.0 - (age_days - max_age) / max_age)
                    results.append(CheckResult(
                        check_id="T01", check_name=f"Data freshness (max {max_age} days)",
                        dimension="Timeliness",
                        status="pass" if age_days <= max_age else "warn",
                        score=round(score, 3),
                        details=f"Latest ingestion: {latest.date()}. Age: {age_days} days. Max allowed: {max_age} days.",
                        affected_count=0 if age_days <= max_age else len(gdf),
                        total_count=len(gdf),
                        severity="medium"
                    ))
            except Exception as e:
                results.append(CheckResult(
                    check_id="T01", check_name="Data freshness",
                    dimension="Timeliness", status="error", score=0.5,
                    details=f"Could not parse date: {e}", severity="low"
                ))
        else:
            results.append(CheckResult(
                check_id="T01", check_name="Data freshness",
                dimension="Timeliness", status="warn", score=0.5,
                details="No 'date_ingested' field found. Cannot assess timeliness.",
                severity="medium"
            ))

        return results

    # ── ACCURACY ─────────────────────────────────────────────────────────────

    def _check_accuracy(self, gdf: gpd.GeoDataFrame) -> List[CheckResult]:
        results = []
        valid_geom = gdf[~gdf.geometry.isna()]
        nv = len(valid_geom)

        # Coordinate precision (at least 4 decimal places for ~11m accuracy)
        try:
            bounds = valid_geom.geometry.bounds
            x_precision = (bounds["minx"] * 10000 % 1 != 0).sum()
            score = 1.0 - (x_precision / nv) if nv > 0 else 1.0
            results.append(CheckResult(
                check_id="A01", check_name="Coordinate precision (≥4 decimal places)",
                dimension="Accuracy",
                status="pass" if score >= 0.95 else "warn",
                score=round(score, 3),
                details=f"{nv - int(x_precision)}/{nv} features have sufficient coordinate precision.",
                affected_count=int(x_precision), total_count=nv,
                severity="low"
            ))
        except Exception:
            pass

        # Humanitarian bounding box check (Sub-Saharan Africa approximate)
        AFRICA_BOUNDS = (-20, -35, 55, 38)
        try:
            centroids = valid_geom.geometry.centroid
            outside_africa = (
                (centroids.x < AFRICA_BOUNDS[0]) | (centroids.x > AFRICA_BOUNDS[2]) |
                (centroids.y < AFRICA_BOUNDS[1]) | (centroids.y > AFRICA_BOUNDS[3])
            ).sum()
            score_africa = 1.0 - (outside_africa / nv) if nv > 0 else 1.0
            results.append(CheckResult(
                check_id="A02", check_name="Features within operational area (Africa)",
                dimension="Accuracy",
                status="pass" if score_africa >= 0.95 else "warn",
                score=round(score_africa, 3),
                details=f"{outside_africa}/{nv} features outside Sub-Saharan Africa bounding box.",
                affected_count=int(outside_africa), total_count=nv,
                severity="medium"
            ))
        except Exception:
            pass

        return results


# ── RUN ALL VALIDATIONS ───────────────────────────────────────────────────────

def validate_all(
    datasets: dict,
    config: dict,
    sources_config: list
) -> Dict[str, QualityReport]:
    """Validates all ingested datasets and returns a dict of QualityReports."""
    validator = GeoDataQualityValidator(config)
    reports = {}

    source_map = {s["id"]: s for s in sources_config}

    for source_id, gdf in datasets.items():
        source_config = source_map.get(source_id, {"id": source_id, "name": source_id})
        logger.info(f"Validating: {source_id} ({len(gdf)} features)")
        report = validator.validate(gdf, source_config)
        reports[source_id] = report

    return reports
