"""Pipeline choropleth (jointure géométrie × agrégats précalculée -> PostGIS)."""

from .pipeline import create_pipeline

__all__ = ["create_pipeline"]
