"""SQLAlchemy models for the ARGOS data warehouse.

Importing here is important: when we import argos.storage.models,
all models are registered in the Base metadata — Alembic finds them
later for autogenerate.
"""

from argos.storage.models.fred import FredObservation, FredSeries
from argos.storage.models.listing import Listing
from argos.storage.models.property import Property
from argos.storage.models.source import Source

__all__ = ["FredObservation", "FredSeries", "Listing", "Property", "Source"]
