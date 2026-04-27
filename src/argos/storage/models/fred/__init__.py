"""FRED-related SQLAlchemy models."""

from argos.storage.models.fred.observation import FredObservation
from argos.storage.models.fred.series import FredSeries

__all__ = ["FredObservation", "FredSeries"]
