"""Source model — represents a data source (Spitogatos, XE, etc.)."""

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from argos.storage.base import Base, TimestampMixin, UUIDMixin


class Source(UUIDMixin, TimestampMixin, Base):
    """Real estate listings source.

    Each listing refers to a specific source.
    """

    __tablename__ = "sources"

    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)

    def __repr__(self) -> str:
        return f"Source(name={self.name!r})"
