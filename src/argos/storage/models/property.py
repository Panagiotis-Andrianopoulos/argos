"""Property model — represents a physical property.

A property can have multiple listings (on different sites
or at different times).
"""

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from argos.storage.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from argos.storage.models.listing import Listing


class Property(UUIDMixin, TimestampMixin, Base):
    """Physical property."""

    __tablename__ = "properties"

    # Property type
    listing_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="sale, rent",
    )

    # Physical characteristics
    total_sqm: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    bedrooms: Mapped[int | None] = mapped_column(Integer)
    bathrooms: Mapped[int | None] = mapped_column(Integer)
    year_built: Mapped[int | None] = mapped_column(Integer)

    # Location
    city: Mapped[str | None] = mapped_column(String(100))
    region: Mapped[str | None] = mapped_column(String(100))
    address: Mapped[str | None] = mapped_column(String(500))
    latitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7))
    longitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 7))

    # Relationship: a property has many listings
    listings: Mapped[list["Listing"]] = relationship(
        back_populates="property_",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"Property(id={self.id}, city={self.city!r}, " f"total_sqm={self.total_sqm})"
