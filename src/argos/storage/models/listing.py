"""Listing model — represents a property listing on a specific source."""

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from argos.storage.base import Base, TimestampMixin, UUIDMixin

if TYPE_CHECKING:
    from argos.storage.models.property import Property
    from argos.storage.models.source import Source


class Listing(UUIDMixin, TimestampMixin, Base):
    """A specific listing on a site."""

    __tablename__ = "listings"

    # Foreign keys
    source_id: Mapped[UUID] = mapped_column(
        ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    property_id: Mapped[UUID] = mapped_column(
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Identification
    external_id: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
        comment="The ID of the listing on the source site",
    )

    # Price
    asking_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))

    # Observation timestamps
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    # Status
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        server_default="true",
        nullable=False,
    )

    # Raw snapshot (for reprocessing)
    raw_data: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    # Relationships
    source: Mapped["Source"] = relationship()
    property_: Mapped["Property"] = relationship(back_populates="listings")

    def __repr__(self) -> str:
        return f"Listing(external_id={self.external_id!r}, price={self.asking_price})"
