"""drop legacy fred_series and fred_observations tables

Revision ID: ed892e36f992
Revises: ab336d3d2413
Create Date: 2026-05-05 14:30:35.148761

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ed892e36f992"
down_revision: str | Sequence[str] | None = "ab336d3d2413"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop the legacy fred_* tables.

    These tables were superseded by the unified economic_series and
    economic_observations in revision 596d1786d618. Their data was
    migrated to the new schema in revision ab336d3d2413, after which
    the FRED ingestion pipeline switched to writing to the new tables.
    The fred_* tables have been read-only since that switchover and
    are no longer referenced by any code path.
    """
    # observations first because of the FK to fred_series
    op.drop_table("fred_observations")
    op.drop_table("fred_series")


def downgrade() -> None:
    """Recreate the legacy fred_* tables (empty).

    Note: this only restores the schema, not the data. If you need to
    reverse the schema split entirely, you would also have to copy
    rows back from economic_* — but at that point you should probably
    just stop and reconsider what you're doing.
    """
    op.create_table(
        "fred_series",
        sa.Column("series_id", sa.String(length=50), primary_key=True),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("frequency", sa.String(length=50), nullable=False),
        sa.Column("frequency_short", sa.String(length=5), nullable=False),
        sa.Column("units", sa.String(length=200), nullable=False),
        sa.Column("units_short", sa.String(length=50), nullable=False),
        sa.Column("seasonal_adjustment", sa.String(length=100), nullable=False),
        sa.Column("seasonal_adjustment_short", sa.String(length=10), nullable=False),
        sa.Column("notes", sa.String(), nullable=True),
        sa.Column("observation_start", sa.Date(), nullable=False),
        sa.Column("observation_end", sa.Date(), nullable=False),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "last_ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_table(
        "fred_observations",
        sa.Column("series_id", sa.String(length=50), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("value", sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column("realtime_start", sa.Date(), nullable=False),
        sa.Column("realtime_end", sa.Date(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("series_id", "observation_date"),
        sa.ForeignKeyConstraint(
            ["series_id"],
            ["fred_series.series_id"],
            ondelete="CASCADE",
        ),
    )
