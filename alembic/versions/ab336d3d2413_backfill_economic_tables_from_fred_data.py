"""backfill economic tables from fred data

Revision ID: ab336d3d2413
Revises: 596d1786d618
Create Date: 2026-05-04 16:33:55.232122

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ab336d3d2413"
down_revision: str | Sequence[str] | None = "596d1786d618"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Copy existing FRED rows into the unified economic_* tables.

    Read-only on the source tables; idempotent on the destination via
    ON CONFLICT DO NOTHING. Safe to re-run if interrupted. FRED-specific
    fields with no first-class home in the unified schema are preserved
    in the JSONB extra_metadata columns.
    """
    # Series first because observations depend on them via FK.
    # observation_start, observation_end, last_ingested_at are cast to
    # text so they survive a JSON round-trip (jsonb has no native date type).
    op.execute(
        """
        INSERT INTO economic_series (
            source,
            series_id,
            title,
            frequency,
            units,
            seasonal_adjustment,
            last_updated,
            extra_metadata,
            created_at,
            updated_at
        )
        SELECT
            'fred'::data_source AS source,
            series_id,
            title,
            frequency,
            units,
            seasonal_adjustment,
            last_updated,
            jsonb_build_object(
                'notes', notes,
                'frequency_short', frequency_short,
                'units_short', units_short,
                'seasonal_adjustment_short', seasonal_adjustment_short,
                'observation_start', observation_start::text,
                'observation_end', observation_end::text,
                'last_ingested_at', last_ingested_at::text
            ) AS extra_metadata,
            created_at,
            updated_at
        FROM fred_series
        ON CONFLICT (source, series_id) DO NOTHING;
        """
    )

    # Observations: realtime_start and realtime_end are FRED revision
    # vintage fields, preserved in extra_metadata for audit/reconstruction.
    op.execute(
        """
        INSERT INTO economic_observations (
            source,
            series_id,
            observation_date,
            value,
            status,
            extra_metadata,
            ingested_at
        )
        SELECT
            'fred'::data_source AS source,
            series_id,
            observation_date,
            value,
            NULL AS status,
            jsonb_build_object(
                'realtime_start', realtime_start::text,
                'realtime_end', realtime_end::text
            ) AS extra_metadata,
            ingested_at
        FROM fred_observations
        ON CONFLICT (source, series_id, observation_date) DO NOTHING;
        """
    )


def downgrade() -> None:
    """Remove FRED-sourced rows form the unified tables.

    The original fred_* tables are untouched, so this is non-destructive
    as long as the next step (drop legacy fred_* tables) has not yet run.
    """
    op.execute("DELETE FROM economic_observations WHERE source = 'fred'::data_source;")
    op.execute("DELETE FROM economic_series WHERE source = 'fred'::data_source;")
