"""Control Plane event consumer: cursor, dedup record, closed actions of notifications.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-25
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Tables of control_plane_client.events.sqlalchemy.SqlAlchemyCursorStore.
    op.create_table(
        "event_cursors",
        sa.Column("consumer", sa.String(length=200), nullable=False),
        sa.Column("cursor", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("consumer", name=op.f("pk_event_cursors")),
    )
    op.create_table(
        "handled_events",
        sa.Column("consumer", sa.String(length=200), nullable=False),
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("handled_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("consumer", "event_id", name=op.f("pk_handled_events")),
    )
    op.create_index(
        "ix_handled_events_consumer_handled_at",
        "handled_events",
        ["consumer", "handled_at"],
    )
    op.add_column(
        "notifications",
        sa.Column("actions_closed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "notifications",
        sa.Column("actions_outcome", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("notifications", "actions_outcome")
    op.drop_column("notifications", "actions_closed_at")
    op.drop_index("ix_handled_events_consumer_handled_at", table_name="handled_events")
    op.drop_table("handled_events")
    op.drop_table("event_cursors")
