"""Notification rules as data: immutable versions of NotificationRule (ADR-0005).

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-27
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notification_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("key", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("spec", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("spec_hash", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "state IN ('active', 'superseded', 'retired')",
            name=op.f("ck_notification_rules_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notification_rules")),
        sa.UniqueConstraint(
            "tenant_id", "key", "version", name=op.f("uq_notification_rules_tenant_id_key_version")
        ),
    )
    # One active version per key: the one the consumer executes.
    op.create_index(
        "uq_notification_rules_active_key",
        "notification_rules",
        ["tenant_id", "key"],
        unique=True,
        postgresql_where=sa.text("state = 'active'"),
    )


def downgrade() -> None:
    op.drop_index("uq_notification_rules_active_key", table_name="notification_rules")
    op.drop_table("notification_rules")
