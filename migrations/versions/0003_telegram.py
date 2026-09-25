"""Telegram channel: group binding codes, button presses, reverse address lookup.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-25
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "channel_group_intents",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("code_hash", sa.String(length=64), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("role_id", sa.Uuid(), nullable=True),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("group_id", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["channel_groups.id"],
            name=op.f("fk_channel_group_intents_group_id_channel_groups"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_channel_group_intents")),
        sa.UniqueConstraint("code_hash", name=op.f("uq_channel_group_intents_code_hash")),
    )
    op.create_table(
        "channel_callbacks",
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("callback_id", sa.String(length=200), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("notification_id", sa.Uuid(), nullable=False),
        sa.Column("action_id", sa.String(length=64), nullable=False),
        sa.Column("external_subject", sa.String(length=200), nullable=False),
        sa.Column("iam_principal_id", sa.Uuid(), nullable=True),
        sa.Column("result", sa.String(length=50), nullable=True),
        sa.Column("answer", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notifications.id"],
            name=op.f("fk_channel_callbacks_notification_id_notifications"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("channel", "callback_id", name=op.f("pk_channel_callbacks")),
    )
    op.create_index(
        "ix_channel_addresses_channel_address", "channel_addresses", ["channel", "address"]
    )


def downgrade() -> None:
    op.drop_index("ix_channel_addresses_channel_address", table_name="channel_addresses")
    op.drop_table("channel_callbacks")
    op.drop_table("channel_group_intents")
