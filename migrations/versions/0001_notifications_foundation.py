"""Notifications foundation: notifications, delivery journal, inbox, preferences, rules.

Revision ID: 0001
Revises:
Create Date: 2026-09-25
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "channel_addresses",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("address", sa.String(length=320), nullable=False),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("disabled_reason", sa.String(length=200), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "principal_id", "channel", name=op.f("pk_channel_addresses")
        ),
    )
    op.create_table(
        "channel_groups",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("external_chat_id", sa.String(length=100), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=False),
        sa.Column("role_id", sa.Uuid(), nullable=True),
        sa.Column("linked_by", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("disabled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("disabled_reason", sa.String(length=200), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_channel_groups")),
    )
    op.create_index(
        "ix_channel_groups_workspace_role",
        "channel_groups",
        ["tenant_id", "workspace_id", "role_id"],
        unique=False,
    )
    op.create_index(
        "uq_channel_groups_active_chat",
        "channel_groups",
        ["tenant_id", "channel", "external_chat_id"],
        unique=True,
        postgresql_where=sa.text("disabled_at IS NULL"),
    )
    op.create_table(
        "mandatory_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("type_pattern", sa.String(length=200), nullable=False),
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_mandatory_rules")),
        sa.UniqueConstraint(
            "tenant_id",
            "type_pattern",
            "channel",
            name=op.f("uq_mandatory_rules_tenant_id_type_pattern_channel"),
        ),
    )
    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("sender_id", sa.Uuid(), nullable=False),
        sa.Column("sender_type", sa.String(length=30), nullable=False),
        sa.Column("dedup_key", sa.String(length=200), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("recipient_kind", sa.String(length=20), nullable=False),
        sa.Column("recipient_id", sa.Uuid(), nullable=False),
        sa.Column("workspace_id", sa.Uuid(), nullable=True),
        sa.Column("type", sa.String(length=200), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("links", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("actions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "recipient_kind IN ('principal', 'role', 'group')",
            name=op.f("ck_notifications_recipient_kind"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
        sa.UniqueConstraint(
            "tenant_id",
            "sender_id",
            "dedup_key",
            name=op.f("uq_notifications_tenant_id_sender_id_dedup_key"),
        ),
    )
    op.create_index(
        "ix_notifications_tenant_created",
        "notifications",
        ["tenant_id", "created_at"],
        unique=False,
    )
    op.create_table(
        "preferences",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.Uuid(), nullable=False),
        sa.Column("type_pattern", sa.String(length=200), nullable=False),
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_preferences")),
        sa.UniqueConstraint(
            "tenant_id",
            "principal_id",
            "type_pattern",
            "channel",
            name=op.f("uq_preferences_tenant_id_principal_id_type_pattern_channel"),
        ),
    )
    op.create_table(
        "quiet_hours",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.Uuid(), nullable=False),
        sa.Column("start_minute", sa.Integer(), nullable=False),
        sa.Column("end_minute", sa.Integer(), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("end_minute BETWEEN 0 AND 1439", name=op.f("ck_quiet_hours_end_minute")),
        sa.CheckConstraint(
            "start_minute BETWEEN 0 AND 1439", name=op.f("ck_quiet_hours_start_minute")
        ),
        sa.PrimaryKeyConstraint("tenant_id", "principal_id", name=op.f("pk_quiet_hours")),
    )
    op.create_table(
        "deliveries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("notification_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=30), nullable=False),
        sa.Column("recipient_kind", sa.String(length=20), nullable=False),
        sa.Column("recipient_id", sa.Uuid(), nullable=False),
        sa.Column("iam_principal_id", sa.Uuid(), nullable=True),
        sa.Column("mandatory", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("external_id", sa.String(length=200), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "recipient_kind IN ('principal', 'group')", name=op.f("ck_deliveries_recipient_kind")
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'sending', 'delivered', 'failed')",
            name=op.f("ck_deliveries_status"),
        ),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notifications.id"],
            name=op.f("fk_deliveries_notification_id_notifications"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_deliveries")),
        sa.UniqueConstraint(
            "notification_id",
            "channel",
            "recipient_kind",
            "recipient_id",
            name=op.f("uq_deliveries_notification_id_channel_recipient_kind_recipient_id"),
        ),
    )
    op.create_index(
        "ix_deliveries_due",
        "deliveries",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status IN ('pending', 'sending')"),
    )
    op.create_table(
        "inbox_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("iam_principal_id", sa.Uuid(), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("notification_id", sa.Uuid(), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["notification_id"],
            ["notifications.id"],
            name=op.f("fk_inbox_items_notification_id_notifications"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_inbox_items")),
        sa.UniqueConstraint(
            "tenant_id",
            "iam_principal_id",
            "notification_id",
            name=op.f("uq_inbox_items_tenant_id_iam_principal_id_notification_id"),
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "iam_principal_id",
            "seq",
            name=op.f("uq_inbox_items_tenant_id_iam_principal_id_seq"),
        ),
    )
    op.create_index(
        "ix_inbox_items_unread",
        "inbox_items",
        ["tenant_id", "iam_principal_id"],
        unique=False,
        postgresql_where=sa.text("read_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_inbox_items_unread",
        table_name="inbox_items",
        postgresql_where=sa.text("read_at IS NULL"),
    )
    op.drop_table("inbox_items")
    op.drop_index(
        "ix_deliveries_due",
        table_name="deliveries",
        postgresql_where=sa.text("status IN ('pending', 'sending')"),
    )
    op.drop_table("deliveries")
    op.drop_table("quiet_hours")
    op.drop_table("preferences")
    op.drop_index("ix_notifications_tenant_created", table_name="notifications")
    op.drop_table("notifications")
    op.drop_table("mandatory_rules")
    op.drop_index(
        "uq_channel_groups_active_chat",
        table_name="channel_groups",
        postgresql_where=sa.text("disabled_at IS NULL"),
    )
    op.drop_index("ix_channel_groups_workspace_role", table_name="channel_groups")
    op.drop_table("channel_groups")
    op.drop_table("channel_addresses")
