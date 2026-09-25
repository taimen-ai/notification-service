"""Storage model.

Identifiers of people come in two kinds and the columns say which one they
hold: ``principal_id`` of an addressee is a Control Plane principal (approvals
and roles are assigned to those), ``iam_principal_id`` is the IAM identity that
reads the inbox and owns preferences (the ``sub`` of its token). The bridge
between them is the Control Plane IAM binding, resolved when a notification is
accepted.

``tenant_id`` is always the IAM tenant of the caller's token.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from control_plane_client.events.sqlalchemy import cursor_tables
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from notification_service.db import Base, utcnow

RECIPIENT_KINDS = ("principal", "role", "group")
DELIVERY_STATUSES = ("pending", "sending", "delivered", "failed")


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        CheckConstraint("recipient_kind IN ('principal', 'role', 'group')", name="recipient_kind"),
        # Deduplication is per caller: a key is the sender's own, and a shared
        # namespace would hand one sender another sender's notification back.
        UniqueConstraint("tenant_id", "sender_id", "dedup_key"),
        Index("ix_notifications_tenant_created", "tenant_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    sender_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    sender_type: Mapped[str] = mapped_column(String(30))
    dedup_key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    recipient_kind: Mapped[str] = mapped_column(String(20))
    recipient_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    workspace_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, nullable=True)
    type: Mapped[str] = mapped_column(String(200))
    title: Mapped[str] = mapped_column(String(200))
    body: Mapped[str] = mapped_column(Text, default="")
    links: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    actions: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    # Set once the actions no longer apply (the decision was taken elsewhere,
    # the request withdrawn): channels render them inactive, with the outcome.
    actions_closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    actions_outcome: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ChannelGroup(Base):
    """A group chat of a messenger bound to a workspace, optionally to a role.

    Rows are created by the channel adapter that verifies the chat: an
    administrator gets a one-time code (``ChannelGroupIntent``) and sends it to
    the bot in the group. A removed bot or an unbinding disables the row.
    """

    __tablename__ = "channel_groups"
    __table_args__ = (
        Index(
            "uq_channel_groups_active_chat",
            "tenant_id",
            "channel",
            "external_chat_id",
            unique=True,
            postgresql_where=text("disabled_at IS NULL"),
        ),
        Index("ix_channel_groups_workspace_role", "tenant_id", "workspace_id", "role_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    channel: Mapped[str] = mapped_column(String(30))
    external_chat_id: Mapped[str] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(String(200), default="")
    workspace_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    role_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, nullable=True)
    linked_by: Mapped[uuid.UUID] = mapped_column(Uuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    disabled_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)


class ChannelGroupIntent(Base):
    """A one-time code an administrator sends to the bot in a group to bind it."""

    __tablename__ = "channel_group_intents"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    channel: Mapped[str] = mapped_column(String(30))
    # Only the hash is kept: the code is shown to the administrator once.
    code_hash: Mapped[str] = mapped_column(String(64), unique=True)
    workspace_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    role_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, nullable=True)
    created_by: Mapped[uuid.UUID] = mapped_column(Uuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    group_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("channel_groups.id", ondelete="SET NULL"), nullable=True
    )


class ChannelCallback(Base):
    """A press of an action button, keyed by the channel's own callback id.

    A redelivered callback finds its row and gets the recorded answer instead
    of a second decision; the decision itself also carries the callback id as
    its ``Idempotency-Key``.
    """

    __tablename__ = "channel_callbacks"

    channel: Mapped[str] = mapped_column(String(30), primary_key=True)
    callback_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("notifications.id", ondelete="CASCADE")
    )
    action_id: Mapped[str] = mapped_column(String(64))
    external_subject: Mapped[str] = mapped_column(String(200))
    iam_principal_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, nullable=True)
    # ``None`` while in flight; then the outcome code (``decided``,
    # ``already_decided``, ``not_eligible``, ...) and what the person was told.
    result: Mapped[str | None] = mapped_column(String(50), nullable=True)
    answer: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Delivery(Base):
    """One notification to one recipient over one channel: the delivery journal."""

    __tablename__ = "deliveries"
    __table_args__ = (
        CheckConstraint("recipient_kind IN ('principal', 'group')", name="recipient_kind"),
        CheckConstraint("status IN ('pending', 'sending', 'delivered', 'failed')", name="status"),
        UniqueConstraint("notification_id", "channel", "recipient_kind", "recipient_id"),
        Index(
            "ix_deliveries_due",
            "next_attempt_at",
            postgresql_where=text("status IN ('pending', 'sending')"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("notifications.id", ondelete="CASCADE")
    )
    channel: Mapped[str] = mapped_column(String(30))
    recipient_kind: Mapped[str] = mapped_column(String(20))
    # Control Plane principal id or channel group id, depending on the kind.
    recipient_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    iam_principal_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, nullable=True)
    mandatory: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class InboxItem(Base):
    """What the ``web`` channel delivered: the recipient's inbox.

    ``seq`` grows per recipient and is assigned under a per-recipient lock, so
    rows become visible strictly in ``seq`` order — that is what makes
    ``Last-Event-ID`` a safe resume point for the stream.
    """

    __tablename__ = "inbox_items"
    __table_args__ = (
        UniqueConstraint("tenant_id", "iam_principal_id", "seq"),
        UniqueConstraint("tenant_id", "iam_principal_id", "notification_id"),
        Index(
            "ix_inbox_items_unread",
            "tenant_id",
            "iam_principal_id",
            postgresql_where=text("read_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    iam_principal_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    seq: Mapped[int] = mapped_column(BigInteger)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("notifications.id", ondelete="CASCADE")
    )
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Preference(Base):
    """A recipient's choice for notifications matching ``type_pattern`` on a channel."""

    __tablename__ = "preferences"
    __table_args__ = (UniqueConstraint("tenant_id", "principal_id", "type_pattern", "channel"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    principal_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    type_pattern: Mapped[str] = mapped_column(String(200))
    channel: Mapped[str] = mapped_column(String(30))
    enabled: Mapped[bool] = mapped_column(Boolean)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class MandatoryRule(Base):
    """An organization rule: matching notifications always go to ``channel``."""

    __tablename__ = "mandatory_rules"
    __table_args__ = (UniqueConstraint("tenant_id", "type_pattern", "channel"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    type_pattern: Mapped[str] = mapped_column(String(200))
    channel: Mapped[str] = mapped_column(String(30))
    created_by: Mapped[uuid.UUID] = mapped_column(Uuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class QuietHours(Base):
    """A daily window in the recipient's time zone when push channels wait."""

    __tablename__ = "quiet_hours"
    __table_args__ = (
        CheckConstraint("start_minute BETWEEN 0 AND 1439", name="start_minute"),
        CheckConstraint("end_minute BETWEEN 0 AND 1439", name="end_minute"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    principal_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    start_minute: Mapped[int] = mapped_column(Integer)
    end_minute: Mapped[int] = mapped_column(Integer)
    timezone: Mapped[str] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ChannelAddress(Base):
    """Where a recipient is reached on an address-based channel (email, Telegram).

    A channel that reports the address as unreachable disables it; setting the
    address again re-enables it. The ``telegram`` address is the private chat
    with the bot, whose id is the person's Telegram user id: the webhook finds
    the person who pressed a button by it.
    """

    __tablename__ = "channel_addresses"
    __table_args__ = (Index("ix_channel_addresses_channel_address", "channel", "address"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    principal_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    channel: Mapped[str] = mapped_column(String(30), primary_key=True)
    address: Mapped[str] = mapped_column(String(320))
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    disabled_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


# Position and dedup record of the Control Plane event consumer (SDK tables,
# ``event_cursors`` and ``handled_events``), declared here for the migrations.
EVENT_CURSORS, HANDLED_EVENTS = cursor_tables(Base.metadata)
