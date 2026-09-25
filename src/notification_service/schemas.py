"""HTTP contract: request/response models (camelCase over the wire)."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, time
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

from notification_service.routing import PATTERN_RE, TYPE_RE

MAX_TITLE = 200
MAX_BODY = 4000
CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class ApiModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
        extra="forbid",
    )


def _no_control_chars(value: str) -> str:
    if CONTROL_CHARS.search(value):
        raise ValueError("control characters are not allowed")
    return value


# --- Sending -------------------------------------------------------------------


class Recipient(ApiModel):
    """Who is addressed: a Control Plane principal, a role in a workspace or a group."""

    kind: Literal["principal", "role", "group"]
    id: uuid.UUID
    workspace_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def _workspace_for_role(self) -> Recipient:
        if self.kind == "role" and self.workspace_id is None:
            raise ValueError("a role is addressed within a workspace: workspaceId is required")
        return self


class Link(ApiModel):
    label: str = Field(min_length=1, max_length=100)
    url: AnyHttpUrl

    _plain = field_validator("label")(_no_control_chars)


class Action(ApiModel):
    """Something the recipient can do from the notification (a channel may render it)."""

    id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")
    label: str = Field(min_length=1, max_length=64)
    # Opaque to this service; the channel that executes actions interprets it.
    data: dict[str, Any] = Field(default_factory=dict)

    _plain = field_validator("label")(_no_control_chars)


class NotificationContent(ApiModel):
    """A notification without actions: what any sender, a skill too, may send."""

    recipient: Recipient
    type: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=MAX_TITLE)
    body: str = Field(default="", max_length=MAX_BODY)
    links: list[Link] = Field(default_factory=list, max_length=10)

    @field_validator("type")
    @classmethod
    def _type_shape(cls, value: str) -> str:
        if not TYPE_RE.match(value):
            raise ValueError("type is a dotted name, e.g. 'approval.requested'")
        return value

    @field_validator("title")
    @classmethod
    def _title_one_line(cls, value: str) -> str:
        if "\n" in value or "\r" in value:
            raise ValueError("title is a single line")
        return _no_control_chars(value)

    @field_validator("body")
    @classmethod
    def _body_plain(cls, value: str) -> str:
        return _no_control_chars(value)


class NotificationCreate(NotificationContent):
    actions: list[Action] = Field(default_factory=list, max_length=5)


class DeliveryOut(ApiModel):
    id: uuid.UUID
    channel: str
    recipient_kind: str
    recipient_id: uuid.UUID
    mandatory: bool
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    delivered_at: datetime | None


class NotificationOut(ApiModel):
    id: uuid.UUID
    type: str
    title: str
    body: str
    links: list[dict[str, Any]]
    actions: list[dict[str, Any]]
    # When the actions stopped applying and why (e.g. ``{"status": "approved"}``);
    # ``null`` while they are open.
    actions_closed_at: datetime | None = None
    actions_outcome: dict[str, Any] | None = None
    recipient: Recipient
    sender_id: uuid.UUID
    created_at: datetime


class SentNotificationOut(NotificationOut):
    deliveries: list[DeliveryOut]


# --- Inbox ---------------------------------------------------------------------


class InboxItemOut(ApiModel):
    id: uuid.UUID
    seq: int
    notification_id: uuid.UUID
    type: str
    title: str
    body: str
    links: list[dict[str, Any]]
    actions: list[dict[str, Any]]
    actions_closed_at: datetime | None = None
    actions_outcome: dict[str, Any] | None = None
    sender_id: uuid.UUID
    created_at: datetime
    read_at: datetime | None


class InboxPageOut(ApiModel):
    items: list[InboxItemOut]
    unread_count: int
    next_cursor: str | None


class ReadAllOut(ApiModel):
    marked: int


# --- Preferences and rules -----------------------------------------------------

TypePattern = Annotated[str, Field(min_length=1, max_length=200)]


def _pattern_shape(value: str) -> str:
    if not PATTERN_RE.match(value):
        raise ValueError("type pattern is a type, 'prefix.*' or '*'")
    return value


class PreferenceIn(ApiModel):
    type: TypePattern
    channel: str = Field(min_length=1, max_length=30)
    # ``None`` removes the preference, returning to the default (on).
    enabled: bool | None

    _pattern = field_validator("type")(_pattern_shape)


class PreferenceOut(ApiModel):
    type: str
    channel: str
    enabled: bool


class QuietHoursIn(ApiModel):
    start: time
    end: time
    timezone: str = Field(min_length=1, max_length=64)

    @field_validator("timezone")
    @classmethod
    def _known_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("unknown time zone") from exc
        return value


class QuietHoursOut(ApiModel):
    start: str
    end: str
    timezone: str


class ChannelAddressOut(ApiModel):
    channel: str
    address: str
    disabled_at: datetime | None
    disabled_reason: str | None


class MandatoryRuleIn(ApiModel):
    type: TypePattern
    channel: str = Field(min_length=1, max_length=30)

    _pattern = field_validator("type")(_pattern_shape)


class MandatoryRuleOut(ApiModel):
    id: uuid.UUID
    type: str
    channel: str
    created_by: uuid.UUID
    created_at: datetime


class PreferencesPatch(ApiModel):
    """Only the fields present change; ``quietHours: null`` and ``email: null`` clear."""

    preferences: list[PreferenceIn] = Field(default_factory=list, max_length=200)
    quiet_hours: QuietHoursIn | None = None
    email: EmailStr | None = None


class PreferencesOut(ApiModel):
    channels: list[str]
    preferences: list[PreferenceOut]
    quiet_hours: QuietHoursOut | None
    addresses: list[ChannelAddressOut]
    mandatory: list[MandatoryRuleOut]


# --- Channel groups --------------------------------------------------------------


class ChannelGroupCreate(ApiModel):
    """Bind a group chat to the workspace (and, optionally, to a role in it)."""

    channel: str = Field(default="telegram", min_length=1, max_length=30)
    role_id: uuid.UUID | None = None


class ChannelGroupIntentOut(ApiModel):
    """The one-time code to send to the bot in the group; shown only here."""

    id: uuid.UUID
    channel: str
    workspace_id: uuid.UUID
    role_id: uuid.UUID | None
    code: str
    # What to send in the group, and a link that adds the bot and sends it
    # (when the bot's username is configured).
    command: str
    deep_link: str | None
    expires_at: datetime


class ChannelGroupOut(ApiModel):
    id: uuid.UUID
    channel: str
    external_chat_id: str
    title: str
    workspace_id: uuid.UUID
    role_id: uuid.UUID | None
    linked_by: uuid.UUID
    created_at: datetime
    disabled_at: datetime | None
    disabled_reason: str | None
