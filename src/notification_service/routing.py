"""Channel selection for one recipient: preferences, mandatory rules, quiet hours.

Pure functions over already loaded data, so the decision is checkable without
a database. The order is fixed:

1. an organization's mandatory rule selects the channel whatever the
   recipient chose, and ignores quiet hours;
2. otherwise the recipient's most specific matching preference decides; with
   no preference a channel is on — opting out is the recipient's move;
3. a channel is only selected when the recipient is reachable on it; a
   mandatory channel that is not reachable is still selected, so the journal
   shows that a required delivery could not happen;
4. quiet hours postpone push channels (everything but the inbox, which
   interrupts nobody) until the window ends.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

WILDCARD = "*"
# A type is a dotted name; a pattern is a type, ``prefix.*`` or ``*``.
TYPE_RE = re.compile(r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$")
PATTERN_RE = re.compile(r"^(\*|[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*(\.\*)?)$")


def pattern_matches(pattern: str, notification_type: str) -> bool:
    if pattern == WILDCARD:
        return True
    if pattern.endswith(".*"):
        return notification_type.startswith(pattern[:-1])
    return pattern == notification_type


def specificity(pattern: str) -> tuple[int, int]:
    """Exact beats any prefix, a longer prefix beats a shorter one, ``*`` is last."""
    if pattern == WILDCARD:
        return (0, 0)
    if pattern.endswith(".*"):
        return (1, len(pattern))
    return (2, len(pattern))


@dataclass(frozen=True)
class PreferenceRule:
    type_pattern: str
    channel: str
    enabled: bool


@dataclass(frozen=True)
class QuietWindow:
    start: time
    end: time
    timezone: str

    def ends_at(self, now: datetime) -> datetime | None:
        """End of the window ``now`` falls into, or ``None`` outside of it."""
        if self.start == self.end:
            return None
        local = now.astimezone(ZoneInfo(self.timezone))
        current = local.time().replace(second=0, microsecond=0, tzinfo=None)
        if self.start < self.end:
            inside = self.start <= current < self.end
            end_day = local.date()
        else:
            # The window wraps midnight, e.g. 22:00-08:00.
            inside = current >= self.start or current < self.end
            end_day = local.date() + timedelta(days=1) if current >= self.start else local.date()
        if not inside:
            return None
        end_local = datetime.combine(end_day, self.end, tzinfo=local.tzinfo)
        return end_local.astimezone(now.tzinfo)


@dataclass(frozen=True)
class ChannelDecision:
    channel: str
    mandatory: bool
    reachable: bool
    not_before: datetime | None = None


def select_channels(
    notification_type: str,
    *,
    channels: Iterable[str],
    reachable: Mapping[str, bool],
    preferences: Iterable[PreferenceRule],
    mandatory_patterns: Mapping[str, Iterable[str]],
    push_channels: Iterable[str],
    quiet: QuietWindow | None,
    now: datetime,
) -> list[ChannelDecision]:
    prefs = list(preferences)
    push = set(push_channels)
    decisions: list[ChannelDecision] = []
    for channel in channels:
        mandatory = any(
            pattern_matches(p, notification_type) for p in mandatory_patterns.get(channel, ())
        )
        can_reach = reachable.get(channel, False)
        if not mandatory:
            matching = [
                p
                for p in prefs
                if p.channel == channel and pattern_matches(p.type_pattern, notification_type)
            ]
            enabled = (
                max(matching, key=lambda p: specificity(p.type_pattern)).enabled
                if matching
                else True
            )
            if not enabled or not can_reach:
                continue
        not_before = None
        if not mandatory and channel in push and quiet is not None:
            not_before = quiet.ends_at(now)
        decisions.append(ChannelDecision(channel, mandatory, can_reach, not_before))
    return decisions
