"""Channel selection rules, without a database."""

from __future__ import annotations

from datetime import UTC, datetime, time

import pytest

from notification_service.routing import (
    PreferenceRule,
    QuietWindow,
    pattern_matches,
    select_channels,
)

NOW = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)
CHANNELS = ["web", "email"]
REACHABLE = {"web": True, "email": True}


def select(type_: str = "work.review_requested", **overrides: object) -> dict[str, object]:
    args: dict[str, object] = {
        "channels": CHANNELS,
        "reachable": REACHABLE,
        "preferences": [],
        "mandatory_patterns": {},
        "push_channels": ["email"],
        "quiet": None,
        "now": NOW,
    }
    args.update(overrides)
    return {d.channel: d for d in select_channels(type_, **args)}  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("pattern", "type_", "matches"),
    [
        ("*", "a.b", True),
        ("a.*", "a.b", True),
        ("a.*", "a.b.c", True),
        ("a.*", "ab.c", False),
        ("a.b", "a.b", True),
        ("a.b", "a.b.c", False),
    ],
)
def test_pattern_matching(pattern: str, type_: str, matches: bool) -> None:
    assert pattern_matches(pattern, type_) is matches


def test_every_reachable_channel_is_on_by_default() -> None:
    assert set(select()) == {"web", "email"}


def test_unreachable_channel_is_not_selected() -> None:
    assert set(select(reachable={"web": True, "email": False})) == {"web"}


def test_recipient_opts_out_of_a_channel() -> None:
    chosen = select(preferences=[PreferenceRule("work.*", "email", False)])
    assert set(chosen) == {"web"}


def test_most_specific_preference_wins() -> None:
    prefs = [
        PreferenceRule("*", "email", False),
        PreferenceRule("work.*", "email", True),
        PreferenceRule("work.review_requested", "email", False),
    ]
    assert "email" not in select(preferences=prefs)
    assert "email" in select("work.done", preferences=prefs)
    assert "email" not in select("other.thing", preferences=prefs)


def test_mandatory_rule_overrides_opt_out() -> None:
    chosen = select(
        preferences=[PreferenceRule("*", "email", False)],
        mandatory_patterns={"email": ["work.*"]},
    )
    assert chosen["email"].mandatory is True
    assert chosen["web"].mandatory is False


def test_mandatory_channel_is_kept_even_when_unreachable() -> None:
    chosen = select(reachable={"web": True, "email": False}, mandatory_patterns={"email": ["*"]})
    assert chosen["email"].reachable is False


def test_mandatory_rule_of_another_type_does_not_apply() -> None:
    chosen = select(
        preferences=[PreferenceRule("*", "email", False)],
        mandatory_patterns={"email": ["billing.*"]},
    )
    assert "email" not in chosen


def test_quiet_hours_postpone_push_channels_only() -> None:
    # 12:00 UTC is 15:00 in Moscow, inside 14:00-16:00.
    quiet = QuietWindow(time(14, 0), time(16, 0), "Europe/Moscow")
    chosen = select(quiet=quiet)
    assert chosen["web"].not_before is None
    assert chosen["email"].not_before == datetime(2026, 9, 25, 13, 0, tzinfo=UTC)


def test_quiet_hours_do_not_hold_mandatory_delivery() -> None:
    quiet = QuietWindow(time(14, 0), time(16, 0), "Europe/Moscow")
    chosen = select(quiet=quiet, mandatory_patterns={"email": ["*"]})
    assert chosen["email"].not_before is None


@pytest.mark.parametrize(
    ("now", "end"),
    [
        # 23:30 local: the window ends tomorrow at 08:00.
        (datetime(2026, 9, 25, 23, 30, tzinfo=UTC), datetime(2026, 9, 26, 8, 0, tzinfo=UTC)),
        # 03:00 local: the window ends today at 08:00.
        (datetime(2026, 9, 25, 3, 0, tzinfo=UTC), datetime(2026, 9, 25, 8, 0, tzinfo=UTC)),
        # 12:00 local: outside the window.
        (datetime(2026, 9, 25, 12, 0, tzinfo=UTC), None),
    ],
)
def test_quiet_window_across_midnight(now: datetime, end: datetime | None) -> None:
    window = QuietWindow(time(22, 0), time(8, 0), "UTC")
    assert window.ends_at(now) == end


def test_empty_quiet_window_never_applies() -> None:
    assert QuietWindow(time(8, 0), time(8, 0), "UTC").ends_at(NOW) is None
