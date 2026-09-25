"""SSE inbox stream over a real HTTP server: live delivery and catch-up (FR-012)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
import uvicorn
from platform_auth import TokenVerifier
from sqlalchemy.ext.asyncio import AsyncEngine

from notification_service.app import Overrides, create_app

from conftest import FakeDirectory, Person, auth, notification, settings_for, to_principal


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
async def server(
    engine: AsyncEngine, sessions: Any, verifier: TokenVerifier, directory: FakeDirectory
) -> AsyncIterator[str]:
    # The worker runs as in production: the stream sees deliveries made by it.
    app = create_app(
        settings_for(worker_enabled=True, worker_poll_seconds=0.1),
        Overrides(engine=engine, verifier=verifier, directory=directory),
    )
    port = free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    instance = uvicorn.Server(config)
    task = asyncio.create_task(instance.serve())
    for _ in range(500):
        if instance.started:
            break
        await asyncio.sleep(0.02)
    assert instance.started, "test server did not start"
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        instance.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)


async def events(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    """Parse ``text/event-stream`` into ``{id, event, data}`` dicts, skipping comments."""
    current: dict[str, Any] = {}
    async for line in response.aiter_lines():
        if not line:
            if "data" in current:
                yield current
            current = {}
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        value = value.removeprefix(" ")
        current[name] = json.loads(value) if name == "data" else value


async def take(stream: AsyncIterator[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    async def collect() -> list[dict[str, Any]]:
        return [await anext(stream) for _ in range(n)]

    return await asyncio.wait_for(collect(), timeout=10)


async def send(client: httpx.AsyncClient, token: str, person: Person, title: str) -> None:
    response = await client.post(
        "/api/v1/notifications",
        json=notification(to_principal(person), title=title),
        headers={**auth(token), "Idempotency-Key": uuid.uuid4().hex},
    )
    assert response.status_code == 201, response.text


async def test_stream_delivers_live_and_catches_up_after_reconnect(
    server: str, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    me = auth(token(person.iam_principal_id))
    async with httpx.AsyncClient(base_url=server, timeout=10) as client:
        # Connected: a new notification arrives without polling.
        async with client.stream("GET", "/api/v1/me/notifications/stream", headers=me) as live:
            assert live.headers["content-type"].startswith("text/event-stream")
            stream = events(live)
            await send(client, token(), person, "first")
            [first] = await take(stream, 1)
        assert first["event"] == "notification"
        assert first["data"]["title"] == "first"

        # Disconnected while two more arrive.
        await send(client, token(), person, "second")
        await send(client, token(), person, "third")
        await asyncio.sleep(0.5)

        # Reconnect with the last seen id: the missed ones come first, in order,
        # then the stream stays live.
        headers = {**me, "Last-Event-ID": first["id"]}
        async with client.stream(
            "GET", "/api/v1/me/notifications/stream", headers=headers
        ) as again:
            stream = events(again)
            missed = await take(stream, 2)
            await send(client, token(), person, "fourth")
            [fourth] = await take(stream, 1)

    assert [e["data"]["title"] for e in missed] == ["second", "third"]
    assert [int(e["id"]) for e in [first, *missed, fourth]] == [1, 2, 3, 4]
    assert fourth["data"]["title"] == "fourth"


async def test_stream_resumes_from_query_parameter_and_is_private(
    server: str, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person, other = make_person(), make_person()
    async with httpx.AsyncClient(base_url=server, timeout=10) as client:
        for title in ("a", "b"):
            await send(client, token(), person, title)
        await send(client, token(), other, "not yours")
        await asyncio.sleep(0.5)

        async with client.stream(
            "GET",
            "/api/v1/me/notifications/stream",
            params={"lastEventId": "0"},
            headers=auth(token(person.iam_principal_id)),
        ) as response:
            received = await take(events(response), 2)

        invalid = await client.get(
            "/api/v1/me/notifications/stream",
            headers={**auth(token(person.iam_principal_id)), "Last-Event-ID": "abc"},
        )
        anonymous = await client.get("/api/v1/me/notifications/stream")

    assert [e["data"]["title"] for e in received] == ["a", "b"]
    assert invalid.status_code == 422
    assert anonymous.status_code == 401


async def test_stream_ends_when_the_token_expires(
    server: str, token: Callable[..., str], make_person: Callable[..., Person]
) -> None:
    person = make_person()
    short = auth(token(person.iam_principal_id, ttl_seconds=1))
    async with (
        httpx.AsyncClient(base_url=server, timeout=10) as client,
        client.stream("GET", "/api/v1/me/notifications/stream", headers=short) as response,
    ):
        body = await asyncio.wait_for(response.aread(), timeout=10)
    assert response.status_code == 200
    assert body.startswith(b"retry:")
