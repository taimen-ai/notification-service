"""``notification-service``: run the API and the delivery worker."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "notification_service.app:create_app",
        factory=True,
        host=os.environ.get("NS_HOST", "0.0.0.0"),
        port=int(os.environ.get("NS_PORT", "8000")),
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
