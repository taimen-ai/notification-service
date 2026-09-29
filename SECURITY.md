# Security Policy

## Supported versions

Taimen is pre-1.0. Security fixes are made on the `main` branch of each
component and included in the next platform release; there are no long-term
support branches yet.

| Platform version | Supported |
|---|---|
| latest `main` / latest `v0.x` tag | yes |
| older tags | no |

## Reporting a vulnerability

Please do **not** open a public issue for security problems.

Report privately through GitHub's private vulnerability reporting: open the
**Security** tab of the affected repository and choose **Report a vulnerability**.
If that option is not available to you, open an issue that only says
"security: please contact me" without any details, and a maintainer will reach
out through a private channel. Include in the private report:

- the component and version (tag or commit),
- steps to reproduce or a proof of concept,
- the impact you expect (what an attacker gains),
- whether the issue is already public.

You will get an acknowledgement within 5 business days and a status update at
least every 14 days until the issue is resolved. We ask for coordinated
disclosure: please give us up to 90 days to ship a fix before publishing details.
Credit is given in the release notes unless you prefer to stay anonymous.

## Scope

In scope: the code in this repository — the Taimen Notification Service (`src/`: the send API, notification rules, the delivery worker, the web, email and Telegram channels, the Control Plane event consumer) and its Alembic migrations.

Out of scope: third-party dependencies (report upstream; tell us if a fix
requires a coordinated update), the messaging providers themselves (Telegram, SMTP servers), demo data, and deployments run by
third parties.

## Hardening notes for operators

- Keep the Telegram bot token and SMTP credentials in `secrets/`, never in `.env` committed to Git.
- Expose only the edge (Caddy); the service binds to `127.0.0.1` by default.
