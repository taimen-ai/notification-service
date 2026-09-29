# Contributing to Taimen Notification Service

Thank you for taking the time to contribute. Taimen is an organizational
runtime in which people, AI agents, workflows and services execute the work
of an organization; the platform is developed in the open under the
Apache License 2.0. This repository holds the **Notification Service** — it accepts notifications from any service or skill, routes them to channels by recipient preferences (web inbox, Telegram, email), keeps a delivery log and turns channel buttons into human decisions in the Control Plane.

## Before you start

- Read the platform overview in the
  [guide](https://github.com/taimen-ai/taimen/tree/main/guide/docs/overview)
  (in Russian).
  Architecture decisions are recorded as ADRs (in Russian, with an English
  title line); English summaries are provided on request in the ADR's
  discussion.
- Check the open issues before starting a large change. For anything that changes
  an API, a contract or a service boundary, open an issue first and propose
  an ADR.

## Contributor License Agreement

We require a signed Contributor License Agreement (CLA) for every
contribution, so that the project can be relicensed or defended without
tracking down every author. You sign once for all Taimen repositories.

- Individuals: [`cla/CLA-individual.md`](https://github.com/taimen-ai/taimen/blob/main/cla/CLA-individual.md)
- Companies contributing on behalf of employees: [`cla/CLA-entity.md`](https://github.com/taimen-ai/taimen/blob/main/cla/CLA-entity.md)

The CLA grants the project a copyright and patent licence to your
contribution; you keep your copyright.

## Development setup

The component is a [uv](https://docs.astral.sh/uv/) project on Python 3.12.
It depends on sibling repositories by path (`../platform-auth-sdk`, `../control-plane/client` and `../control-plane`), so develop it from
the umbrella checkout, where the siblings are submodules:

```bash
git clone --recurse-submodules https://github.com/taimen-ai/taimen.git
cd taimen/notification-service
uv sync                       # runtime dependencies plus the `dev` group
uv run pytest                 # tests
uv run ruff check .           # lint
uv run ruff format --check .  # formatting
```

## Pull requests

- One logical change per pull request; keep the history linear (rebase, no
  merge commits).
- Tests and `ruff check` / `ruff format --check` must pass; behaviour changes
  come with tests.
- Commit messages explain *why*, not *what*; reference the ADR or issue.
- Public contract changes update the README.
- Do not include secrets, customer data or internal hostnames.

## Reporting bugs and security issues

Bugs: open an issue in this repository with the version, steps to reproduce
and logs. Security issues: see [SECURITY.md](SECURITY.md) and do not open a
public issue.

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
