# Security Policy

## Supported versions

This project is in active development. Security fixes are applied to
`main` and to the most recent tagged release. Older tags are not
maintained.

| Branch / tag    | Supported          |
| --------------- | ------------------ |
| `main`          | :white_check_mark: |
| Latest `v0.x.y` | :white_check_mark: |
| Older `v0.x.y`  | :x:                |

Once the project hits `v1.0.0`, this table will pivot to maintaining
the latest minor of the current major plus one previous major.

## Reporting a vulnerability

**Do not open a public GitHub issue for security problems.** Public
issues are crawled by bots and copy-pasted into Slack / Discord
seconds after they go up.

Report privately, through one of these channels:

1. **GitHub private vulnerability reports** (preferred):
   https://github.com/eveys-mobility/OCPP/security/advisories/new
2. **Email**: mostafa21tr@gmail.com
   - Subject line should start with `[SECURITY]`.
   - PGP / signed mail welcome but not required.

Please include:

- A description of the issue and the impact you observed.
- Reproduction steps or a minimal proof-of-concept.
- The branch / commit / tag you tested against.
- Your environment (OS, Python version, charger firmware if relevant).
- Whether the issue is already public elsewhere.

## What to expect

| Stage                    | Target turnaround                     |
| ------------------------ | ------------------------------------- |
| Acknowledgement          | within **3 business days**            |
| Initial assessment       | within **7 business days**            |
| Fix or mitigation plan   | within **30 days** for high/critical  |
| Public disclosure        | coordinated, after a fix is available |

If a report is **accepted**, the maintainer will:

- Confirm the issue and assign a severity (CVSS-style: low / medium /
  high / critical).
- Work on a fix in a private branch.
- Coordinate disclosure timing with the reporter.
- Credit the reporter in the release notes (unless they prefer to stay
  anonymous).

If a report is **declined** (e.g. it's not actually a vulnerability,
or it's already known and tracked), the maintainer will explain why
in writing.

## Scope

In scope:

- Authentication and authorization of OCPP charger connections.
- Authentication of the gateway REST API.
- Input validation on OCPP messages, REST bodies, gRPC requests.
- Secrets / token handling in `Settings` and the runtime.
- Dependency vulnerabilities that affect this codebase.
- Anything that could let one charger affect another, or let an
  unauthenticated party read charger / transaction data.

Out of scope (please don't report these as vulnerabilities):

- Findings against deliberately-permissive dev defaults
  (e.g. `EVEYS_OCPP_REST_AUTH_DISABLED=true`, which is documented in
  `Settings` as a dev-only knob and logs a loud warning at boot when
  enabled).
- Issues in upstream dependencies that already have a public CVE and a
  pinned-version fix path; open a regular issue tagged `dependency`.
- Denial-of-service via traffic volume against a single dev instance.
- Missing TLS at the gateway layer — TLS is terminated upstream by
  Envoy in production by design.

## Safe-harbor

Good-faith security research that follows this policy will not be
pursued legally. Please:

- Don't access data beyond what's needed to demonstrate the issue.
- Don't degrade service for other users.
- Give the maintainer a reasonable window to fix before going public.
