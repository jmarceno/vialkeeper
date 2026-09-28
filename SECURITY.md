# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Report it
privately instead:

1. Use GitHub's private vulnerability reporting on this repository
   (**Security → Report a vulnerability**), or
2. Open a
   [GitHub security advisory](https://github.com/jmarceno/vialkeeper/security/advisories/new)
   draft.

Include, as far as you can:

- the affected route or component (for example `/v1/databases/:uuid/query`),
- a minimal reproduction (request shape plus the host configuration in
  `host.toml`, with secrets removed),
- the impact you observed: data disclosure, data loss, authentication bypass,
  privilege boundary, denial of service, or something else,
- the VialKeeper version (`bin/vial_keeper eval 'IO.inspect(VialKeeper.Diagnostics.runtime(), pretty: true)'`)
  and the deployment details (host OS, TLS on/off, auth on/off).

You will get an acknowledgement, a maintainer assessment of severity, and a
coordination plan before any public disclosure. Please allow reasonable time
for a fix and a release before you publish details.

## What to look at first

VialKeeper is a network service that holds application data and operator
secrets, so these boundaries matter most:

- **Authentication and TLS.** Binding a non-loopback address requires
  `[auth] enabled = true` or `[tls] enabled = true` unless
  `[security] allow_insecure_remote = true` is set deliberately. Bearer tokens
  are compared by SHA-256 digest; clients send the raw token. There is no
  runtime token revocation API, so treat a token as valid until the host is
  restarted with the digest removed.
- **Path handling.** Clients never send absolute paths. Create/register take
  relative paths that must not escape the database root (`..` and symlinks are
  rejected).
- **Limits and admission.** Host ceilings in `host.toml` `[limits]` and
  `[admission]` bound body size, batch size, query results, subscription
  membership, attachment size, and rebuild duration. Public error codes
  (`resource_limit`, `payload_too_large`, `database_overloaded`,
  `subscription_overloaded`, `attachment_overloaded`) are the observable
  behaviour; exhausting a limit should never be a way to read other data.
- **Isolation between databases.** A database UUID grants access to that
  database. Federation reads several ordinary databases in one request and is
  read-only. Shadow reads never serve a closed or unregistered source.
- **The embedded console.** `/ui` shell and `/ui/assets/…` are anonymous so the
  browser can collect a token; every `/ui/fragments/…` and `/ui/actions/…`
  request requires the same bearer token as the public API.

Deployment, auth, TLS, and limit configuration: [Operations.md](Operations.md).

## Out of scope

- Running the host on a public interface with auth and TLS disabled
  (including via `allow_insecure_remote = true`).
- Weak or shared bearer tokens, and tokens committed to the repository or
  pasted into issues.
- Denial of service from a client that simply exceeds documented limits.
- Vulnerabilities in upstream dependencies that are not reachable from the
  shipped release; report those upstream.
- Deploying a release built for a different OS/ABI than the target host, or
  restoring a bundle on a host where `mix check.full`'s clean-host restore drill
  has not passed.

## Supported versions

VialKeeper is pre-1.0 and the on-disk format carries no compatibility promise
across releases. Fixes land on the default branch and ship in the next release;
there is no long-term support branch during the `0.x` series.
