# Security policy

## Reporting a vulnerability

Report it privately through GitHub: open the repository's **Security** tab and
choose **Report a vulnerability**
(<https://github.com/happydave/hoardmq/security/advisories/new>). The report
becomes a draft security advisory that only you and the maintainers can see.

Please do not open a public issue, discussion or pull request about a
vulnerability before it has been fixed and announced.

A useful report says:

- the version or commit, and how HoardMQ was built and deployed (binary, image,
  Helm chart);
- the configuration that matters, with secrets removed;
- the steps or a client that reproduces it;
- what an attacker gains, and from where (a client, a peer on the cluster
  network, a local user).

## Scope

In scope:

- the broker: `cmd/hoardmq` and `internal/`;
- the extension API (`ext/`) and the modules in this repository;
- the Helm chart (`deploy/helm/hoardmq`) and the image the `Dockerfile` builds;
- the tools in `tools/`.

Out of scope:

- a defect in a third-party dependency itself: report it to its project. How
  HoardMQ uses a dependency is in scope.
- the simulation harness (`sim/`), the conformance suite and other tests;
- a deployment that chooses a setting the documentation describes as unsafe, such as
  `secret` cluster authentication on a network it does not trust, or
  anonymous access in production.

## Supported versions

| Version | Supported |
|---|---|
| `main` | yes |
| `v0.1.x`, the latest minor release line | yes |

Fixes go to `main` and to the latest minor release line. Older lines are not
patched; upgrade to the latest release.

## What to expect

HoardMQ is a small project, so these are targets, not guarantees:

- an acknowledgement within 7 days;
- an assessment within 30 days: whether the report is accepted as a
  vulnerability, and why if not;
- for an accepted report, a fix or mitigation prepared in the private
  advisory, with you invited to review it;
- coordinated disclosure: the advisory is published when the fix is
  available, by default no later than 90 days after the report, earlier or
  later by agreement with you;
- credit in the advisory, unless you ask not to be named.

## How fixes are announced

Each fix is announced as a GitHub security advisory for this repository,
naming the affected versions, the fixing commit or release, and any
workaround. A CVE identifier is requested through GitHub when the issue
warrants one. The release notes list the advisory too.
