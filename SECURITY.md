# Security Policy

## Supported versions

MatrixRaft is pre-1.0. Security fixes target the latest `main` and the most recent
published `0.x` release.

## Reporting a vulnerability

Please report suspected vulnerabilities **privately** via GitHub Security Advisories
— use **"Report a vulnerability"** on the Security tab of
<https://github.com/bjmeetsfo/MatrixRaft/security/advisories/new> — rather than
opening a public issue or pull request.

Include, if possible:

- a description of the issue and its impact,
- the affected version or commit,
- steps to reproduce or a proof of concept.

We aim to acknowledge reports within a few business days and will coordinate a fix
and a disclosure timeline with you.

## Scope

Because MatrixRaft encodes consensus safety contracts — read-index/lease safety,
leader-only writes, applied-index fences, snapshot floors, and membership safety —
correctness defects that could cause **stale reads, split-brain, or lost/rolled-back
writes** are treated as security issues, not merely bugs.
