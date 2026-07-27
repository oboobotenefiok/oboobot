# Security Policy

## Supported Versions

oboobot is currently at v0.1.x. Security updates are applied only to the latest release.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

As the project matures and stable releases are tagged, this table will be expanded accordingly.

---

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

If you discover a vulnerability in oboobot, especially one involving broker credential exposure, the state-repository token, notification webhook exposure, or an order-submission or reconciliation bug that could misrepresent real account state, please report it privately.

### How to Report

Open a [GitHub Security Advisory](https://github.com/oboobotenefiok/oboobot/security/advisories/new) on this repository. This keeps the disclosure private until a fix is ready.

If you are unable to use GitHub's advisory system, send a plain-text email describing the issue. Include:

- A clear description of the vulnerability
- Steps to reproduce it
- The version of oboobot affected (check Cargo.toml)
- Your assessment of the potential impact
- Any suggested fix, if you have one

### What to Expect

| Timeline | What happens |
| -------- | ------------ |
| Within 48 hours | You receive acknowledgment that your report was received |
| Within 7 days | You receive an initial assessment: confirmed, needs more info, or not a vulnerability |
| Within 30 days | A fix is developed and a patched release is prepared (critical issues may be faster) |
| After the fix ships | You are credited in the CHANGELOG and release notes, unless you prefer to remain anonymous |

### If the Vulnerability Is Accepted

You will be kept in the loop throughout the fix process. A disclosure date will be coordinated with you before anything is made public. Credit will be given in the release notes.

### If the Vulnerability Is Declined

You will receive a clear explanation of why it was not considered a security issue. If you disagree with the assessment, you are welcome to discuss it further via the same private channel before going public.

---

## Scope

oboobot is a trading daemon: it connects to a broker (Deriv over WebSocket, or Bybit), submits and closes real orders, and manages real account exposure, running on a schedule via GitHub Actions. Its credentials and its correctness both matter for security purposes here: a credential leak is a security incident in the conventional sense, and a bug that causes it to misrepresent open positions, double-submit an order, or size a position incorrectly is also treated as one, since either can directly cost real money.

### In scope

- **Credential exposure**: vulnerabilities that cause `DERIV_API_TOKEN`, `DERIV_APP_ID`, `BYBIT_API_KEY`, `BYBIT_API_SECRET`, the Slack/Telegram webhook secrets, or `STATE_REPO_TOKEN` (the GitHub PAT with write access to the separate state repository) to be logged, transmitted unencrypted, or written to a world-readable file or a public commit.
- **Command or config injection**: unescaped configuration values passed anywhere they could result in arbitrary code execution or an unintended broker request.
- **Arbitrary file write**: path traversal via configuration values that causes oboobot to write outside its configured `--state-dir`.
- **Reconciliation or double-execution bugs**: any bug that could cause a `buy`/`sell` request to be sent twice for the same intended trade, or cause local state and broker state to diverge silently (a mismatch is supposed to be loudly logged and notified, never quietly resolved).
- **Insecure default configuration**: configuration defaults that expose credentials or create an exploitable condition without the operator having done anything non-standard.
- **Notification content exposure**: scenarios where a Slack/Telegram notification leaks a credential or other sensitive value.
- **Log or state-repo exposure**: world-readable log output, or a `positions.cursor`/`decisions.cursor`/status file committed to the state repo, containing a credential it shouldn't.

### Out of scope

- Bugs that only affect log formatting or CLI output cosmetics.
- Vulnerabilities in Deriv's or Bybit's own platforms; report those directly to Deriv or Bybit.
- Losses from normal trading risk (the strategy being wrong, the market moving against a position) are not security issues. A bug that causes oboobot to *misrepresent* that risk, or to act other than the strategy and risk configuration actually specify, is in scope under "reconciliation or double-execution bugs" above.
- Feature requests or general usability concerns; open a regular GitHub issue for those.

---

## What oboobot Stores and Transmits

Being explicit about the data model is part of the security posture.

oboobot transmits to:
- Deriv, over a WebSocket connection, for market data and order execution.
- Bybit, once `BybitAdapter` is implemented (currently a stub; see the README).
- Slack and/or Telegram, if configured, for cycle notifications (opened/closed positions, reconciliation mismatches, correlation regime shifts).
- The dedicated state repository (`oboobot_report` or whatever `--state-dir` is checked out from), via a git push, for every cursor and snapshot file described in the README's "State Files" section.

oboobot stores locally (under `--state-dir`, which in the deployed GitHub Actions case is a checkout of the separate state repo):
- Every file listed in the README's "State Files" section: position and decision history, buffer/correlation/spread state per configured pair-set, True Open levels, the status snapshot, and (for a `--replay-days` run only) an isolated `replay/` subdirectory.

oboobot does **not** store or transmit:
- Credentials themselves in any of the above files. Every secret (`DERIV_API_TOKEN`, `DERIV_APP_ID`, `BYBIT_API_KEY`, `BYBIT_API_SECRET`, webhook URLs, `STATE_REPO_TOKEN`) is read from an environment variable at process start and is never written to a cursor file, a snapshot file, or `config.toml` itself, on the same principle `config.toml`'s own header comment states: "None of this is secret; API keys and webhook URLs are read from environment variables named here, never written directly into this file, since this file is meant to live in an open-source repo."
- Shell history or commands of any kind.
- File contents from any directory outside `--state-dir`.

---

## Credential Security

Every credential oboobot uses is a GitHub Actions secret, injected as an environment variable at process start, never committed to either repository (the code repo or the state repo).

- `DERIV_APP_ID` / `DERIV_API_TOKEN`: read once at `DerivAdapter::connect_from_env`, used only for the Deriv WebSocket `authorize` call.
- `BYBIT_API_KEY` / `BYBIT_API_SECRET`: read once at `BybitAdapter::from_env`; currently unused beyond that, since the adapter itself is a stub.
- `STATE_REPO_TOKEN`: a GitHub personal access token scoped to write access on the state repository only, used solely by the `trading.yml` workflow to check out and push to that repo. It is never passed to the daemon binary itself.
- Slack/Telegram: a webhook URL and/or bot token and chat ID, read from whichever environment variable names `config.toml`'s `[notifications]` section points at.

If a credential is suspected compromised: revoke and rotate it at the source (Deriv's or Bybit's API settings, the Slack app's webhook, the Telegram bot's token, or GitHub's token settings for `STATE_REPO_TOKEN`), update the corresponding GitHub Actions secret, and report the incident via the reporting channels above.

---

## Transport Security

Deriv WebSocket traffic goes over `wss://` (TLS), via `tokio-tungstenite`. Certificate verification is not disabled anywhere in this codebase.

---

## Philosophy

This is a system that submits real orders against a real account on a schedule with no human in the loop for each individual cycle. That combination means correctness and credential security are treated as the same priority, not two separate concerns: a bug that silently double-submits an order is exactly as serious as a bug that leaks a credential, even though only one of those is a "security" issue in the traditional sense. Reconciliation, the collision check, the deliberate choice not to retry mutating broker calls, and the loud (never silent) handling of any local/broker mismatch all exist because of this.

If you find a way to break that model, please tell us privately. We will fix it, credit you, and be grateful.
