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

If you discover a vulnerability in oboobot — especially one involving API key exposure, command injection, environment variable leakage, or rate limit bypass — please report it privately.

### How to Report

Open a [GitHub Security Advisory](https://github.com/oboobotenefiok/oboobot/security/advisories/new) on this repository. This keeps the disclosure private until a fix is ready.

If you are unable to use GitHub's advisory system, send a plain-text email describing the issue. Include:

- A clear description of the vulnerability
- Steps to reproduce it
- The version of oboobot affected (`cargo run -- --version` or check Cargo.toml)
- Your assessment of the potential impact
- Any suggested fix, if you have one

### What to Expect

| Timeline | What happens |
| -------- | ------------ |
| Within 48 hours | You receive acknowledgment that your report was received |
| Within 7 days | You receive an initial assessment — confirmed, needs more info, or not a vulnerability |
| Within 30 days | A fix is developed and a patched release is prepared (critical issues may be faster) |
| After the fix ships | You are credited in the CHANGELOG and release notes, unless you prefer to remain anonymous |

### If the Vulnerability Is Accepted

You will be kept in the loop throughout the fix process. A disclosure date will be coordinated with you before anything is made public. Credit will be given in the release notes.

### If the Vulnerability Is Declined

You will receive a clear explanation of why it was not considered a security issue. If you disagree with the assessment, you are welcome to discuss it further via the same private channel before going public.

---

## Scope

oboobot runs as a background process that periodically fetches cryptocurrency prices from CoinGecko and can send notifications via Termux. While it does not execute trades or handle sensitive financial data directly, it does handle API keys and environment variables that could be used to access paid services or reveal private information.

### In scope

- **API key exposure** — vulnerabilities that cause `COINGECKO_API_KEY` or any future configured API key to be logged, transmitted unencrypted, or written to a world-readable file
- **Command injection** — unescaped configuration values or environment variables passed to system commands (`curl`, `termux-notification`) that could result in arbitrary code execution
- **Arbitrary file write** — path traversal via configuration values that causes oboobot to write outside its designated data directories (`~/.local/share/oboobot`, `~/.config/oboobot` on Unix)
- **Environment variable leakage** — any path by which environment variables containing secrets are exposed in error messages, logs, or notifications
- **Rate limit bypass** — vulnerabilities that allow an attacker to exhaust CoinGecko API rate limits by manipulating request timing or parameters
- **Insecure default configuration** — configuration defaults that expose sensitive data or create an exploitable condition without the user having done anything non-standard
- **Notification content exposure** — scenarios where price data or error messages containing sensitive information are sent to unintended recipients via notifications
- **Log file exposure** — world-readable log files that contain API keys, price data, or other sensitive information

### Out of scope

- Bugs that only affect terminal output formatting or colour rendering
- Vulnerabilities in third-party tools that oboobot observes but does not control (`curl`, `termux-notification`, `rust-analyzer`, etc.)
- Issues in CoinGecko's own API — report those directly to CoinGecko
- Feature requests or general usability concerns — open a regular GitHub issue for those
- The fact that oboobot fetches price data by design — this is the core function of the tool, not a vulnerability
- Price volatility or trading losses — oboobot is a monitoring tool, not a trading execution engine

---

## What oboobot Stores and Transmits

Being explicit about the data model is part of the security posture.

oboobot does **not** transmit anything to external services except the HTTPS requests to CoinGecko.

oboobot stores locally:

- Environment variables from `.env` file (read at startup)
- API key for CoinGecko (if provided)
- Configuration settings in `~/.config/oboobot/config.toml`
- Cache files in `~/.local/share/oboobot/` (future feature)

oboobot does **not** store or transmit:

- Shell history or commands of any kind
- File contents from any directory
- Other environment variables beyond those explicitly loaded from `.env`
- Any data that could identify the user beyond the API key (which is only sent to CoinGecko)

The `.env` file is designed to be excluded from version control by default (see `.gitignore`). Users are responsible for ensuring that any API keys stored in the `.env` file are kept secure.

---

## API Key Security

oboobot supports two authentication modes for CoinGecko:

1. **Keyless mode**: Uses the public API with strict rate limits (10-30 calls per minute, shared by IP)
2. **Authenticated mode**: Uses a CoinGecko API key for higher rate limits

When using authenticated mode:

- The API key is read from the `COINGECKO_API_KEY` environment variable in `.env`
- The key is only transmitted as a query parameter: `&x_cg_demo_api_key=KEY`
- The key is never logged, stored in plaintext outside the `.env` file, or transmitted to any other endpoint
- Users should rotate keys regularly and use the principle of least privilege (demo keys only)

If a user suspects their API key has been compromised, they should:

1. Revoke the key immediately via CoinGecko dashboard
2. Generate a new key
3. Update the `.env` file with the new key
4. Report the incident via the reporting channels above

---

## Transport Security

oboobot uses the Rust standard library's TLS implementation via the `rustls-tls` feature in `reqwest`. This ensures:

- No dependency on system OpenSSL (avoiding certificate store surprises)
- Modern TLS 1.2 and 1.3 support
- Certificate verification enabled by default

All API requests to CoinGecko are made over HTTPS. The API endpoint is hardcoded to `https://api.coingecko.com/api/v3/simple/price` to prevent downgrade attacks.

---

## Philosophy

oboobot runs as a background process that fetches price data and can send notifications. While it does not handle sensitive financial data directly, it does handle API keys that could be used to access paid services. The principle of least privilege applies: the bot should only have access to what it needs (the public API or a demo key) and nothing more.

Security and correctness are treated as the same priority. A bug that causes the bot to log an API key in plaintext is not a minor bug. It is a security incident. The use of environment variables for configuration, hardcoded endpoints, and the `dotenvy` crate for `.env` loading are all deliberate choices made with this trust model in mind.

If you find a way to break that model, please tell us privately. We will fix it, credit you, and be grateful.
