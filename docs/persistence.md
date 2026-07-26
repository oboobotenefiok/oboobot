# Persistence Layer

Two file-backed primitives, both under `persistence::`. Neither knows what it's storing beyond `T: Serialize + DeserializeOwned`: the meaning of a given file comes entirely from which type is stored in it and what the caller does with that type.

## `CursorFile<T>`

Append-only. Each `append(&value)` call writes one JSON line and fsyncs before returning, so a value that was reported as durably appended actually is, even across an abrupt process kill. `read_all()` reads and deserializes every line ever appended, in order, from the start of the file: it does not deduplicate by any notion of identity, and it does not forget an entry once whatever it represents is no longer true (a position that closed still has its last-known-open snapshot sitting in the file forever).

This matters for reconciliation: `daemon::main::reconcile_and_notify` deliberately does *not* pass `positions_cursor.read_all()` as the locally-known baseline for the reconciliation that runs immediately after a fill. Doing so would permanently misflag every position that has ever closed as still "locally known," since the cursor log never marks an old entry closed. The in-memory `open_positions` list: which accurately reflects everything believed to be open as of right now: is the correct baseline for that call. The *startup* reconciliation still reads the cursor file, which is the only source available before any in-memory state exists for a freshly-started process.

Used for: `positions.cursor`, `decisions.cursor`, `recommendations.cursor`.

## `SnapshotFile<T>`

Holds exactly one current value, overwritten on every `write()`. `read()` returns `None` if the file doesn't exist yet (a fresh pair-set, a fresh deployment) rather than erroring: every caller is expected to have a sensible default for that case (`unwrap_or_default()`, or a fresh capture).

Used for: `buffer_daily_<PAIR>.json`, `buffer_session_<PAIR>.json`, `correlation_<PRIMARY>_<SECONDARY>.json`, `spread_history_<PRIMARY>.json`, `true_open_weekly.json`, `true_open_daily.json`, `status.json`.

## Why two primitives instead of one

A cursor file is a history a human or the reconciliation logic might need to look back through; overwriting it would destroy exactly the information it exists to keep. A snapshot file is the *current* state of something that gets superseded every cycle; appending to it forever would just be an ever-growing file nobody reads sequentially. Picking the right one for a new piece of state is a design decision made once, at the call site that first creates the file: see the individual `SnapshotFile`/`CursorFile` construction sites in `daemon::main` for the reasoning behind each specific choice.

## State directory layout

See the "State Files" section of the top-level README for the current, complete list of files a running deployment produces.
