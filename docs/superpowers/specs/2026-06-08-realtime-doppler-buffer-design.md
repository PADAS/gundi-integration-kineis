# Realtime re-emit of settled Doppler fixes (held-fix buffer)

**Date:** 2026-06-08
**Ticket:** ERCS-7275 (follow-up enhancement)
**Status:** Design approved, pending implementation plan

## Problem

The Doppler-revision fix (`collapse_doppler_revisions`) holds a fix until it is older
than `doppler_settle_hours`, then emits the final revision once. But the **realtime**
pull path uses an advancing checkpoint (`fetch_telemetry_realtime` returns
`(messages, new_checkpoint)`), so each message is delivered exactly once. A fix that is
still unsettled when realtime first sees it is dropped and never re-delivered by
realtime — it is only re-emitted later by the **daily** `backfill_telemetry` action.

Net effect: finalized positions reach EarthRanger on a ~daily cadence, not near-real-time.
We want finalized Doppler positions to appear promptly via the realtime path.

## Goal

In the realtime path, hold unsettled Doppler fixes in a small persistent buffer and
re-emit each fix on the first realtime run after it crosses the settle window — latency
≈ `doppler_settle_hours` + up to one realtime interval (~2 min) — instead of ~daily.

## Approach (held-fix buffer)

Keep the efficient checkpoint fetch. Add a per-integration buffer of held Doppler fixes,
persisted in the existing Redis `IntegrationStateManager` alongside the checkpoint. Each
realtime run merges newly fetched fixes into the buffer and emits any that have settled.

### New pure function (`app/actions/transformers.py`)

```
reconcile_doppler_buffer(buffer, observations, settle_window, now)
    -> (emit, new_buffer, stats)
```

- `buffer`: `dict[str, dict]` mapping `"{source}|{dopplerLocId}"` → the best-revision-so-far
  observation held from previous runs.
- `observations`: this run's transformed observations (all location types).
- `settle_window`: `timedelta`; `now`: timezone-aware UTC datetime (raise `ValueError` if naive
  and `settle_window > 0`, matching `collapse_doppler_revisions`).

Logic:
1. Non-Doppler observations (`location_type != "doppler"`) → `emit` immediately; never buffered.
2. Doppler observations:
   - With a `dopplerLocId`: upsert into a working copy of `buffer` keyed by `(source, dopplerLocId)`,
     keeping the higher `dopplerRevision` (tie-break: latest `dopplerAcqDatetime`, then last seen —
     reuse `_revision_sort_key`). A discarded lower revision increments `revisions_collapsed`.
   - Without a `dopplerLocId`: cannot be re-identified across runs, so not buffered — emit if already
     settled (`recorded_at ≤ now − settle_window`), otherwise drop (the daily backfill will catch it).
3. Scan the working buffer: every entry whose kept `recorded_at ≤ now − settle_window` → move to
   `emit` and evict; the remainder become `new_buffer`.
4. When `settle_window <= 0`: no holding — every Doppler fix (collapsed to its max revision within the
   batch) is emitted and the buffer is left empty.

`stats`: `{"buffered": len(new_buffer), "emitted_from_buffer": <int>, "revisions_collapsed": <int>}`.
Output ordering: non-Doppler first (arrival order), then emitted Doppler fixes; ER dedups on
`(source, recorded_at)` so cross-batch ordering is not significant.

### Handler wiring (realtime branch of `action_pull_telemetry` only)

- Load the state object once: `{kineis_realtime_checkpoint, kineis_doppler_buffer}`.
- After fetching realtime messages and transforming them, call `reconcile_doppler_buffer(buffer,
  observations, settle_window, now=_utc_now())`.
- Batch-send `emit` to Gundi.
- Persist **both** the new checkpoint and `new_buffer` in a single `set_state` call. (Current code
  overwrites the whole state object with just the checkpoint; fix it to carry both keys.)
- Buffer keys are JSON-safe strings (`"{source}|{dopplerLocId}"`); values are observation dicts.

The **bulk-fallback path and the daily `backfill_telemetry` action keep using
`collapse_doppler_revisions` unchanged** — backfill remains the safety net.

## Components / data flow (realtime)

```
get_state -> {checkpoint, buffer}
fetch_telemetry_realtime(checkpoint) -> (messages, new_checkpoint)
telemetry_batch_to_observations_detailed(messages) -> observations
reconcile_doppler_buffer(buffer, observations, settle_window, now)
    -> (emit, new_buffer, stats)
generate_batches(emit) -> send_observations_to_gundi
set_state({checkpoint: new_checkpoint, buffer: new_buffer})
```

## Edge cases & safety

- **Doppler without `dopplerLocId`**: not buffered; emitted only if already settled, else left to backfill.
- **State loss (Redis)**: buffer empties; in-flight held fixes are re-emitted by the daily backfill
  instead — no data loss, just slower for those fixes. Backfill is the insurance.
- **Late revision after a fix was already emitted**: same accepted residual risk as today (the higher
  revision becomes a second point); the settle window makes it rare.
- **Future-dated `recorded_at`**: would never settle and would linger in the buffer; not expected from
  CLS. Not specially handled (documented).
- **Config**: reuses `doppler_settle_hours`; no new config.

## Logging

Realtime summary `data` gains:
- `doppler_buffered` — fixes still held after this run.
- `doppler_emitted_from_buffer` — fixes released this run.
(`doppler_revisions_collapsed` continues to be reported.)

## Testing

Pure-function unit tests for `reconcile_doppler_buffer`:
- unsettled Doppler fix → buffered, not emitted (`buffered == 1`, `emit == []`);
- buffered fix with `now` advanced past settle → emitted and evicted (`emitted_from_buffer == 1`,
  empty buffer);
- higher revision arrives for a buffered key → buffer holds the higher revision, lower discarded
  (`revisions_collapsed == 1`);
- GPS / unlocated → emitted immediately, never buffered;
- Doppler already older than settle on arrival → emitted immediately, not buffered;
- missing `dopplerLocId` → settled emits, unsettled dropped (not buffered);
- `settle_window == 0` → emits collapsed max revision, empty buffer;
- naive `now` with settle active → `ValueError`.

Handler-level test (realtime): two runs with persisted state and advancing `now` — fix buffered on
run 1 (sent nothing), emitted on run 2; assert `set_state` persisted both checkpoint and buffer.

## Out of scope

- Changing the bulk-fallback or daily backfill behavior (they keep `collapse_doppler_revisions`).
- Retuning `doppler_settle_hours` or the backfill schedule.
