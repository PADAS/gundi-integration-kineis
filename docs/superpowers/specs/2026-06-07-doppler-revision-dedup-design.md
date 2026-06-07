# Doppler revision de-duplication (Option A: settle window + collapse)

**Date:** 2026-06-07
**Ticket:** ERCS-7275
**Status:** Design approved, pending implementation plan

## Problem

CLS assigns each Doppler location a `dopplerLocId` and **revises it** over time
(`dopplerRevision` 0 → 1 → 2 …) as more satellite-pass data arrives. Each
revision has a different computed time (`dopplerDatetime`), a progressively
corrected position, and is delivered to us in a later pull.

The Kineis integration maps each message to an observation with
`recorded_at = dopplerDatetime` and sends it to Gundi. Because every revision
has a different `dopplerDatetime` and a different position, the integration
emits **one observation per revision**. EarthRanger stores them all.

Confirmed on subject 45020 (Roofline Roxy), 2026-05-17:

| dopplerLocId | revisions stored | recorded_at | class / error | note |
| --- | --- | --- | --- | --- |
| 11 | rev0, rev2 | 01:28:53, 01:31:09 | B/269 m, 2/261 m | rev0 near Bluff, rev2 ~9 km away |
| 12 | rev1, rev2 | 01:53:40, 01:55:11 | B/363 m, 2/343 m | rev1 near Bluff, rev2 ~13 km offshore |

Result: duplicate, time-shifted, out-of-order points versus what CLS shows
(one best location per fix). This is the customer-reported "time out of sync"
and scrambled-track behaviour.

## Constraint that shapes the solution

The Gundi pipeline is **append-only**: it cannot update or replace an existing
EarthRanger observation. EarthRanger de-duplicates on `(source, recorded_at)`
with **first-write-wins** — re-sending the same `(source, recorded_at)` with a
different location is discarded, not overwritten.

Therefore we cannot send an early revision and correct it later. The only way to
get the best version of a fix into ER is to **emit each fix once, already
finalized.** (A future Gundi "observation update" capability — mirroring the
existing explicit Event update path, posting to ER's `/status` endpoint — would
remove this constraint; that is a separate, longer-term effort and is out of
scope here.)

## Approach (Option A)

Suppress not-yet-final Doppler revisions and collapse the rest, **before**
sending. Applied identically in both the `pull_telemetry` and
`backfill_telemetry` actions.

### Algorithm

A new pure function in `app/actions/transformers.py` runs **after** the existing
transform, over the produced observation list. It only touches observations with
`location_type == "doppler"`; GPS and unlocated observations pass through
unchanged (GPS fixes are final on arrival).

1. **Settle filter.** Drop any Doppler observation whose `recorded_at`
   (= `dopplerDatetime`) is newer than `now − settle_window`. The fix may still
   be revised by CLS, so we hold it. A held observation is not lost: a later run
   re-fetches it and emits it once it is past the window.
2. **Revision collapse.** Group the surviving Doppler observations by
   `(source, dopplerLocId)` and keep only the one with the highest
   `dopplerRevision`. Tie-break deterministically: highest `dopplerAcqDatetime`,
   then last seen. Observations missing a `dopplerLocId` each form their own
   singleton group (never collapsed).

### Why it survives across runs

Once a fix is past the settle window it is at its final revision, so its
`dopplerDatetime` / `recorded_at` is stable. Every subsequent run (realtime
every 2 min, daily backfill) re-emits the same `(source, recorded_at)`, which
ER's first-write-wins dedup drops. No duplicates, no cross-run state required.

### Configuration

Add `doppler_settle_hours` to both `PullTelemetryConfiguration` and
`BackfillTelemetryConfiguration`:

- Default: **6** hours (comfortable margin over the ~1.5 h revision latency
  observed in the data).
- Range: 0–48. `0` disables the settle filter but keeps in-batch collapse.
- Surfaced in the portal UI (range widget), consistent with existing fields.

### Handler wiring

In both `action_pull_telemetry` and `action_backfill_telemetry`, after building
`transform_result`, call the new function on `transform_result.observations`
with `settle_window = timedelta(hours=action_config.doppler_settle_hours)` and
`now = _utc_now()`. Send the returned (reduced) list to Gundi.

### Logging

Extend the summary `data` payload with:

- `doppler_held_unsettled` — count held by the settle filter.
- `doppler_revisions_collapsed` — count removed by collapse (superseded
  revisions).

## Components / data flow

```
fetch messages
  -> telemetry_batch_to_observations_detailed()      (unchanged)
  -> collapse_doppler_revisions(observations,         (NEW, pure)
        settle_window, now)
        -> kept observations + stats
  -> generate_batches -> send_observations_to_gundi   (unchanged)
```

The new function is pure (inputs: observations, settle_window, now; outputs:
filtered observations + stats dict) and independently unit-testable.

## Edge cases

- **Missing `dopplerLocId`** on a Doppler observation → its own singleton group;
  still subject to the settle filter.
- **`doppler_settle_hours == 0`** → no holding; collapse still applies within the
  batch.
- **GPS / unlocated** observations → untouched by both steps.
- **`now`** is injected (from `_utc_now()`) so tests are deterministic.

## Known residual risk (accepted for near term)

If CLS revises a fix **after** the settle window closes, the higher revision
arrives at a new `dopplerDatetime` and becomes a second point. The settle window
makes this rare. Full robustness (emit-once state, or true in-place updates) is
deferred to the Gundi observation-update roadmap.

## Testing

Unit tests for `collapse_doppler_revisions`:

- collapses multiple revisions of one `dopplerLocId` to the highest revision;
- holds Doppler observations newer than the settle window; emits them once past it;
- passes through GPS and unlocated observations untouched;
- handles Doppler observations with no `dopplerLocId`;
- regression: Roxy locId 11 (rev0+rev2 → rev2) and locId 12 (rev1+rev2 → rev2);
- reports correct `doppler_held_unsettled` / `doppler_revisions_collapsed` stats.

Handler-level: existing action tests extended to assert the collapse function is
applied and its stats appear in the summary log.

## Out of scope

- Cleaning up the duplicate Argos/Kineis subjects and sources (the older-sharks
  half of ERCS-7275) — operational data cleanup, tracked separately.
- Gundi observation-update capability — separate long-term effort.
