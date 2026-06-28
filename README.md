# North Star Engine Profiler Plugin

[![CI](https://img.shields.io/badge/CI-GITHUB%20ACTIONS-2D9CFF?style=flat-square&labelColor=5A5A5A)](https://github.com/Take-Some/ProfilerPlugin/actions/workflows/ci.yml)
![RUST](https://img.shields.io/badge/RUST-STABLE-F28C28?style=flat-square&labelColor=5A5A5A)
![ENGINE](https://img.shields.io/badge/ENGINE-NORTH%20STAR-2D9CFF?style=flat-square&labelColor=5A5A5A)
![PLUGIN](https://img.shields.io/badge/PLUGIN-PROFILER-103B4A?style=flat-square&labelColor=5A5A5A)
![PROVIDER](https://img.shields.io/badge/PROVIDER-STARPROFILER-0C3340?style=flat-square&labelColor=5A5A5A)
![STATUS](https://img.shields.io/badge/STATUS-ADOPTED-D4B000?style=flat-square&labelColor=5A5A5A)

## Origin / adoption

> [!NOTE]
> **Kalista Verner / Калиста**, developer of **Take Some()**, originally built this as her personal profiler plugin.
> It was not designed as a host-bundled profiler subsystem. It started as a focused plugin-provider experiment.
> The implementation proved strong enough — gateway-routed, jobs-aware, report-oriented and cleanly isolated — that North Star adopted it as a first-party profiler provider.

This history matters architecturally: `engine.profiler` stays host-owned, while `starProfiler-profiler` remains a replaceable provider implementation.

Runtime profiler as a plugin/provider, not an host-bundled profiler subsystem.

## Gateway / service

- Gateway: `engine.profiler`
- Provider service: `profiler.api`
- Capability: `profiler.backend`

## CI / quality gates

GitHub Actions builds this repository in the same layout used by the North Star plugin workspace:

```text
NorthStar/NewEngine
NorthStar/Plugins/ProfilerPlugin
```

The workflow checks:

```text
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked --profile release
```

If `Take-Some/NewEngine` is private in the target GitHub organization, configure `NORTHSTAR_CI_TOKEN` with read access to that repository before running CI.

## Capture model

The plugin uses existing host surfaces:

- `HostApiV1.register_service_v1` to expose the profiler service.
- `HostApiV1.subscribe_events_v1` to receive task/job events.
- `HostApiV1.call_service_v1(engine.jobs, job.invoke_service_v1, ...)` to execute heavy report build/write work on the engine.jobs provider path.
- Standard service calls to `engine.profiler` / `profiler.api` for explicit instrumentation.
- Host-emitted generic diagnostics job events for plugin lifecycle operations and service calls.

The profiler must never hide expensive work behind an untracked background thread. By default, report flushing is accepted only when it can be scheduled as a visible `engine.jobs` job; there is no profiler-owned background fallback.

## Event topics

```text
engine.task.event.v1
engine.jobs.event.v1
newengine.diagnostics.job.begin.v1
newengine.diagnostics.job.end.v1
newengine.diagnostics.job.status.v1
newengine.diagnostics.profiler.sample.v1
```

## Service methods

```text
info_json
profiler.job_begin_json_v1
profiler.job_end_json_v1
profiler.job_status_json_v1
profiler.snapshot_json_v1
profiler.diagnostics_json_v1
profiler.flush_status_json_v1
profiler.flush_report_v1
profiler.flush_report_async_v1
profiler.flush_report_sync_v1
shutdown_v1
```

## Scheduling / jobs policy

Default scheduling configuration:

```json
{
  "scheduling": {
    "prefer_engine_jobs": true,
    "require_engine_jobs": true,
    "flush_job_budget_ms": 250.0,
    "service_flush_mode": "engine_jobs",
    "shutdown_flush_mode": "sync_final"
  }
}
```

Method behavior:

- `profiler.flush_report_v1` uses `scheduling.service_flush_mode`; by default it calls `engine.jobs/job.invoke_service_v1`, which schedules `profiler.flush_report_sync_v1` on an engine.jobs worker.
- `profiler.flush_report_async_v1` explicitly schedules a visible profiler flush job through `job.invoke_service_v1`; it does not create plugin-owned background threads.
- `profiler.flush_report_sync_v1` is the synchronous worker entrypoint used by `engine.jobs` or by the final shutdown flush.
- `profiler.flush_status_json_v1` reports scheduled/in-progress/failed flushes and recent scheduling diagnostics.

Heavy report serialization, CSV generation, Markdown generation, file writes and ZIP creation run inside the engine job worker after a short profiler state snapshot. The runtime state lock is held only while copying the profiler state and while committing the final result paths/counters.

## Report output

On shutdown, or when a flush job reaches `profiler.flush_report_sync_v1`, the plugin writes compatibility `latest` files and a dated ZIP archive:

```text
cache/profiler/profiler_report_latest.json
cache/profiler/profiler_report_latest.md
cache/profiler/profiler_jobs_latest.csv
cache/profiler/profiler_top_offenders_latest.csv
cache/profiler/profiler_categories_latest.csv
cache/profiler/profiler_sources_latest.csv
cache/profiler/profiler_methods_latest.csv
cache/profiler/profiler_budget_violations_latest.csv
cache/profiler/profiler_lanes_latest.csv
cache/profiler/profiler_first_latest.csv
cache/profiler/profiler_frame_budget_latest.csv
cache/profiler/profiler_active_jobs_latest.csv
cache/profiler/profiler_timeline_latest.csv
cache/profiler/profiler_diagnostics_latest.csv
cache/profiler/profiler_report_<YYYYMMDD_HHMMSS_mmmZ>.zip
```

The archive contains:

```text
manifest.json
profiler_report_<YYYYMMDD_HHMMSS_mmmZ>.json
profiler_report_<YYYYMMDD_HHMMSS_mmmZ>.md
profiler_jobs_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_top_offenders_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_categories_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_sources_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_methods_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_budget_violations_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_lanes_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_first_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_frame_budget_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_active_jobs_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_timeline_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_diagnostics_<YYYYMMDD_HHMMSS_mmmZ>.csv
profiler_report_latest.json
profiler_report_latest.md
profiler_*_latest.csv
```

The timestamped JSON/Markdown/CSV report files are canonical archive members. The loose `latest` files remain for existing integrations and are duplicated inside the archive so one dated artifact contains the whole report payload.

## How to read the Markdown report

Start at **Quick answer — кто жрёт время**. It points to the grouped offender with the largest `total_elapsed_ms` share. Then read:

1. **Profiler-first telemetry view** — answers the production questions: what was scheduled, blocked, polling, waiting on GPU, over frame budget, or still async.
2. **Load chart — категории по суммарному времени** — high-level domain split.
3. **Load chart — lanes** — `engine.jobs` lane split, including `Simulation`, `RenderPrep`, `Streaming`, `AssetIo`, `Plugin`, `Background` or provider-defined lanes.
4. **Load chart — top offenders** — grouped suspect chart by service/plugin/method/name.
5. **Top offenders by total elapsed time** — best table for answering who consumed the captured processor time.
6. **Top methods by total elapsed time** — which service method accumulated the most captured time.
7. **Budget violations — что пробило кадр/лимит** — jobs that exceeded expected frame/service budgets.
8. **Frame budget violations — explicit frame envelope misses** — jobs that crossed explicit `frame_budget_ms`, grouped with frame/lane/wait/GPU/async fields.
9. **Top single jobs by elapsed time** — worst individual spikes.
10. **Top single jobs by budget load** — jobs that exceeded their expected budget the hardest.
11. **Active jobs** — jobs still running when the report was flushed.

Important metric rules:

```text
elapsed_ms = observed wall-clock time captured by profiler events
budget_ms  = expected budget for that job category or explicit event budget
load       = elapsed_ms / budget_ms
load >= 1  = over budget
```

Render/runtime subsystems may also emit sampled profiler events with explicit `elapsed_ms`; those samples are treated as first-class completed jobs instead of zero-duration event-bus noise.

For profiler-first reports, emitters should attach these optional fields whenever they know them:

```text
lane
priority
dependency_group
frame_id
frame_budget_ms
gpu_wait_ms
wait_reason
async_mode
```

These fields are intentionally domain-neutral. They let the profiler correlate `engine.jobs`, render frame envelopes, asset decode, texture upload, shader compile, streaming residency and world chunk budgets without importing renderer/assets/streaming internals.

The profiler does not claim OS-level sampled CPU cycles. It identifies CPU-time suspects inside instrumented engine/plugin work. If a subsystem is not instrumented with job begin/end/status events, it will not appear as a time consumer.

## CSV files

CSV files are intended for spreadsheet inspection and external charting:

| CSV | Purpose |
|---|---|
| `profiler_jobs_latest.csv` | all completed jobs with elapsed/budget/load columns |
| `profiler_top_offenders_latest.csv` | grouped suspects sorted by total captured time |
| `profiler_categories_latest.csv` | category totals and share-of-time |
| `profiler_sources_latest.csv` | source totals and share-of-time |
| `profiler_methods_latest.csv` | method-level grouped suspects sorted by captured time |
| `profiler_budget_violations_latest.csv` | jobs where `load >= 1.0` or elapsed time exceeds the slow threshold |
| `profiler_lanes_latest.csv` | lane totals and share-of-time |
| `profiler_first_latest.csv` | scheduled/blocked/polling/GPU-wait/frame-budget/async counters |
| `profiler_frame_budget_latest.csv` | explicit frame-budget misses with frame/lane/wait fields |
| `profiler_active_jobs_latest.csv` | jobs still running at flush time with current load |
| `profiler_timeline_latest.csv` | completed jobs with run-relative start/end offsets |
| `profiler_diagnostics_latest.csv` | warnings/errors emitted by profiler analysis |

Recommended spreadsheet charts:

- bar chart: `profiler_top_offenders_latest.csv` → `key` vs `total_elapsed_ms`;
- pie or stacked bar: `profiler_categories_latest.csv` → `category` vs `total_share_percent`;
- scatter plot: `profiler_jobs_latest.csv` → `elapsed_ms` vs `load`, grouped by `category` or `source`;
- bar chart: `profiler_methods_latest.csv` → `key` vs `total_elapsed_ms`;
- table/filter: `profiler_budget_violations_latest.csv` → sort by `load` descending;
- bar chart: `profiler_lanes_latest.csv` → `key` vs `total_elapsed_ms`;
- status dashboard: `profiler_first_latest.csv` → scheduled/blocked/polling/GPU/frame-budget/async counters;
- table/filter: `profiler_frame_budget_latest.csv` → sort by `over_frame_budget_ms` descending;
- timeline chart: `profiler_timeline_latest.csv` → `start_offset_ms`, `end_offset_ms`, `category`.

## Report schema highlights

The JSON report schema is `newengine.profiler.report.v3`. In addition to raw `active_jobs`, `completed_jobs` and `diagnostics`, it contains:

```text
analysis.worst_offender
analysis.top_offenders_by_total_elapsed
analysis.top_completed_jobs_by_elapsed
analysis.top_completed_jobs_by_load
analysis.by_category_ranked
analysis.by_source_ranked
analysis.by_owner_ranked
analysis.by_method_ranked
analysis.by_lane_ranked
analysis.profiler_first
analysis.frame_budget_violations
analysis.budget_violations
summary.elapsed_percentiles_ms
summary.load_percentiles
summary.profiler_first
scheduler
flush_requests
```

This is the same analysis data used by the Markdown report and CSV exports.
