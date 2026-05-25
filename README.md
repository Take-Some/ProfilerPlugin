# North Star Engine Profiler Plugin

Runtime profiler as a plugin/provider, not an engine-owned profiler subsystem.

## Gateway / service

- Gateway: `engine.profiler`
- Provider service: `profiler.api`
- Capability: `profiler.backend`

## Capture model

The plugin uses existing host surfaces:

- `HostApiV1.register_service_v1` to expose the profiler service.
- `HostApiV1.subscribe_events_v1` to receive task/job events.
- Standard service calls to `engine.profiler` / `profiler.api` for explicit instrumentation.
- Host-emitted generic diagnostics job events for plugin lifecycle operations and service calls.

## Event topics

```text
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
profiler.flush_report_v1
shutdown_v1
```

## Report output

On shutdown, or when `profiler.flush_report_v1` is called, the plugin writes compatibility `latest` files and a dated ZIP archive:

```text
cache/profiler/profiler_report_latest.json
cache/profiler/profiler_report_latest.md
cache/profiler/profiler_jobs_latest.csv
cache/profiler/profiler_top_offenders_latest.csv
cache/profiler/profiler_categories_latest.csv
cache/profiler/profiler_sources_latest.csv
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

1. **Load chart — категории по суммарному времени** — high-level domain split.
2. **Load chart — top offenders** — grouped suspect chart by service/plugin/method/name.
3. **Top offenders by total elapsed time** — best table for answering who consumed the captured processor time.
4. **Top single jobs by elapsed time** — worst individual spikes.
5. **Top single jobs by budget load** — jobs that exceeded their expected budget the hardest.
6. **Active jobs** — jobs still running when the report was flushed.

Important metric rules:

```text
elapsed_ms = observed wall-clock time captured by profiler events
budget_ms  = expected budget for that job category or explicit event budget
load       = elapsed_ms / budget_ms
load >= 1  = over budget
```

Render/runtime subsystems may also emit sampled profiler events with explicit `elapsed_ms`; those samples are treated as first-class completed jobs instead of zero-duration event-bus noise.

The profiler does not claim OS-level sampled CPU cycles. It identifies CPU-time suspects inside instrumented engine/plugin work. If a subsystem is not instrumented with job begin/end/status events, it will not appear as a time consumer.

## CSV files

CSV files are intended for spreadsheet inspection and external charting:

| CSV | Purpose |
|---|---|
| `profiler_jobs_latest.csv` | all completed jobs with elapsed/budget/load columns |
| `profiler_top_offenders_latest.csv` | grouped suspects sorted by total captured time |
| `profiler_categories_latest.csv` | category totals and share-of-time |
| `profiler_sources_latest.csv` | source totals and share-of-time |
| `profiler_active_jobs_latest.csv` | jobs still running at flush time with current load |
| `profiler_timeline_latest.csv` | completed jobs with run-relative start/end offsets |
| `profiler_diagnostics_latest.csv` | warnings/errors emitted by profiler analysis |

Recommended spreadsheet charts:

- bar chart: `profiler_top_offenders_latest.csv` → `key` vs `total_elapsed_ms`;
- pie or stacked bar: `profiler_categories_latest.csv` → `category` vs `total_share_percent`;
- scatter plot: `profiler_jobs_latest.csv` → `elapsed_ms` vs `load`, grouped by `category` or `source`;
- timeline chart: `profiler_timeline_latest.csv` → `start_offset_ms`, `end_offset_ms`, `category`.

## Report schema highlights

The JSON report schema is `newengine.profiler.report.v2`. In addition to raw `active_jobs`, `completed_jobs` and `diagnostics`, it contains:

```text
analysis.worst_offender
analysis.top_offenders_by_total_elapsed
analysis.top_completed_jobs_by_elapsed
analysis.top_completed_jobs_by_load
analysis.by_category_ranked
analysis.by_source_ranked
analysis.by_owner_ranked
```

This is the same analysis data used by the Markdown report and CSV exports.
