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
cache/profiler/profiler_report_<YYYYMMDD_HHMMSS_mmmZ>.zip
```

The archive contains:

```text
manifest.json
profiler_report_<YYYYMMDD_HHMMSS_mmmZ>.json
profiler_report_<YYYYMMDD_HHMMSS_mmmZ>.md
profiler_report_latest.json
profiler_report_latest.md
```

The timestamped JSON/Markdown report files are canonical archive members. The loose `latest` files remain for existing integrations and are duplicated inside the archive so one dated artifact contains the whole report payload.

The report includes active jobs, completed jobs, failed jobs, slow/over-budget jobs, category totals, load ratios and diagnostics.
