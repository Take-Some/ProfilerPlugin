use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Instant;

use crate::archive::{write_stored_zip, ZipFileEntry};
use crate::constants::{ENGINE_PROFILER_GATEWAY_ID, PROFILER_PLUGIN_ID, PROFILER_PLUGIN_NAME, PROFILER_SERVICE_ID};
use crate::records::{CategoryStats, JobRecord, ProfilerDiagnostic, ProfilerState, ReportPaths};
use crate::runtime::ProfilerRuntime;
use crate::util::{duration_ms, escape_md, format_json_scalar, path_to_string, unix_ms, utc_stamp_from_unix_ms, write_file};

const MD_TOP_LIMIT: usize = 16;
const JSON_TOP_LIMIT: usize = 64;
const CSV_LIMIT: usize = 100_000;

#[derive(Debug, Default, Clone, Serialize)]
struct AggregateStats {
    key: String,
    category: String,
    source: String,
    sample_name: String,
    count: u64,
    failed: u64,
    slow: u64,
    total_elapsed_ms: f64,
    average_elapsed_ms: f64,
    max_elapsed_ms: f64,
    max_load: f64,
    total_share_percent: f64,
    total_payload_bytes: u64,
    total_output_bytes: u64,
}

struct CsvArtifact {
    kind: &'static str,
    latest_name: String,
    timestamped_name: String,
    bytes: Vec<u8>,
}

impl ProfilerRuntime {
    pub(crate) fn flush_report(&self, reason: &str) -> Result<Value, String> {
        let shutdown_report = is_shutdown_report_reason(reason);
        let flush_started = Instant::now();

        let snapshot = {
            let mut state = self.lock_state();
            if shutdown_report && state.shutdown_report_written {
                return Ok(json!({
                    "schema": "newengine.profiler.flush_report.result.v1",
                    "reason": reason,
                    "paths": state.last_report_paths.clone(),
                    "json_bytes": 0,
                    "markdown_bytes": 0,
                    "csv_bytes": 0,
                    "skipped_duplicate_shutdown_report": true,
                    "lock_policy": "snapshot_only",
                }));
            }
            state.reports_in_progress = state.reports_in_progress.saturating_add(1);
            state.clone()
        };

        let report = self.build_report_from_state(&snapshot, reason);
        let markdown = self.build_markdown_report(&report);
        let json_len = serde_json::to_vec(&report).map(|v| v.len()).unwrap_or(0);
        let markdown_len = markdown.len();
        let write_result = self.write_report_files(&report, &markdown);
        let flush_elapsed_ms = duration_ms(flush_started.elapsed());

        match write_result {
            Ok((paths, csv_bytes)) => {
                let mut state = self.lock_state();
                state.reports_in_progress = state.reports_in_progress.saturating_sub(1);
                state.reports_written = state.reports_written.saturating_add(1);
                if shutdown_report {
                    state.shutdown_report_written = true;
                }
                state.last_report_paths = Some(paths.clone());
                Self::push_diag_locked(
                    &self.cfg,
                    &mut state,
                    "info",
                    "profiler_report_flushed",
                    format!(
                        "profiler report flushed reason='{}' elapsed_ms={:.3} policy='snapshot_then_write_outside_lock'",
                        reason, flush_elapsed_ms,
                    ),
                    None,
                );
                Ok(json!({
                    "schema": "newengine.profiler.flush_report.result.v1",
                    "reason": reason,
                    "paths": paths,
                    "json_bytes": json_len,
                    "markdown_bytes": markdown_len,
                    "csv_bytes": csv_bytes,
                    "flush_elapsed_ms": flush_elapsed_ms,
                    "skipped_duplicate_shutdown_report": false,
                    "lock_policy": "snapshot_then_build_and_write_outside_lock",
                }))
            }
            Err(e) => {
                let mut state = self.lock_state();
                state.reports_in_progress = state.reports_in_progress.saturating_sub(1);
                state.reports_failed = state.reports_failed.saturating_add(1);
                Self::push_diag_locked(
                    &self.cfg,
                    &mut state,
                    "error",
                    "profiler_report_flush_failed",
                    format!("profiler report flush failed reason='{}' elapsed_ms={:.3}: {}", reason, flush_elapsed_ms, e),
                    None,
                );
                Err(e)
            }
        }
    }

    fn build_report_from_state(&self, state: &ProfilerState, reason: &str) -> Value {
        let mut by_category: BTreeMap<String, CategoryStats> = BTreeMap::new();
        let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
        let mut by_source: BTreeMap<String, AggregateStats> = BTreeMap::new();
        let mut by_owner: BTreeMap<String, AggregateStats> = BTreeMap::new();
        let mut by_offender: BTreeMap<String, AggregateStats> = BTreeMap::new();
        let mut by_method: BTreeMap<String, AggregateStats> = BTreeMap::new();
        let mut elapsed_values = Vec::new();
        let mut load_values = Vec::new();
        let mut failed = 0u64;
        let mut slow = 0u64;
        let mut over_budget = 0u64;
        let mut total_elapsed_ms = 0.0f64;
        let mut max_elapsed_ms = 0.0f64;
        let mut total_payload_bytes = 0u64;
        let mut total_output_bytes = 0u64;

        for job in &state.completed {
            let elapsed = job.elapsed_ms.unwrap_or_default();
            let load = job.load.unwrap_or_default();
            let is_failed = job.status == "failed";
            let is_slow = elapsed >= self.cfg.diagnostics.slow_job_warn_ms || load >= 1.0;
            let is_over_budget = load >= 1.0;

            total_elapsed_ms += elapsed;
            max_elapsed_ms = max_elapsed_ms.max(elapsed);
            elapsed_values.push(elapsed);
            load_values.push(load);
            total_payload_bytes = total_payload_bytes.saturating_add(job.payload_bytes.unwrap_or_default());
            total_output_bytes = total_output_bytes.saturating_add(job.output_bytes.unwrap_or_default());
            *by_status.entry(job.status.clone()).or_insert(0) += 1;

            let cat = by_category.entry(job.category.clone()).or_default();
            cat.count = cat.count.saturating_add(1);
            cat.total_elapsed_ms += elapsed;
            cat.max_elapsed_ms = cat.max_elapsed_ms.max(elapsed);
            if is_failed {
                failed = failed.saturating_add(1);
                cat.failed = cat.failed.saturating_add(1);
            }
            if is_slow {
                slow = slow.saturating_add(1);
                cat.slow = cat.slow.saturating_add(1);
            }
            if is_over_budget {
                over_budget = over_budget.saturating_add(1);
            }

            accumulate(
                by_source.entry(job.source.clone()).or_insert_with(|| AggregateStats {
                    key: job.source.clone(),
                    category: "*".to_owned(),
                    source: job.source.clone(),
                    sample_name: job.name.clone(),
                    ..AggregateStats::default()
                }),
                job,
                is_failed,
                is_slow,
            );

            let owner = job_owner_key(job);
            accumulate(
                by_owner.entry(owner.clone()).or_insert_with(|| AggregateStats {
                    key: owner,
                    category: job.category.clone(),
                    source: job.source.clone(),
                    sample_name: job.name.clone(),
                    ..AggregateStats::default()
                }),
                job,
                is_failed,
                is_slow,
            );

            let offender = job_offender_key(job);
            accumulate(
                by_offender.entry(offender.clone()).or_insert_with(|| AggregateStats {
                    key: offender,
                    category: job.category.clone(),
                    source: job.source.clone(),
                    sample_name: job.name.clone(),
                    ..AggregateStats::default()
                }),
                job,
                is_failed,
                is_slow,
            );

            let method = job_method_key(job);
            accumulate(
                by_method.entry(method.clone()).or_insert_with(|| AggregateStats {
                    key: method,
                    category: job.category.clone(),
                    source: job.source.clone(),
                    sample_name: job.name.clone(),
                    ..AggregateStats::default()
                }),
                job,
                is_failed,
                is_slow,
            );
        }

        let active_jobs: Vec<Value> = state
            .active
            .values()
            .map(|job| {
                let active_elapsed_ms = duration_ms(job.started_at.elapsed());
                let current_load = active_elapsed_ms / job.record.budget_ms.max(0.001);
                let mut value = serde_json::to_value(&job.record).unwrap_or(Value::Null);
                if let Value::Object(obj) = &mut value {
                    obj.insert("active_elapsed_ms".to_owned(), json!(active_elapsed_ms));
                    obj.insert("current_load".to_owned(), json!(current_load));
                    obj.insert("current_over_budget".to_owned(), json!(current_load >= 1.0));
                }
                value
            })
            .collect();
        let completed_jobs: Vec<&JobRecord> = state.completed.iter().collect();
        let diagnostics: Vec<&ProfilerDiagnostic> = state.diagnostics.iter().collect();
        let completed_count = state.completed.len() as f64;

        let mut category_ranked = Vec::new();
        for (category, st) in &by_category {
            let avg = if st.count > 0 { st.total_elapsed_ms / st.count as f64 } else { 0.0 };
            category_ranked.push(json!({
                "category": category,
                "count": st.count,
                "failed": st.failed,
                "slow": st.slow,
                "total_elapsed_ms": st.total_elapsed_ms,
                "average_elapsed_ms": avg,
                "max_elapsed_ms": st.max_elapsed_ms,
                "total_share_percent": percent_of(st.total_elapsed_ms, total_elapsed_ms),
            }));
        }
        sort_objects_desc(&mut category_ranked, "total_elapsed_ms");

        finalize_aggregates(&mut by_source, total_elapsed_ms);
        finalize_aggregates(&mut by_owner, total_elapsed_ms);
        finalize_aggregates(&mut by_offender, total_elapsed_ms);
        finalize_aggregates(&mut by_method, total_elapsed_ms);

        let source_ranked = ranked_aggregates(by_source, JSON_TOP_LIMIT);
        let owner_ranked = ranked_aggregates(by_owner, JSON_TOP_LIMIT);
        let offender_ranked = ranked_aggregates(by_offender, JSON_TOP_LIMIT);
        let method_ranked = ranked_aggregates(by_method, JSON_TOP_LIMIT);
        let top_elapsed_jobs = ranked_jobs_by(&state.completed, "elapsed", JSON_TOP_LIMIT);
        let top_load_jobs = ranked_jobs_by(&state.completed, "load", JSON_TOP_LIMIT);
        let budget_violations = ranked_budget_violations(&state.completed, self.cfg.diagnostics.slow_job_warn_ms, JSON_TOP_LIMIT);
        let elapsed_percentiles = percentiles_json(elapsed_values);
        let load_percentiles = percentiles_json(load_values);

        json!({
            "schema": "newengine.profiler.report.v2",
            "reason": reason,
            "generated_unix_ms": unix_ms(),
            "plugin": {
                "id": PROFILER_PLUGIN_ID,
                "name": PROFILER_PLUGIN_NAME,
                "version": env!("CARGO_PKG_VERSION"),
                "service_id": PROFILER_SERVICE_ID,
                "gateway": ENGINE_PROFILER_GATEWAY_ID
            },
            "run": {
                "started_unix_ms": state.run_started_unix_ms,
                "uptime_ms": duration_ms(state.run_started.elapsed()),
                "events_seen": state.events_seen,
                "malformed_events": state.malformed_events,
            },
            "summary": {
                "active_jobs": state.active.len(),
                "completed_jobs_kept": state.completed.len(),
                "failed_jobs": failed,
                "slow_or_over_budget_jobs": slow,
                "over_budget_jobs": over_budget,
                "total_elapsed_ms": total_elapsed_ms,
                "average_elapsed_ms": if completed_count > 0.0 { total_elapsed_ms / completed_count } else { 0.0 },
                "max_elapsed_ms": max_elapsed_ms,
                "total_payload_bytes": total_payload_bytes,
                "total_output_bytes": total_output_bytes,
                "elapsed_percentiles_ms": elapsed_percentiles,
                "load_percentiles": load_percentiles,
                "reports_written": state.reports_written,
                "reports_in_progress": state.reports_in_progress,
                "reports_scheduled": state.reports_scheduled,
                "reports_failed": state.reports_failed,
                "by_status": by_status,
                "by_category": by_category,
            },
            "analysis": {
                "human_reading_order": [
                    "worst_offender",
                    "top_offenders_by_total_elapsed",
                    "top_completed_jobs_by_elapsed",
                    "top_completed_jobs_by_load",
                    "by_category_ranked",
                    "by_source_ranked",
                    "by_method_ranked",
                    "budget_violations",
                    "active_jobs"
                ],
                "interpretation": "elapsed_ms is observed wall-clock time captured by profiler events; load = elapsed_ms / budget_ms. It identifies CPU-time suspects inside instrumented engine/plugin work, not OS-level sampled CPU cycles.",
                "worst_offender": offender_ranked.first().cloned().unwrap_or(Value::Null),
                "by_category_ranked": category_ranked,
                "by_source_ranked": source_ranked,
                "by_owner_ranked": owner_ranked,
                "by_method_ranked": method_ranked,
                "top_offenders_by_total_elapsed": offender_ranked,
                "top_completed_jobs_by_elapsed": top_elapsed_jobs,
                "top_completed_jobs_by_load": top_load_jobs,
                "budget_violations": budget_violations,
            },
            "active_jobs": active_jobs,
            "completed_jobs": completed_jobs,
            "diagnostics": diagnostics,
            "flush_requests": state.flush_requests.iter().collect::<Vec<_>>(),
            "scheduler": {
                "service_flush_mode": self.cfg.scheduling.service_flush_mode.clone(),
                "shutdown_flush_mode": self.cfg.scheduling.shutdown_flush_mode.clone(),
                "prefer_engine_jobs": self.cfg.scheduling.prefer_engine_jobs,
                "require_engine_jobs": self.cfg.scheduling.require_engine_jobs,
                "lock_policy": "snapshot_then_build_and_write_outside_lock",
                "hidden_load_policy": "engine.jobs required for async flush; profiler-owned background fallback is not allowed"
            },
            "config": self.cfg.clone(),
        })
    }

    fn build_markdown_report(&self, report: &Value) -> String {
        let mut out = String::new();
        let summary = report.get("summary").unwrap_or(&Value::Null);
        let run = report.get("run").unwrap_or(&Value::Null);
        let analysis = report.get("analysis").unwrap_or(&Value::Null);

        let total_ms = summary.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0);
        let completed_count = summary.get("completed_jobs_kept").and_then(Value::as_u64).unwrap_or(0);
        let slow_count = summary.get("slow_or_over_budget_jobs").and_then(Value::as_u64).unwrap_or(0);
        let failed_count = summary.get("failed_jobs").and_then(Value::as_u64).unwrap_or(0);
        let active_count = summary.get("active_jobs").and_then(Value::as_u64).unwrap_or(0);

        let _ = writeln!(out, "# North Star Engine Profiler Report");
        let _ = writeln!(out);
        let _ = writeln!(out, "> [!INFO] INFO BLOCK — как читать отчёт");
        let _ = writeln!(out, "> **У нас сейчас:** отчёт показывает instrumented wall-clock time по job/service/plugin событиям. Главная строка для поиска виновника — `total_elapsed_ms` и `total_share_percent`; главная строка для бюджетов кадра — `load`, где `1.0` значит ровно бюджет, а `>1.0` значит перерасход.");
        let _ = writeln!(out, ">");
        let _ = writeln!(out, "> **Technical details (EN):** `load = elapsed_ms / budget_ms`; CSV files are emitted next to JSON/MD and duplicated in the timestamped ZIP archive when archive output is enabled.");
        let _ = writeln!(out);
        let _ = writeln!(out, "- reason: `{}`", report.get("reason").and_then(Value::as_str).unwrap_or("unknown"));
        let _ = writeln!(out, "- uptime_ms: `{:.3}`", run.get("uptime_ms").and_then(Value::as_f64).unwrap_or(0.0));
        let _ = writeln!(out, "- events_seen: `{}`", run.get("events_seen").and_then(Value::as_u64).unwrap_or(0));
        let _ = writeln!(out, "- malformed_events: `{}`", run.get("malformed_events").and_then(Value::as_u64).unwrap_or(0));
        let _ = writeln!(out);

        let _ = writeln!(out, "## Quick answer — кто жрёт время");
        let _ = writeln!(out);
        if let Some(worst) = analysis.get("worst_offender").filter(|v| !v.is_null()) {
            let key = worst.get("key").and_then(Value::as_str).unwrap_or("<unknown>");
            let share = worst.get("total_share_percent").and_then(Value::as_f64).unwrap_or(0.0);
            let count = worst.get("count").and_then(Value::as_u64).unwrap_or(0);
            let max_load = worst.get("max_load").and_then(Value::as_f64).unwrap_or(0.0);
            let failed = worst.get("failed").and_then(Value::as_u64).unwrap_or(0);
            let slow = worst.get("slow").and_then(Value::as_u64).unwrap_or(0);
            let _ = writeln!(out, "**Worst offender:** `{}` — {:.3} ms total, {:.1}% of captured time, {} calls, max load {:.2}x, slow/over-budget {}, failed {}.",
                escape_md(key),
                worst.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                share,
                count,
                max_load,
                slow,
                failed,
            );
            let _ = writeln!(out);
            let _ = writeln!(out, "```text");
            let _ = writeln!(out, "captured time share  [{}] {:>5.1}%", bar(share, 100.0, 32), share);
            let _ = writeln!(out, "max budget load      [{}] {:>5.2}x", bar(max_load.min(4.0), 4.0, 32), max_load);
            let _ = writeln!(out, "```");
        } else {
            let _ = writeln!(out, "No completed jobs were captured yet.");
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Executive summary");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Metric | Value | Meaning |");
        let _ = writeln!(out, "|---|---:|---|");
        let rows = [
            ("active_jobs", active_count.to_string(), "работа ещё не завершилась; если висит долго — смотреть `Active jobs`".to_owned()),
            ("completed_jobs_kept", completed_count.to_string(), "сколько завершённых записей осталось в ring buffer".to_owned()),
            ("failed_jobs", failed_count.to_string(), "ошибки, которые надо читать вместе с diagnostics".to_owned()),
            ("slow_or_over_budget_jobs", slow_count.to_string(), "slow threshold или `load >= 1.0`".to_owned()),
            ("total_elapsed_ms", format!("{total_ms:.3}"), "сумма captured wall-clock времени по завершённым jobs".to_owned()),
            ("average_elapsed_ms", format!("{:.3}", summary.get("average_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0)), "среднее время одной завершённой job".to_owned()),
            ("max_elapsed_ms", format!("{:.3}", summary.get("max_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0)), "самая дорогая одиночная job".to_owned()),
        ];
        for (metric, value, meaning) in rows {
            let _ = writeln!(out, "| `{metric}` | `{}` | {} |", value, meaning);
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Flush and scheduling policy");
        let _ = writeln!(out);
        let scheduler = report.get("scheduler").unwrap_or(&Value::Null);
        let _ = writeln!(out, "| Setting | Value |");
        let _ = writeln!(out, "|---|---|");
        let _ = writeln!(out, "| `service_flush_mode` | `{}` |", scheduler.get("service_flush_mode").and_then(Value::as_str).unwrap_or("unknown"));
        let _ = writeln!(out, "| `shutdown_flush_mode` | `{}` |", scheduler.get("shutdown_flush_mode").and_then(Value::as_str).unwrap_or("unknown"));
        let _ = writeln!(out, "| `prefer_engine_jobs` | `{}` |", scheduler.get("prefer_engine_jobs").and_then(Value::as_bool).unwrap_or(false));
        let _ = writeln!(out, "| `require_engine_jobs` | `{}` |", scheduler.get("require_engine_jobs").and_then(Value::as_bool).unwrap_or(false));
        let _ = writeln!(out, "| `lock_policy` | `{}` |", scheduler.get("lock_policy").and_then(Value::as_str).unwrap_or("snapshot_then_build_and_write_outside_lock"));
        let _ = writeln!(out);
        let _ = writeln!(out, "> [!NOTE] REQUEST NOTE — profiler safety");
        let _ = writeln!(out, "> **У нас сейчас:** heavy report build/write is outside the runtime state lock; async flush is routed through `engine.jobs` by default.");
        let _ = writeln!(out, "> **Было бы здорово:** keep every future heavy profiler export as a visible job/task, never as an invisible background load.");
        let _ = writeln!(out, "> **Technical details (EN):** `profiler.flush_report_v1` uses configured service flush mode; `profiler.flush_report_sync_v1` is the explicit synchronous worker entrypoint for `engine.jobs` and shutdown-final flush.");
        let _ = writeln!(out);

        let elapsed_p = summary.get("elapsed_percentiles_ms").unwrap_or(&Value::Null);
        let load_p = summary.get("load_percentiles").unwrap_or(&Value::Null);
        let _ = writeln!(out, "## Percentiles — latency and budget load");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Metric | p50 | p90 | p95 | p99 |");
        let _ = writeln!(out, "|---|---:|---:|---:|---:|");
        let _ = writeln!(out, "| `elapsed_ms` | {:.3} | {:.3} | {:.3} | {:.3} |",
            elapsed_p.get("p50").and_then(Value::as_f64).unwrap_or(0.0),
            elapsed_p.get("p90").and_then(Value::as_f64).unwrap_or(0.0),
            elapsed_p.get("p95").and_then(Value::as_f64).unwrap_or(0.0),
            elapsed_p.get("p99").and_then(Value::as_f64).unwrap_or(0.0),
        );
        let _ = writeln!(out, "| `load` | {:.2}x | {:.2}x | {:.2}x | {:.2}x |",
            load_p.get("p50").and_then(Value::as_f64).unwrap_or(0.0),
            load_p.get("p90").and_then(Value::as_f64).unwrap_or(0.0),
            load_p.get("p95").and_then(Value::as_f64).unwrap_or(0.0),
            load_p.get("p99").and_then(Value::as_f64).unwrap_or(0.0),
        );
        let _ = writeln!(out);

        write_ranked_chart(
            &mut out,
            "## Load chart — категории по суммарному времени",
            analysis.get("by_category_ranked").and_then(Value::as_array),
            "category",
            total_ms,
        );
        write_ranked_chart(
            &mut out,
            "## Load chart — top offenders",
            analysis.get("top_offenders_by_total_elapsed").and_then(Value::as_array),
            "key",
            total_ms,
        );

        let _ = writeln!(out, "## Top offenders by total elapsed time");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Rank | Offender | Source | Category | Calls | Total ms | Share | Avg ms | Max ms | Max load | Slow | Failed |");
        let _ = writeln!(out, "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        if let Some(items) = analysis.get("top_offenders_by_total_elapsed").and_then(Value::as_array) {
            for (idx, item) in items.iter().take(MD_TOP_LIMIT).enumerate() {
                let _ = writeln!(out,
                    "| {} | `{}` | `{}` | `{}` | {} | {:.3} | {:.1}% | {:.3} | {:.3} | {:.2}x | {} | {} |",
                    idx + 1,
                    escape_md(item.get("key").and_then(Value::as_str).unwrap_or("-")),
                    escape_md(item.get("source").and_then(Value::as_str).unwrap_or("-")),
                    escape_md(item.get("category").and_then(Value::as_str).unwrap_or("-")),
                    item.get("count").and_then(Value::as_u64).unwrap_or(0),
                    item.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("total_share_percent").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("average_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("max_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("max_load").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("slow").and_then(Value::as_u64).unwrap_or(0),
                    item.get("failed").and_then(Value::as_u64).unwrap_or(0),
                );
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Top methods by total elapsed time");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Rank | Method | Source | Category | Calls | Total ms | Share | Avg ms | Max ms | Max load | Slow | Failed |");
        let _ = writeln!(out, "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        if let Some(items) = analysis.get("by_method_ranked").and_then(Value::as_array) {
            for (idx, item) in items.iter().take(MD_TOP_LIMIT).enumerate() {
                let _ = writeln!(out,
                    "| {} | `{}` | `{}` | `{}` | {} | {:.3} | {:.1}% | {:.3} | {:.3} | {:.2}x | {} | {} |",
                    idx + 1,
                    escape_md(item.get("key").and_then(Value::as_str).unwrap_or("-")),
                    escape_md(item.get("source").and_then(Value::as_str).unwrap_or("-")),
                    escape_md(item.get("category").and_then(Value::as_str).unwrap_or("-")),
                    item.get("count").and_then(Value::as_u64).unwrap_or(0),
                    item.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("total_share_percent").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("average_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("max_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("max_load").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("slow").and_then(Value::as_u64).unwrap_or(0),
                    item.get("failed").and_then(Value::as_u64).unwrap_or(0),
                );
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Budget violations — что пробило кадр/лимит");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Rank | Status | Category | Source | Name | Elapsed ms | Budget ms | Load | Detail |");
        let _ = writeln!(out, "|---:|---|---|---|---|---:|---:|---:|---|");
        if let Some(jobs) = analysis.get("budget_violations").and_then(Value::as_array) {
            for (idx, job) in jobs.iter().take(MD_TOP_LIMIT).enumerate() {
                write_job_row(&mut out, idx + 1, job);
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Top single jobs by elapsed time");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Rank | Status | Category | Source | Name | Elapsed ms | Budget ms | Load | Detail |");
        let _ = writeln!(out, "|---:|---|---|---|---|---:|---:|---:|---|");
        if let Some(jobs) = analysis.get("top_completed_jobs_by_elapsed").and_then(Value::as_array) {
            for (idx, job) in jobs.iter().take(MD_TOP_LIMIT).enumerate() {
                write_job_row(&mut out, idx + 1, job);
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Top single jobs by budget load");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Rank | Status | Category | Source | Name | Elapsed ms | Budget ms | Load | Detail |");
        let _ = writeln!(out, "|---:|---|---|---|---|---:|---:|---:|---|");
        if let Some(jobs) = analysis.get("top_completed_jobs_by_load").and_then(Value::as_array) {
            for (idx, job) in jobs.iter().take(MD_TOP_LIMIT).enumerate() {
                write_job_row(&mut out, idx + 1, job);
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## By category");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Category | Count | Failed | Slow | Total ms | Share | Avg ms | Max ms |");
        let _ = writeln!(out, "|---|---:|---:|---:|---:|---:|---:|---:|");
        if let Some(cats) = analysis.get("by_category_ranked").and_then(Value::as_array) {
            for cat in cats {
                let _ = writeln!(
                    out,
                    "| `{}` | {} | {} | {} | {:.3} | {:.1}% | {:.3} | {:.3} |",
                    escape_md(cat.get("category").and_then(Value::as_str).unwrap_or("-")),
                    cat.get("count").and_then(Value::as_u64).unwrap_or(0),
                    cat.get("failed").and_then(Value::as_u64).unwrap_or(0),
                    cat.get("slow").and_then(Value::as_u64).unwrap_or(0),
                    cat.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    cat.get("total_share_percent").and_then(Value::as_f64).unwrap_or(0.0),
                    cat.get("average_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    cat.get("max_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                );
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## By source");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Source | Calls | Total ms | Share | Avg ms | Max ms | Max load | Slow | Failed |");
        let _ = writeln!(out, "|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        if let Some(items) = analysis.get("by_source_ranked").and_then(Value::as_array) {
            for item in items.iter().take(MD_TOP_LIMIT) {
                let _ = writeln!(out,
                    "| `{}` | {} | {:.3} | {:.1}% | {:.3} | {:.3} | {:.2}x | {} | {} |",
                    escape_md(item.get("key").and_then(Value::as_str).unwrap_or("-")),
                    item.get("count").and_then(Value::as_u64).unwrap_or(0),
                    item.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("total_share_percent").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("average_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("max_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("max_load").and_then(Value::as_f64).unwrap_or(0.0),
                    item.get("slow").and_then(Value::as_u64).unwrap_or(0),
                    item.get("failed").and_then(Value::as_u64).unwrap_or(0),
                );
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## Active jobs");
        let _ = writeln!(out);
        if let Some(active) = report.get("active_jobs").and_then(Value::as_array) {
            if active.is_empty() {
                let _ = writeln!(out, "No active jobs at report flush time.");
            } else {
                let _ = writeln!(out, "| Status | Category | Source | Name | Active ms | Budget ms | Current load | Progress | Detail |");
                let _ = writeln!(out, "|---|---|---|---|---:|---:|---:|---:|---|");
                for job in active.iter().take(MD_TOP_LIMIT) {
                    let _ = writeln!(out,
                        "| `{}` | `{}` | `{}` | `{}` | {:.3} | {:.3} | {:.2}x | {:.1}% | {} |",
                        job.get("status").and_then(Value::as_str).unwrap_or("-"),
                        job.get("category").and_then(Value::as_str).unwrap_or("-"),
                        job.get("source").and_then(Value::as_str).unwrap_or("-"),
                        escape_md(job.get("name").and_then(Value::as_str).unwrap_or("-")),
                        job.get("active_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                        job.get("budget_ms").and_then(Value::as_f64).unwrap_or(0.0),
                        job.get("current_load").and_then(Value::as_f64).unwrap_or(0.0),
                        job.get("progress").and_then(Value::as_f64).unwrap_or(0.0) * 100.0,
                        escape_md(job.get("detail").and_then(Value::as_str).unwrap_or("")),
                    );
                }
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "## CSV outputs");
        let _ = writeln!(out);
        let _ = writeln!(out, "When enabled, the profiler writes these machine-readable tables:");
        let _ = writeln!(out);
        let _ = writeln!(out, "| CSV | Purpose |");
        let _ = writeln!(out, "|---|---|");
        let _ = writeln!(out, "| `profiler_jobs_latest.csv` | all completed jobs with elapsed/budget/load columns |");
        let _ = writeln!(out, "| `profiler_top_offenders_latest.csv` | grouped suspects sorted by total captured time |");
        let _ = writeln!(out, "| `profiler_categories_latest.csv` | category totals and share-of-time |");
        let _ = writeln!(out, "| `profiler_sources_latest.csv` | source totals and share-of-time |");
        let _ = writeln!(out, "| `profiler_active_jobs_latest.csv` | jobs still running at flush time with current load |");
        let _ = writeln!(out, "| `profiler_timeline_latest.csv` | completed jobs with run-relative start/end offsets |");
        let _ = writeln!(out, "| `profiler_methods_latest.csv` | method/service grouped timing totals |");
        let _ = writeln!(out, "| `profiler_budget_violations_latest.csv` | jobs where `load >= 1.0` or slow threshold was crossed |");
        let _ = writeln!(out, "| `profiler_diagnostics_latest.csv` | warnings/errors emitted by profiler analysis |");
        let _ = writeln!(out);

        let _ = writeln!(out, "## Diagnostics");
        let _ = writeln!(out);
        if let Some(diags) = report.get("diagnostics").and_then(Value::as_array) {
            if diags.is_empty() {
                let _ = writeln!(out, "No diagnostics recorded.");
            } else {
                for d in diags.iter().rev().take(128) {
                    let _ = writeln!(
                        out,
                        "- `{}` `{}`: {}",
                        d.get("level").and_then(Value::as_str).unwrap_or("info"),
                        d.get("code").and_then(Value::as_str).unwrap_or("diagnostic"),
                        d.get("message").and_then(Value::as_str).unwrap_or("")
                    );
                }
            }
        }
        out
    }

    fn write_report_files(&self, report: &Value, markdown: &str) -> Result<(ReportPaths, usize), String> {
        let dir = PathBuf::from(&self.cfg.report.directory);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create report directory '{}' failed: {e}", dir.display()))?;
        let created_unix_ms = unix_ms();
        let created_utc = utc_stamp_from_unix_ms(created_unix_ms);
        let archive_prefix = safe_archive_prefix(&self.cfg.report.archive_prefix);
        let archive_name = format!("{archive_prefix}_{created_utc}.zip");
        let archive_path = dir.join(&archive_name);
        let json_entry_name = format!("profiler_report_{created_utc}.json");
        let markdown_entry_name = format!("profiler_report_{created_utc}.md");
        let manifest_entry_name = "manifest.json".to_owned();

        let mut paths = ReportPaths {
            archive: None,
            archive_created_unix_ms: None,
            archive_created_utc: None,
            archive_manifest: None,
            json_latest: None,
            json_timestamped: None,
            markdown_latest: None,
            markdown_timestamped: None,
            csv_latest: None,
            csv_timestamped: None,
        };

        let json_bytes = if self.cfg.report.write_json {
            let bytes = serde_json::to_vec_pretty(report).map_err(|e| e.to_string())?;
            let latest = dir.join(&self.cfg.report.latest_json);
            write_file(&latest, &bytes)?;
            paths.json_latest = Some(path_to_string(&latest));
            Some(bytes)
        } else {
            None
        };

        let markdown_bytes = if self.cfg.report.write_markdown {
            let latest = dir.join(&self.cfg.report.latest_markdown);
            write_file(&latest, markdown.as_bytes())?;
            paths.markdown_latest = Some(path_to_string(&latest));
            Some(markdown.as_bytes().to_vec())
        } else {
            None
        };

        let csv_artifacts = if self.cfg.report.write_csv {
            self.build_csv_artifacts(report, &created_utc)?
        } else {
            Vec::new()
        };
        let csv_total_bytes = csv_artifacts.iter().map(|a| a.bytes.len()).sum::<usize>();

        if !csv_artifacts.is_empty() {
            let mut latest = BTreeMap::new();
            for artifact in &csv_artifacts {
                let latest_path = dir.join(&artifact.latest_name);
                write_file(&latest_path, &artifact.bytes)?;
                latest.insert(artifact.kind.to_owned(), path_to_string(&latest_path));
            }
            paths.csv_latest = Some(latest);
        }

        if self.cfg.report.write_archive {
            let archive_path_string = path_to_string(&archive_path);
            if json_bytes.is_some() {
                paths.json_timestamped = Some(format!("{archive_path_string}#{json_entry_name}"));
            }
            if markdown_bytes.is_some() {
                paths.markdown_timestamped = Some(format!("{archive_path_string}#{markdown_entry_name}"));
            }
            if !csv_artifacts.is_empty() {
                let mut csv_timestamped = BTreeMap::new();
                for artifact in &csv_artifacts {
                    csv_timestamped.insert(artifact.kind.to_owned(), format!("{archive_path_string}#{}", artifact.timestamped_name));
                }
                paths.csv_timestamped = Some(csv_timestamped);
            }
            paths.archive = Some(archive_path_string.clone());
            paths.archive_created_unix_ms = Some(created_unix_ms);
            paths.archive_created_utc = Some(created_utc.clone());
            paths.archive_manifest = Some(format!("{archive_path_string}#{manifest_entry_name}"));

            let csv_entry_names = csv_artifacts
                .iter()
                .map(|a| (a.kind.to_owned(), a.timestamped_name.clone()))
                .collect::<BTreeMap<_, _>>();
            let manifest = self.build_report_archive_manifest(
                report,
                &paths,
                &created_utc,
                created_unix_ms,
                json_bytes.as_ref().map(|_| json_entry_name.as_str()),
                markdown_bytes.as_ref().map(|_| markdown_entry_name.as_str()),
                &csv_entry_names,
            );
            let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;

            let mut entries = Vec::new();
            entries.push(ZipFileEntry { name: manifest_entry_name, bytes: &manifest_bytes });
            if let Some(bytes) = json_bytes.as_ref() {
                entries.push(ZipFileEntry { name: json_entry_name.clone(), bytes });
                if self.cfg.report.include_latest_in_archive {
                    entries.push(ZipFileEntry { name: self.cfg.report.latest_json.clone(), bytes });
                }
            }
            if let Some(bytes) = markdown_bytes.as_ref() {
                entries.push(ZipFileEntry { name: markdown_entry_name.clone(), bytes });
                if self.cfg.report.include_latest_in_archive {
                    entries.push(ZipFileEntry { name: self.cfg.report.latest_markdown.clone(), bytes });
                }
            }
            for artifact in &csv_artifacts {
                entries.push(ZipFileEntry { name: artifact.timestamped_name.clone(), bytes: &artifact.bytes });
                if self.cfg.report.include_latest_in_archive {
                    entries.push(ZipFileEntry { name: artifact.latest_name.clone(), bytes: &artifact.bytes });
                }
            }
            write_stored_zip(&archive_path, created_unix_ms, &entries)?;
        } else {
            if let Some(bytes) = json_bytes.as_ref() {
                let stamped = dir.join(&json_entry_name);
                write_file(&stamped, bytes)?;
                paths.json_timestamped = Some(path_to_string(&stamped));
            }
            if let Some(bytes) = markdown_bytes.as_ref() {
                let stamped = dir.join(&markdown_entry_name);
                write_file(&stamped, bytes)?;
                paths.markdown_timestamped = Some(path_to_string(&stamped));
            }
            if !csv_artifacts.is_empty() {
                let mut timestamped = BTreeMap::new();
                for artifact in &csv_artifacts {
                    let stamped = dir.join(&artifact.timestamped_name);
                    write_file(&stamped, &artifact.bytes)?;
                    timestamped.insert(artifact.kind.to_owned(), path_to_string(&stamped));
                }
                paths.csv_timestamped = Some(timestamped);
            }
        }

        Ok((paths, csv_total_bytes))
    }

    fn build_report_archive_manifest(
        &self,
        report: &Value,
        paths: &ReportPaths,
        created_utc: &str,
        created_unix_ms: u128,
        json_entry_name: Option<&str>,
        markdown_entry_name: Option<&str>,
        csv_entry_names: &BTreeMap<String, String>,
    ) -> Value {
        json!({
            "schema": "newengine.profiler.report_archive.manifest.v2",
            "created_utc": created_utc,
            "created_unix_ms": created_unix_ms,
            "reason": report.get("reason").cloned().unwrap_or(Value::Null),
            "archive": paths.archive.clone(),
            "latest": {
                "json": paths.json_latest.clone(),
                "markdown": paths.markdown_latest.clone(),
                "csv": paths.csv_latest.clone(),
            },
            "entries": {
                "json": json_entry_name,
                "markdown": markdown_entry_name,
                "csv": csv_entry_names,
                "manifest": "manifest.json",
            },
            "policy": {
                "timestamped_files_are_archive_members": self.cfg.report.write_archive,
                "latest_files_are_written_for_compatibility": true,
                "latest_files_are_duplicated_in_archive": self.cfg.report.include_latest_in_archive,
                "csv_files_are_machine_readable_source_for_external_charts": self.cfg.report.write_csv,
            },
        })
    }

    fn build_csv_artifacts(&self, report: &Value, created_utc: &str) -> Result<Vec<CsvArtifact>, String> {
        let specs = [
            ("jobs", self.cfg.report.latest_jobs_csv.clone(), format!("profiler_jobs_{created_utc}.csv"), csv_completed_jobs(report)),
            ("categories", self.cfg.report.latest_categories_csv.clone(), format!("profiler_categories_{created_utc}.csv"), csv_category_summary(report)),
            ("sources", self.cfg.report.latest_sources_csv.clone(), format!("profiler_sources_{created_utc}.csv"), csv_source_summary(report)),
            ("top_offenders", self.cfg.report.latest_offenders_csv.clone(), format!("profiler_top_offenders_{created_utc}.csv"), csv_top_offenders(report)),
            ("active_jobs", self.cfg.report.latest_active_csv.clone(), format!("profiler_active_jobs_{created_utc}.csv"), csv_active_jobs(report)),
            ("diagnostics", self.cfg.report.latest_diagnostics_csv.clone(), format!("profiler_diagnostics_{created_utc}.csv"), csv_diagnostics(report)),
            ("timeline", self.cfg.report.latest_timeline_csv.clone(), format!("profiler_timeline_{created_utc}.csv"), csv_timeline(report)),
            ("methods", self.cfg.report.latest_methods_csv.clone(), format!("profiler_methods_{created_utc}.csv"), csv_methods(report)),
            ("budget_violations", self.cfg.report.latest_budget_violations_csv.clone(), format!("profiler_budget_violations_{created_utc}.csv"), csv_budget_violations(report)),
        ];

        let mut artifacts = Vec::with_capacity(specs.len());
        for (kind, latest_name, timestamped_name, content) in specs {
            let bytes = content.into_bytes();
            if bytes.len() > CSV_LIMIT * 1024 * 1024 {
                return Err(format!("profiler CSV artifact '{kind}' is unexpectedly large: {} bytes", bytes.len()));
            }
            artifacts.push(CsvArtifact { kind, latest_name, timestamped_name, bytes });
        }
        Ok(artifacts)
    }
}

fn accumulate(stats: &mut AggregateStats, job: &JobRecord, failed: bool, slow: bool) {
    let elapsed = job.elapsed_ms.unwrap_or_default();
    let load = job.load.unwrap_or_default();
    stats.count = stats.count.saturating_add(1);
    stats.total_elapsed_ms += elapsed;
    stats.max_elapsed_ms = stats.max_elapsed_ms.max(elapsed);
    stats.max_load = stats.max_load.max(load);
    stats.total_payload_bytes = stats.total_payload_bytes.saturating_add(job.payload_bytes.unwrap_or_default());
    stats.total_output_bytes = stats.total_output_bytes.saturating_add(job.output_bytes.unwrap_or_default());
    if failed {
        stats.failed = stats.failed.saturating_add(1);
    }
    if slow {
        stats.slow = stats.slow.saturating_add(1);
    }
}

fn finalize_aggregates(map: &mut BTreeMap<String, AggregateStats>, total_elapsed_ms: f64) {
    for value in map.values_mut() {
        value.average_elapsed_ms = if value.count > 0 {
            value.total_elapsed_ms / value.count as f64
        } else {
            0.0
        };
        value.total_share_percent = percent_of(value.total_elapsed_ms, total_elapsed_ms);
    }
}

fn ranked_aggregates(map: BTreeMap<String, AggregateStats>, limit: usize) -> Vec<Value> {
    let mut values = map.into_values().collect::<Vec<_>>();
    values.sort_by(|a, b| cmp_f64_desc(a.total_elapsed_ms, b.total_elapsed_ms).then_with(|| a.key.cmp(&b.key)));
    values
        .into_iter()
        .take(limit)
        .map(|v| serde_json::to_value(v).unwrap_or(Value::Null))
        .collect()
}

fn ranked_jobs_by(jobs: &std::collections::VecDeque<JobRecord>, by: &str, limit: usize) -> Vec<Value> {
    let mut values = jobs.iter().collect::<Vec<_>>();
    values.sort_by(|a, b| {
        let av = if by == "load" { a.load.unwrap_or_default() } else { a.elapsed_ms.unwrap_or_default() };
        let bv = if by == "load" { b.load.unwrap_or_default() } else { b.elapsed_ms.unwrap_or_default() };
        cmp_f64_desc(av, bv).then_with(|| a.name.cmp(&b.name))
    });
    values
        .into_iter()
        .take(limit)
        .map(|job| serde_json::to_value(job).unwrap_or(Value::Null))
        .collect()
}


fn ranked_budget_violations(jobs: &std::collections::VecDeque<JobRecord>, slow_job_warn_ms: f64, limit: usize) -> Vec<Value> {
    let mut values = jobs
        .iter()
        .filter(|job| job.load.unwrap_or_default() >= 1.0 || job.elapsed_ms.unwrap_or_default() >= slow_job_warn_ms)
        .collect::<Vec<_>>();
    values.sort_by(|a, b| {
        cmp_f64_desc(a.load.unwrap_or_default(), b.load.unwrap_or_default())
            .then_with(|| cmp_f64_desc(a.elapsed_ms.unwrap_or_default(), b.elapsed_ms.unwrap_or_default()))
            .then_with(|| a.name.cmp(&b.name))
    });
    values
        .into_iter()
        .take(limit)
        .map(|job| serde_json::to_value(job).unwrap_or(Value::Null))
        .collect()
}

fn percentiles_json(mut values: Vec<f64>) -> Value {
    values.retain(|value| value.is_finite());
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "p50": percentile_sorted(&values, 0.50),
        "p90": percentile_sorted(&values, 0.90),
        "p95": percentile_sorted(&values, 0.95),
        "p99": percentile_sorted(&values, 0.99),
    })
}

fn percentile_sorted(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let last = values.len().saturating_sub(1);
    let idx = ((last as f64) * q.clamp(0.0, 1.0)).round() as usize;
    values[idx.min(last)]
}

fn sort_objects_desc(values: &mut [Value], key: &str) {
    values.sort_by(|a, b| {
        let av = a.get(key).and_then(Value::as_f64).unwrap_or(0.0);
        let bv = b.get(key).and_then(Value::as_f64).unwrap_or(0.0);
        cmp_f64_desc(av, bv)
    });
}

fn cmp_f64_desc(a: f64, b: f64) -> std::cmp::Ordering {
    b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal)
}

fn percent_of(value: f64, total: f64) -> f64 {
    if total > 0.0 { (value / total) * 100.0 } else { 0.0 }
}

fn job_owner_key(job: &JobRecord) -> String {
    first_metadata_str(&job.metadata, &["/service_id", "/metadata/service_id", "/provider_service_id", "/metadata/provider_service_id"])
        .or_else(|| first_metadata_str(&job.metadata, &["/plugin_id", "/metadata/plugin_id", "/owner_plugin_id", "/metadata/owner_plugin_id"])
            .map(|v| format!("plugin:{v}")))
        .or_else(|| first_metadata_str(&job.metadata, &["/gateway", "/engine_gateway", "/metadata/gateway", "/metadata/engine_gateway"])
            .map(|v| format!("gateway:{v}")))
        .unwrap_or_else(|| format!("{}:{}", job.source, job.category))
}


fn job_method_key(job: &JobRecord) -> String {
    first_metadata_str(&job.metadata, &["/method", "/method_name", "/metadata/method", "/metadata/method_name"])
        .map(|method| format!("{}::{method}", job_owner_key(job)))
        .unwrap_or_else(|| format!("{}::<no-method>", job_owner_key(job)))
}

fn job_offender_key(job: &JobRecord) -> String {
    let owner = job_owner_key(job);
    if let Some(method) = first_metadata_str(&job.metadata, &["/method", "/method_name", "/metadata/method", "/metadata/method_name"]) {
        format!("{owner}::{method}")
    } else {
        format!("{owner}::{}", job.name)
    }
}

fn first_metadata_str(value: &Value, paths: &[&str]) -> Option<String> {
    paths
        .iter()
        .filter_map(|path| value.pointer(path).and_then(Value::as_str))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .next()
}

fn bar(value: f64, max: f64, width: usize) -> String {
    let ratio = if max > 0.0 { (value / max).clamp(0.0, 1.0) } else { 0.0 };
    let filled = (ratio * width as f64).round() as usize;
    format!("{}{}", "█".repeat(filled), "░".repeat(width.saturating_sub(filled)))
}

fn write_ranked_chart(out: &mut String, title: &str, rows: Option<&Vec<Value>>, label_key: &str, total_ms: f64) {
    let _ = writeln!(out, "{title}");
    let _ = writeln!(out);
    let Some(rows) = rows else {
        let _ = writeln!(out, "No data.");
        let _ = writeln!(out);
        return;
    };
    if rows.is_empty() {
        let _ = writeln!(out, "No data.");
        let _ = writeln!(out);
        return;
    }
    let _ = writeln!(out, "```text");
    for row in rows.iter().take(10) {
        let label = row.get(label_key).and_then(Value::as_str).unwrap_or("-");
        let elapsed = row.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0);
        let share = row.get("total_share_percent").and_then(Value::as_f64).unwrap_or_else(|| percent_of(elapsed, total_ms));
        let short = shorten(label, 34);
        let _ = writeln!(out, "{short:<34} [{}] {:>7.3} ms {:>5.1}%", bar(share, 100.0, 28), elapsed, share);
    }
    let _ = writeln!(out, "```");
    let _ = writeln!(out);
}

fn write_job_row(out: &mut String, rank: usize, job: &Value) {
    let _ = writeln!(out,
        "| {} | `{}` | `{}` | `{}` | `{}` | {:.3} | {:.3} | {:.2}x | {} |",
        rank,
        job.get("status").and_then(Value::as_str).unwrap_or("-"),
        job.get("category").and_then(Value::as_str).unwrap_or("-"),
        job.get("source").and_then(Value::as_str).unwrap_or("-"),
        escape_md(job.get("name").and_then(Value::as_str).unwrap_or("-")),
        job.get("elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
        job.get("budget_ms").and_then(Value::as_f64).unwrap_or(0.0),
        job.get("load").and_then(Value::as_f64).unwrap_or(0.0),
        escape_md(job.get("detail").and_then(Value::as_str).unwrap_or("")),
    );
}

fn shorten(s: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in s.chars().enumerate() {
        if idx >= max_chars.saturating_sub(1) {
            out.push('…');
            return out;
        }
        out.push(ch);
    }
    out
}

fn csv_completed_jobs(report: &Value) -> String {
    let mut out = csv_header(&[
        "id", "status", "category", "source", "name", "elapsed_ms", "budget_ms", "load", "load_percent", "over_budget", "started_unix_ms", "ended_unix_ms", "payload_bytes", "output_bytes", "service_id", "method", "plugin_id", "gateway", "error", "detail",
    ]);
    if let Some(jobs) = report.get("completed_jobs").and_then(Value::as_array) {
        for job in jobs {
            let load = f(job, "load");
            csv_push(&mut out, &[
                s(job, "id"), s(job, "status"), s(job, "category"), s(job, "source"), s(job, "name"),
                f(job, "elapsed_ms"), f(job, "budget_ms"), load.clone(), format!("{:.3}", load.parse::<f64>().unwrap_or(0.0) * 100.0), (load.parse::<f64>().unwrap_or(0.0) >= 1.0).to_string(),
                scalar(job.get("started_unix_ms")), scalar(job.get("ended_unix_ms")), scalar(job.get("payload_bytes")), scalar(job.get("output_bytes")),
                metadata_csv(job, &["/metadata/service_id", "/metadata/metadata/service_id"]),
                metadata_csv(job, &["/metadata/method", "/metadata/method_name", "/metadata/metadata/method"]),
                metadata_csv(job, &["/metadata/plugin_id", "/metadata/metadata/plugin_id"]),
                metadata_csv(job, &["/metadata/gateway", "/metadata/engine_gateway", "/metadata/metadata/gateway"]),
                s(job, "error"), s(job, "detail"),
            ]);
        }
    }
    out
}

fn csv_category_summary(report: &Value) -> String {
    let mut out = csv_header(&["rank", "category", "count", "failed", "slow", "total_elapsed_ms", "total_share_percent", "average_elapsed_ms", "max_elapsed_ms"]);
    if let Some(rows) = report.pointer("/analysis/by_category_ranked").and_then(Value::as_array) {
        for (idx, row) in rows.iter().enumerate() {
            csv_push(&mut out, &[rank(idx), s(row, "category"), u(row, "count"), u(row, "failed"), u(row, "slow"), f(row, "total_elapsed_ms"), f(row, "total_share_percent"), f(row, "average_elapsed_ms"), f(row, "max_elapsed_ms")]);
        }
    }
    out
}

fn csv_source_summary(report: &Value) -> String {
    csv_aggregate(report.pointer("/analysis/by_source_ranked").and_then(Value::as_array))
}

fn csv_top_offenders(report: &Value) -> String {
    csv_aggregate(report.pointer("/analysis/top_offenders_by_total_elapsed").and_then(Value::as_array))
}

fn csv_methods(report: &Value) -> String {
    csv_aggregate(report.pointer("/analysis/by_method_ranked").and_then(Value::as_array))
}

fn csv_budget_violations(report: &Value) -> String {
    let mut out = csv_header(&["rank", "id", "status", "category", "source", "name", "elapsed_ms", "budget_ms", "load", "load_percent", "started_unix_ms", "ended_unix_ms", "service_id", "method", "plugin_id", "gateway", "error", "detail"]);
    if let Some(jobs) = report.pointer("/analysis/budget_violations").and_then(Value::as_array) {
        for (idx, job) in jobs.iter().enumerate() {
            let load = f(job, "load");
            csv_push(&mut out, &[
                rank(idx), s(job, "id"), s(job, "status"), s(job, "category"), s(job, "source"), s(job, "name"),
                f(job, "elapsed_ms"), f(job, "budget_ms"), load.clone(), format!("{:.3}", load.parse::<f64>().unwrap_or(0.0) * 100.0),
                scalar(job.get("started_unix_ms")), scalar(job.get("ended_unix_ms")),
                metadata_csv(job, &["/metadata/service_id", "/metadata/metadata/service_id"]),
                metadata_csv(job, &["/metadata/method", "/metadata/method_name", "/metadata/metadata/method"]),
                metadata_csv(job, &["/metadata/plugin_id", "/metadata/metadata/plugin_id"]),
                metadata_csv(job, &["/metadata/gateway", "/metadata/engine_gateway", "/metadata/metadata/gateway"]),
                s(job, "error"), s(job, "detail"),
            ]);
        }
    }
    out
}

fn csv_aggregate(rows: Option<&Vec<Value>>) -> String {
    let mut out = csv_header(&["rank", "key", "category", "source", "sample_name", "count", "failed", "slow", "total_elapsed_ms", "total_share_percent", "average_elapsed_ms", "max_elapsed_ms", "max_load", "total_payload_bytes", "total_output_bytes"]);
    if let Some(rows) = rows {
        for (idx, row) in rows.iter().enumerate() {
            csv_push(&mut out, &[rank(idx), s(row, "key"), s(row, "category"), s(row, "source"), s(row, "sample_name"), u(row, "count"), u(row, "failed"), u(row, "slow"), f(row, "total_elapsed_ms"), f(row, "total_share_percent"), f(row, "average_elapsed_ms"), f(row, "max_elapsed_ms"), f(row, "max_load"), u(row, "total_payload_bytes"), u(row, "total_output_bytes")]);
        }
    }
    out
}

fn csv_active_jobs(report: &Value) -> String {
    let mut out = csv_header(&["id", "status", "category", "source", "name", "active_elapsed_ms", "budget_ms", "current_load", "current_load_percent", "current_over_budget", "progress", "started_unix_ms", "detail"]);
    if let Some(jobs) = report.get("active_jobs").and_then(Value::as_array) {
        for job in jobs {
            let load = job.get("current_load").and_then(Value::as_f64).unwrap_or(0.0);
            csv_push(&mut out, &[s(job, "id"), s(job, "status"), s(job, "category"), s(job, "source"), s(job, "name"), f(job, "active_elapsed_ms"), f(job, "budget_ms"), format!("{load:.6}"), format!("{:.3}", load * 100.0), (load >= 1.0).to_string(), f(job, "progress"), scalar(job.get("started_unix_ms")), s(job, "detail")]);
        }
    }
    out
}

fn csv_diagnostics(report: &Value) -> String {
    let mut out = csv_header(&["at_unix_ms", "level", "code", "job_id", "message"]);
    if let Some(rows) = report.get("diagnostics").and_then(Value::as_array) {
        for row in rows {
            csv_push(&mut out, &[scalar(row.get("at_unix_ms")), s(row, "level"), s(row, "code"), s(row, "job_id"), s(row, "message")]);
        }
    }
    out
}

fn csv_timeline(report: &Value) -> String {
    let run_start = report.pointer("/run/started_unix_ms").and_then(Value::as_u64).unwrap_or(0);
    let mut out = csv_header(&["id", "category", "source", "name", "status", "start_offset_ms", "end_offset_ms", "elapsed_ms", "budget_ms", "load", "detail"]);
    if let Some(jobs) = report.get("completed_jobs").and_then(Value::as_array) {
        for job in jobs {
            let start = job.get("started_unix_ms").and_then(Value::as_u64).unwrap_or(run_start).saturating_sub(run_start);
            let end = job.get("ended_unix_ms").and_then(Value::as_u64).unwrap_or(run_start).saturating_sub(run_start);
            csv_push(&mut out, &[s(job, "id"), s(job, "category"), s(job, "source"), s(job, "name"), s(job, "status"), start.to_string(), end.to_string(), f(job, "elapsed_ms"), f(job, "budget_ms"), f(job, "load"), s(job, "detail")]);
        }
    }
    out
}

fn csv_header(cols: &[&str]) -> String {
    let mut out = String::new();
    csv_push(&mut out, &cols.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    out
}

fn csv_push(out: &mut String, cells: &[String]) {
    for (idx, cell) in cells.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&csv_escape(cell));
    }
    out.push('\n');
}

fn csv_escape(cell: &str) -> String {
    if cell.contains(',') || cell.contains('"') || cell.contains('\n') || cell.contains('\r') {
        format!("\"{}\"", cell.replace('"', "\"\""))
    } else {
        cell.to_owned()
    }
}

fn rank(idx: usize) -> String { (idx + 1).to_string() }
fn s(row: &Value, key: &str) -> String { row.get(key).and_then(Value::as_str).unwrap_or("").to_owned() }
fn u(row: &Value, key: &str) -> String { row.get(key).and_then(Value::as_u64).map(|v| v.to_string()).unwrap_or_default() }
fn f(row: &Value, key: &str) -> String { row.get(key).and_then(Value::as_f64).map(|v| format!("{v:.6}")).unwrap_or_default() }

fn scalar(value: Option<&Value>) -> String {
    value.map(format_json_scalar).unwrap_or_default()
}

fn metadata_csv(row: &Value, paths: &[&str]) -> String {
    paths.iter().find_map(|path| row.pointer(path).and_then(Value::as_str)).unwrap_or("").to_owned()
}

fn safe_archive_prefix(value: &str) -> String {
    let sanitized: String = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('.').trim_matches('_').to_owned();
    if sanitized.is_empty() {
        "profiler_report".to_owned()
    } else {
        sanitized
    }
}

fn is_shutdown_report_reason(reason: &str) -> bool {
    matches!(reason, "service.shutdown_v1" | "plugin.shutdown")
}
