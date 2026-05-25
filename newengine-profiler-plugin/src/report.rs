use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;

use crate::archive::{write_stored_zip, ZipFileEntry};
use crate::constants::{ENGINE_PROFILER_GATEWAY_ID, PROFILER_PLUGIN_ID, PROFILER_PLUGIN_NAME, PROFILER_SERVICE_ID};
use crate::records::{CategoryStats, JobRecord, ProfilerDiagnostic, ProfilerState, ReportPaths};
use crate::runtime::ProfilerRuntime;
use crate::util::{duration_ms, escape_md, format_json_scalar, path_to_string, unix_ms, utc_stamp_from_unix_ms, write_file};

impl ProfilerRuntime {
    pub(crate) fn flush_report(&self, reason: &str) -> Result<Value, String> {
        let shutdown_report = is_shutdown_report_reason(reason);
        {
            let state = self.lock_state();
            if shutdown_report && state.shutdown_report_written {
                return Ok(json!({
                    "schema": "newengine.profiler.flush_report.result.v1",
                    "reason": reason,
                    "paths": state.last_report_paths.clone(),
                    "json_bytes": 0,
                    "markdown_bytes": 0,
                    "skipped_duplicate_shutdown_report": true,
                }));
            }
        }

        let (report, markdown, paths) = {
            let mut state = self.lock_state();
            if shutdown_report && state.shutdown_report_written {
                return Ok(json!({
                    "schema": "newengine.profiler.flush_report.result.v1",
                    "reason": reason,
                    "paths": state.last_report_paths.clone(),
                    "json_bytes": 0,
                    "markdown_bytes": 0,
                    "skipped_duplicate_shutdown_report": true,
                }));
            }
            let report = self.build_report_locked(&state, reason);
            let markdown = self.build_markdown_report(&report);
            let paths = self.write_report_files(&report, &markdown)?;
            state.reports_written = state.reports_written.saturating_add(1);
            if shutdown_report {
                state.shutdown_report_written = true;
            }
            state.last_report_paths = Some(paths);
            (report, markdown, state.last_report_paths.as_ref().cloned())
        };

        Ok(json!({
            "schema": "newengine.profiler.flush_report.result.v1",
            "reason": reason,
            "paths": paths,
            "json_bytes": serde_json::to_vec(&report).map(|v| v.len()).unwrap_or(0),
            "markdown_bytes": markdown.len(),
            "skipped_duplicate_shutdown_report": false,
        }))
    }

    fn build_report_locked(&self, state: &ProfilerState, reason: &str) -> Value {
        let mut by_category: BTreeMap<String, CategoryStats> = BTreeMap::new();
        let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
        let mut failed = 0u64;
        let mut slow = 0u64;
        let mut total_elapsed_ms = 0.0f64;
        let mut max_elapsed_ms = 0.0f64;

        for job in &state.completed {
            let elapsed = job.elapsed_ms.unwrap_or_default();
            total_elapsed_ms += elapsed;
            max_elapsed_ms = max_elapsed_ms.max(elapsed);
            *by_status.entry(job.status.clone()).or_insert(0) += 1;
            let cat = by_category.entry(job.category.clone()).or_default();
            cat.count = cat.count.saturating_add(1);
            cat.total_elapsed_ms += elapsed;
            cat.max_elapsed_ms = cat.max_elapsed_ms.max(elapsed);
            if job.status == "failed" {
                failed = failed.saturating_add(1);
                cat.failed = cat.failed.saturating_add(1);
            }
            if elapsed >= self.cfg.diagnostics.slow_job_warn_ms || job.load.unwrap_or_default() >= 1.0 {
                slow = slow.saturating_add(1);
                cat.slow = cat.slow.saturating_add(1);
            }
        }

        let active_jobs: Vec<&JobRecord> = state.active.values().map(|j| &j.record).collect();
        let completed_jobs: Vec<&JobRecord> = state.completed.iter().collect();
        let diagnostics: Vec<&ProfilerDiagnostic> = state.diagnostics.iter().collect();
        let completed_count = state.completed.len() as f64;

        json!({
            "schema": "newengine.profiler.report.v1",
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
                "total_elapsed_ms": total_elapsed_ms,
                "average_elapsed_ms": if completed_count > 0.0 { total_elapsed_ms / completed_count } else { 0.0 },
                "max_elapsed_ms": max_elapsed_ms,
                "by_status": by_status,
                "by_category": by_category,
            },
            "active_jobs": active_jobs,
            "completed_jobs": completed_jobs,
            "diagnostics": diagnostics,
            "config": self.cfg.clone(),
        })
    }

    fn build_markdown_report(&self, report: &Value) -> String {
        let mut out = String::new();
        let summary = report.get("summary").unwrap_or(&Value::Null);
        let run = report.get("run").unwrap_or(&Value::Null);

        let _ = writeln!(out, "# North Star Engine Profiler Report");
        let _ = writeln!(out);
        let _ = writeln!(out, "- reason: `{}`", report.get("reason").and_then(Value::as_str).unwrap_or("unknown"));
        let _ = writeln!(out, "- uptime_ms: `{:.3}`", run.get("uptime_ms").and_then(Value::as_f64).unwrap_or(0.0));
        let _ = writeln!(out, "- events_seen: `{}`", run.get("events_seen").and_then(Value::as_u64).unwrap_or(0));
        let _ = writeln!(out, "- malformed_events: `{}`", run.get("malformed_events").and_then(Value::as_u64).unwrap_or(0));
        let _ = writeln!(out);
        let _ = writeln!(out, "## Summary");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Metric | Value |");
        let _ = writeln!(out, "|---|---:|");
        for key in [
            "active_jobs",
            "completed_jobs_kept",
            "failed_jobs",
            "slow_or_over_budget_jobs",
            "total_elapsed_ms",
            "average_elapsed_ms",
            "max_elapsed_ms",
        ] {
            let value = summary.get(key).cloned().unwrap_or(Value::Null);
            let _ = writeln!(out, "| `{key}` | `{}` |", format_json_scalar(&value));
        }

        let _ = writeln!(out);
        let _ = writeln!(out, "## By category");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Category | Count | Failed | Slow | Total ms | Max ms |");
        let _ = writeln!(out, "|---|---:|---:|---:|---:|---:|");
        if let Some(cats) = summary.get("by_category").and_then(Value::as_object) {
            for (cat, st) in cats {
                let _ = writeln!(
                    out,
                    "| `{}` | {} | {} | {} | {:.3} | {:.3} |",
                    cat,
                    st.get("count").and_then(Value::as_u64).unwrap_or(0),
                    st.get("failed").and_then(Value::as_u64).unwrap_or(0),
                    st.get("slow").and_then(Value::as_u64).unwrap_or(0),
                    st.get("total_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    st.get("max_elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                );
            }
        }

        let _ = writeln!(out);
        let _ = writeln!(out, "## Recent completed jobs");
        let _ = writeln!(out);
        let _ = writeln!(out, "| Status | Category | Name | Elapsed ms | Load | Detail |");
        let _ = writeln!(out, "|---|---|---|---:|---:|---|");
        if let Some(jobs) = report.get("completed_jobs").and_then(Value::as_array) {
            for job in jobs.iter().rev().take(128) {
                let _ = writeln!(
                    out,
                    "| `{}` | `{}` | `{}` | {:.3} | {:.2} | {} |",
                    job.get("status").and_then(Value::as_str).unwrap_or("-"),
                    job.get("category").and_then(Value::as_str).unwrap_or("-"),
                    escape_md(job.get("name").and_then(Value::as_str).unwrap_or("-")),
                    job.get("elapsed_ms").and_then(Value::as_f64).unwrap_or(0.0),
                    job.get("load").and_then(Value::as_f64).unwrap_or(0.0),
                    escape_md(job.get("detail").and_then(Value::as_str).unwrap_or("")),
                );
            }
        }

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

    fn write_report_files(&self, report: &Value, markdown: &str) -> Result<ReportPaths, String> {
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

        if self.cfg.report.write_archive {
            let archive_path_string = path_to_string(&archive_path);
            if json_bytes.is_some() {
                paths.json_timestamped = Some(format!("{archive_path_string}#{json_entry_name}"));
            }
            if markdown_bytes.is_some() {
                paths.markdown_timestamped = Some(format!("{archive_path_string}#{markdown_entry_name}"));
            }
            paths.archive = Some(archive_path_string.clone());
            paths.archive_created_unix_ms = Some(created_unix_ms);
            paths.archive_created_utc = Some(created_utc.clone());
            paths.archive_manifest = Some(format!("{archive_path_string}#{manifest_entry_name}"));

            let manifest = self.build_report_archive_manifest(
                report,
                &paths,
                &created_utc,
                created_unix_ms,
                json_bytes.as_ref().map(|_| json_entry_name.as_str()),
                markdown_bytes.as_ref().map(|_| markdown_entry_name.as_str()),
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
        }

        Ok(paths)
    }

    fn build_report_archive_manifest(
        &self,
        report: &Value,
        paths: &ReportPaths,
        created_utc: &str,
        created_unix_ms: u128,
        json_entry_name: Option<&str>,
        markdown_entry_name: Option<&str>,
    ) -> Value {
        json!({
            "schema": "newengine.profiler.report_archive.manifest.v1",
            "created_utc": created_utc,
            "created_unix_ms": created_unix_ms,
            "reason": report.get("reason").cloned().unwrap_or(Value::Null),
            "archive": paths.archive.clone(),
            "latest": {
                "json": paths.json_latest.clone(),
                "markdown": paths.markdown_latest.clone(),
            },
            "entries": {
                "json": json_entry_name,
                "markdown": markdown_entry_name,
                "manifest": "manifest.json",
            },
            "policy": {
                "timestamped_files_are_archive_members": self.cfg.report.write_archive,
                "latest_files_are_written_for_compatibility": true,
                "latest_files_are_duplicated_in_archive": self.cfg.report.include_latest_in_archive,
            },
        })
    }


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
