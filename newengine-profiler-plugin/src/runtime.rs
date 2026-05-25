use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::config::ProfilerConfig;
use crate::constants::{
    ENGINE_PROFILER_GATEWAY_ID, PROFILER_PLUGIN_ID, PROFILER_SERVICE_ID, TOPIC_JOB_BEGIN,
    TOPIC_JOB_END, TOPIC_JOB_STATUS,
};
use crate::records::{
    ActiveJob, JobBeginWire, JobEndWire, JobRecord, JobStatusWire, ProfilerDiagnostic,
    ProfilerState,
};
use crate::util::{
    begin_to_json, duration_ms, merge_metadata, sanitize_non_empty, trim_payload_preview, unix_ms,
};

pub(crate) static RUNTIME: OnceLock<Arc<ProfilerRuntime>> = OnceLock::new();

pub(crate) struct ProfilerRuntime {
    pub(crate) cfg: ProfilerConfig,
    state: Mutex<ProfilerState>,
}

impl ProfilerRuntime {
    pub(crate) fn new(cfg: ProfilerConfig) -> Self {
        Self {
            cfg,
            state: Mutex::new(ProfilerState::new()),
        }
    }

    pub(crate) fn on_event(&self, topic: &str, payload: &[u8]) {
        if !self.cfg.enabled {
            return;
        }

        let mut state = self.lock_state();
        state.events_seen = state.events_seen.saturating_add(1);

        let parsed = match serde_json::from_slice::<Value>(payload) {
            Ok(v) => v,
            Err(e) => {
                state.malformed_events = state.malformed_events.saturating_add(1);
                Self::push_diag_locked(
                    &self.cfg,
                    &mut state,
                    "warn",
                    "malformed_event_json",
                    format!("event topic='{topic}' has invalid JSON: {e}"),
                    None,
                );
                return;
            }
        };

        let category = parsed
            .get("category")
            .and_then(Value::as_str)
            .unwrap_or_default();

        if !self.capture_topic(topic, category) {
            return;
        }

        if self.cfg.ignore_self && self.is_self_event(&parsed) {
            return;
        }

        match topic {
            TOPIC_JOB_BEGIN => {
                if let Err(e) = self.record_begin_value_locked(&mut state, parsed) {
                    state.malformed_events = state.malformed_events.saturating_add(1);
                    Self::push_diag_locked(
                        &self.cfg,
                        &mut state,
                        "warn",
                        "bad_job_begin_event",
                        format!("bad job begin event: {e}"),
                        None,
                    );
                }
            }
            TOPIC_JOB_END => {
                if let Err(e) = self.record_end_value_locked(&mut state, parsed) {
                    state.malformed_events = state.malformed_events.saturating_add(1);
                    Self::push_diag_locked(
                        &self.cfg,
                        &mut state,
                        "warn",
                        "bad_job_end_event",
                        format!("bad job end event: {e}"),
                        None,
                    );
                }
            }
            TOPIC_JOB_STATUS => {
                if let Err(e) = self.record_status_value_locked(&mut state, parsed) {
                    state.malformed_events = state.malformed_events.saturating_add(1);
                    Self::push_diag_locked(
                        &self.cfg,
                        &mut state,
                        "warn",
                        "bad_job_status_event",
                        format!("bad job status event: {e}"),
                        None,
                    );
                }
            }
            _ => {
                if self.cfg.capture.custom_events {
                    self.record_custom_event_locked(&mut state, topic, parsed);
                }
            }
        }
    }

    fn capture_topic(&self, topic: &str, category: &str) -> bool {
        if !self.cfg.enabled {
            return false;
        }
        if category == "service_call" && !self.cfg.capture.service_calls {
            return false;
        }
        if category == "plugin_lifecycle" && !self.cfg.capture.plugin_lifecycle {
            return false;
        }
        if topic == TOPIC_JOB_STATUS && !self.cfg.capture.task_status_events {
            return false;
        }
        true
    }

    fn is_self_event(&self, value: &Value) -> bool {
        value
            .get("plugin_id")
            .or_else(|| value.pointer("/metadata/plugin_id"))
            .or_else(|| value.get("service_id"))
            .or_else(|| value.pointer("/metadata/service_id"))
            .and_then(Value::as_str)
            .map(|id| id == PROFILER_PLUGIN_ID || id == PROFILER_SERVICE_ID || id == ENGINE_PROFILER_GATEWAY_ID)
            .unwrap_or(false)
    }

    pub(crate) fn record_begin_value(&self, payload: &[u8]) -> Result<Value, String> {
        let value = serde_json::from_slice::<Value>(payload).map_err(|e| e.to_string())?;
        let mut state = self.lock_state();
        self.record_begin_value_locked(&mut state, value)?;
        Ok(self.snapshot_locked(&state))
    }

    pub(crate) fn record_end_value(&self, payload: &[u8]) -> Result<Value, String> {
        let value = serde_json::from_slice::<Value>(payload).map_err(|e| e.to_string())?;
        let mut state = self.lock_state();
        self.record_end_value_locked(&mut state, value)?;
        Ok(self.snapshot_locked(&state))
    }

    pub(crate) fn record_status_value(&self, payload: &[u8]) -> Result<Value, String> {
        let value = serde_json::from_slice::<Value>(payload).map_err(|e| e.to_string())?;
        let mut state = self.lock_state();
        self.record_status_value_locked(&mut state, value)?;
        Ok(self.snapshot_locked(&state))
    }

    fn record_begin_value_locked(&self, state: &mut ProfilerState, value: Value) -> Result<(), String> {
        let wire: JobBeginWire = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        let id = wire.id.unwrap_or_else(|| state.local_id());
        let category = sanitize_non_empty(wire.category.as_deref(), "custom_job");
        let budget = wire.budget_ms.unwrap_or_else(|| self.default_budget_for(&category));
        let name = wire
            .name
            .or(wire.label)
            .unwrap_or_else(|| id.clone());

        let record = JobRecord {
            id: id.clone(),
            name,
            category,
            source: wire.source.unwrap_or_else(|| "event".to_owned()),
            status: "running".to_owned(),
            detail: wire.detail.unwrap_or_default(),
            started_unix_ms: unix_ms(),
            ended_unix_ms: None,
            elapsed_ms: None,
            budget_ms: budget.max(0.001),
            load: None,
            progress: None,
            payload_bytes: wire.payload_bytes,
            output_bytes: None,
            error: None,
            metadata: wire.metadata.unwrap_or(value),
        };

        if state.active.insert(id.clone(), ActiveJob { record, started_at: Instant::now() }).is_some() {
            Self::push_diag_locked(
                &self.cfg,
                state,
                "warn",
                "job_restarted_without_end",
                format!("job '{id}' was started again before an end event"),
                Some(id),
            );
        }
        Ok(())
    }

    fn record_end_value_locked(&self, state: &mut ProfilerState, value: Value) -> Result<(), String> {
        let wire: JobEndWire = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        let id = wire.id;
        let Some(mut active) = state.active.remove(&id) else {
            Self::push_diag_locked(
                &self.cfg,
                state,
                "warn",
                "job_end_without_begin",
                format!("job '{id}' ended without a matching begin event"),
                Some(id),
            );
            return Ok(());
        };

        let elapsed = active.started_at.elapsed();
        let elapsed_ms = duration_ms(elapsed);
        let status = wire.status.unwrap_or_else(|| {
            if wire.error.is_some() { "failed".to_owned() } else { "completed".to_owned() }
        });
        active.record.status = status;
        if let Some(detail) = wire.detail {
            active.record.detail = detail;
        }
        active.record.ended_unix_ms = Some(unix_ms());
        active.record.elapsed_ms = Some(elapsed_ms);
        active.record.load = Some(elapsed_ms / active.record.budget_ms.max(0.001));
        active.record.output_bytes = wire.output_bytes;
        active.record.error = wire.error;
        if let Some(extra) = wire.metadata {
            active.record.metadata = merge_metadata(active.record.metadata, extra);
        } else {
            active.record.metadata = merge_metadata(active.record.metadata, value);
        }

        self.complete_job_locked(state, active.record);
        Ok(())
    }

    fn record_status_value_locked(&self, state: &mut ProfilerState, value: Value) -> Result<(), String> {
        let wire: JobStatusWire = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        let phase = wire.phase.as_deref().unwrap_or("running").to_ascii_lowercase();
        let category = wire.kind.unwrap_or_else(|| "task_status".to_owned());
        let budget = wire.budget_ms.unwrap_or_else(|| self.default_budget_for("task_status"));
        let progress = match (wire.current, wire.total) {
            (Some(current), Some(total)) if total != 0 => Some((current as f64 / total as f64).clamp(0.0, 1.0)),
            _ => None,
        };

        if matches!(phase.as_str(), "completed" | "failed" | "cancelled") {
            let end_payload = json!({
                "id": wire.id,
                "status": phase.clone(),
                "detail": wire.detail.unwrap_or_default(),
                "metadata": wire.metadata.unwrap_or(value),
            });
            self.record_end_value_locked(state, end_payload)?;
            return Ok(());
        }

        if let Some(active) = state.active.get_mut(&wire.id) {
            active.record.status = phase;
            if let Some(label) = wire.label {
                active.record.name = label;
            }
            if let Some(detail) = wire.detail {
                active.record.detail = detail;
            }
            active.record.progress = progress;
            active.record.budget_ms = budget.max(0.001);
            if let Some(extra) = wire.metadata {
                active.record.metadata = merge_metadata(active.record.metadata.clone(), extra);
            }
            return Ok(());
        }

        let begin = JobBeginWire {
            id: Some(wire.id),
            name: wire.label,
            label: None,
            category: Some(category),
            source: Some("task_status".to_owned()),
            detail: wire.detail,
            budget_ms: Some(budget),
            payload_bytes: None,
            metadata: wire.metadata.or(Some(value)),
        };
        let begin_value = serde_json::to_value(begin_to_json(begin)).map_err(|e| e.to_string())?;
        self.record_begin_value_locked(state, begin_value)?;
        Ok(())
    }

    fn record_custom_event_locked(&self, state: &mut ProfilerState, topic: &str, value: Value) {
        let id = value
            .get("id")
            .or_else(|| value.get("task_id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| state.local_id());
        let category = sanitize_non_empty(
            value.get("category").and_then(Value::as_str),
            "event",
        );
        let source = sanitize_non_empty(
            value.get("source").and_then(Value::as_str),
            "event_bus",
        );
        let name = sanitize_non_empty(
            value.get("name")
                .or_else(|| value.get("label"))
                .and_then(Value::as_str),
            topic,
        );
        let detail = value
            .get("detail")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("event observed")
            .to_owned();
        let elapsed_ms = value
            .get("elapsed_ms")
            .or_else(|| value.get("duration_ms"))
            .or_else(|| value.get("total_ms"))
            .and_then(Value::as_f64)
            .map(|value| value.max(0.0));
        let budget = value
            .get("budget_ms")
            .and_then(Value::as_f64)
            .unwrap_or_else(|| self.default_budget_for("custom_event"))
            .max(0.001);
        let load = elapsed_ms.map(|elapsed| elapsed / budget);
        let now = unix_ms();
        let started_unix_ms = elapsed_ms
            .map(|elapsed| now.saturating_sub(elapsed.round().max(0.0) as u128))
            .unwrap_or(now);

        let mut record = JobRecord {
            id,
            name,
            category,
            source,
            status: "completed".to_owned(),
            detail,
            started_unix_ms,
            ended_unix_ms: Some(now),
            elapsed_ms: Some(elapsed_ms.unwrap_or(0.0)),
            budget_ms: budget,
            load: Some(load.unwrap_or(0.0)),
            progress: None,
            payload_bytes: None,
            output_bytes: None,
            error: None,
            metadata: value,
        };
        trim_payload_preview(&mut record.metadata, self.cfg.diagnostics.max_payload_preview_bytes);
        self.complete_job_locked(state, record);
    }

    fn complete_job_locked(&self, state: &mut ProfilerState, mut record: JobRecord) {
        trim_payload_preview(&mut record.metadata, self.cfg.diagnostics.max_payload_preview_bytes);

        let elapsed = record.elapsed_ms.unwrap_or_default();
        if elapsed >= self.cfg.diagnostics.slow_job_warn_ms || record.load.unwrap_or_default() >= 1.0 {
            Self::push_diag_locked(
                &self.cfg,
                state,
                "warn",
                "slow_or_over_budget_job",
                format!(
                    "job '{}' category='{}' elapsed_ms={:.3} budget_ms={:.3} load={:.2}",
                    record.name,
                    record.category,
                    elapsed,
                    record.budget_ms,
                    record.load.unwrap_or_default()
                ),
                Some(record.id.clone()),
            );
        }

        if record.status == "failed" {
            Self::push_diag_locked(
                &self.cfg,
                state,
                "error",
                "failed_job",
                format!("job '{}' failed: {}", record.name, record.error.as_deref().unwrap_or("<no error payload>")),
                Some(record.id.clone()),
            );
        }

        state.completed.push_back(record);
        while state.completed.len() > self.cfg.diagnostics.max_recent_jobs {
            state.completed.pop_front();
        }
    }

    pub(crate) fn snapshot(&self) -> Value {
        let state = self.lock_state();
        self.snapshot_locked(&state)
    }

    pub(crate) fn diagnostics(&self) -> Value {
        let state = self.lock_state();
        self.diagnostics_locked(&state)
    }

    pub(crate) fn snapshot_locked(&self, state: &ProfilerState) -> Value {
        let active_jobs: Vec<&JobRecord> = state.active.values().map(|j| &j.record).collect();
        let recent_completed: Vec<&JobRecord> = state.completed.iter().rev().take(256).collect();
        let diagnostics: Vec<&ProfilerDiagnostic> = state.diagnostics.iter().rev().take(256).collect();
        json!({
            "schema": "newengine.profiler.snapshot.v1",
            "service_id": PROFILER_SERVICE_ID,
            "gateway": ENGINE_PROFILER_GATEWAY_ID,
            "enabled": self.cfg.enabled,
            "run_started_unix_ms": state.run_started_unix_ms,
            "uptime_ms": duration_ms(state.run_started.elapsed()),
            "events_seen": state.events_seen,
            "malformed_events": state.malformed_events,
            "active_count": state.active.len(),
            "completed_count": state.completed.len(),
            "reports_written": state.reports_written,
            "active_jobs": active_jobs,
            "recent_completed": recent_completed,
            "diagnostics": diagnostics,
            "last_report_paths": state.last_report_paths.clone(),
        })
    }

    pub(crate) fn diagnostics_locked(&self, state: &ProfilerState) -> Value {
        let stale: Vec<Value> = state
            .active
            .values()
            .filter_map(|job| {
                let elapsed_ms = duration_ms(job.started_at.elapsed());
                if elapsed_ms >= self.cfg.diagnostics.stale_active_job_ms {
                    Some(json!({
                        "id": job.record.id.clone(),
                        "name": job.record.name.clone(),
                        "category": job.record.category.clone(),
                        "elapsed_ms": elapsed_ms,
                        "budget_ms": job.record.budget_ms,
                        "load": elapsed_ms / job.record.budget_ms.max(0.001),
                    }))
                } else {
                    None
                }
            })
            .collect();

        let failed_jobs = state.completed.iter().filter(|job| job.status == "failed").count();
        let slow_or_over_budget_jobs = state
            .completed
            .iter()
            .filter(|job| {
                job.elapsed_ms.unwrap_or_default() >= self.cfg.diagnostics.slow_job_warn_ms
                    || job.load.unwrap_or_default() >= 1.0
            })
            .count();

        let status = if state.malformed_events > 0 || !stale.is_empty() || failed_jobs > 0 {
            "warn"
        } else {
            "ok"
        };

        json!({
            "schema": "newengine.profiler.diagnostics.v1",
            "status": status,
            "enabled": self.cfg.enabled,
            "active_jobs": state.active.len(),
            "completed_jobs_kept": state.completed.len(),
            "failed_jobs": failed_jobs,
            "slow_or_over_budget_jobs": slow_or_over_budget_jobs,
            "events_seen": state.events_seen,
            "malformed_events": state.malformed_events,
            "stale_active_jobs": stale,
            "report_directory": self.cfg.report.directory.clone(),
            "recent_diagnostics": state.diagnostics.iter().rev().take(512).collect::<Vec<_>>(),
        })
    }

    fn default_budget_for(&self, category: &str) -> f64 {
        match category {
            "service_call" => self.cfg.budgets.service_call_ms,
            "plugin_lifecycle" => self.cfg.budgets.plugin_lifecycle_ms,
            "task_status" => self.cfg.budgets.task_status_ms,
            _ => self.cfg.budgets.custom_job_ms,
        }
        .max(0.001)
    }

    pub(crate) fn push_diag_locked(
        cfg: &ProfilerConfig,
        state: &mut ProfilerState,
        level: &str,
        code: &str,
        message: String,
        job_id: Option<String>,
    ) {
        state.diagnostics.push_back(ProfilerDiagnostic {
            level: level.to_owned(),
            code: code.to_owned(),
            message,
            job_id,
            at_unix_ms: unix_ms(),
        });
        while state.diagnostics.len() > cfg.diagnostics.max_recent_diagnostics {
            state.diagnostics.pop_front();
        }
    }

    pub(crate) fn lock_state(&self) -> std::sync::MutexGuard<'_, ProfilerState> {
        match self.state.lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        }
    }
}
