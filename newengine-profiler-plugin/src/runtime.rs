use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::config::ProfilerConfig;
use crate::constants::{
    ENGINE_JOBS_GATEWAY_ID, ENGINE_PROFILER_GATEWAY_ID, JOBS_INVOKE_SERVICE_V1,
    METHOD_FLUSH_REPORT_SYNC_V1, PROFILER_PLUGIN_ID, PROFILER_SERVICE_ID, TOPIC_ENGINE_JOB_EVENT,
    TOPIC_ENGINE_TASK_EVENT, TOPIC_JOB_BEGIN, TOPIC_JOB_END, TOPIC_JOB_STATUS,
};
use crate::records::{
    ActiveJob, FlushRequestRecord, JobBeginWire, JobEndWire, JobRecord, JobStatusWire,
    ProfilerDiagnostic, ProfilerState,
};
use crate::scheduler::HostJobScheduler;
use crate::util::{
    begin_to_json, duration_ms, merge_metadata, sanitize_non_empty, trim_payload_preview, unix_ms,
};

pub(crate) static RUNTIME: OnceLock<Arc<ProfilerRuntime>> = OnceLock::new();

fn event_elapsed_ms(value: &Value) -> Option<f64> {
    const PATHS: &[&str] = &[
        "/elapsed_ms",
        "/duration_ms",
        "/total_ms",
        "/metadata/elapsed_ms",
        "/metadata/duration_ms",
        "/metadata/total_ms",
    ];
    for path in PATHS {
        if let Some(ms) = value.pointer(path).and_then(value_to_f64_ms) {
            return Some(ms.max(0.0));
        }
    }
    value
        .get("detail")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .and_then(parse_first_ms_from_text)
        .map(|ms| ms.max(0.0))
}

fn value_to_f64_ms(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse::<f64>().ok()))
}

fn parse_first_ms_from_text(text: &str) -> Option<f64> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    for window in parts.windows(2) {
        let unit = window[1].trim_matches(|c: char| !c.is_ascii_alphabetic()).to_ascii_lowercase();
        if unit == "ms" || unit == "msec" || unit == "millisecond" || unit == "milliseconds" {
            let number = window[0].trim_matches(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-'));
            if let Ok(value) = number.parse::<f64>() {
                return Some(value);
            }
        }
    }
    None
}

fn parse_breakdown_parts(breakdown: &str) -> Vec<(String, f64)> {
    let mut out = Vec::new();
    for token in breakdown.split_whitespace() {
        let Some((name, raw_ms)) = token.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let raw_ms = raw_ms.trim().strip_suffix("ms").unwrap_or(raw_ms.trim());
        let Ok(elapsed_ms) = raw_ms.parse::<f64>() else {
            continue;
        };
        out.push((name.to_owned(), elapsed_ms.max(0.0)));
    }
    out
}

fn engine_job_envelope_to_profiler_event(envelope: Value) -> Value {
    let Some(mut event) = envelope.get("event").cloned() else {
        return envelope;
    };
    let Value::Object(event_obj) = &mut event else {
        return event;
    };

    let envelope_meta = json!({
        "schema": envelope.get("schema").cloned(),
        "authority": envelope.get("authority").cloned(),
        "executor": envelope.get("executor").cloned(),
        "semantic_owner": envelope.get("semantic_owner").cloned(),
    });
    event_obj.insert("job_envelope".to_owned(), envelope_meta);
    if event_obj.get("owner").is_none() {
        if let Some(owner) = envelope.get("semantic_owner").cloned() {
            event_obj.insert("owner".to_owned(), owner);
        }
    }
    event
}

fn refresh_record_classification(record: &mut JobRecord) {
    if let Some(v) = first_value_str(&record.metadata, &["/lane", "/metadata/lane", "/event/lane"]) {
        record.lane = sanitize_non_empty(Some(v.as_str()), record.lane.as_str());
    }
    if let Some(v) = first_value_str(&record.metadata, &["/priority", "/metadata/priority", "/event/priority"]) {
        record.priority = sanitize_non_empty(Some(v.as_str()), record.priority.as_str());
    }
    if let Some(v) = first_value_str(&record.metadata, &["/dependency_group", "/dependencyGroup", "/metadata/dependency_group", "/metadata/dependencyGroup"]) {
        record.dependency_group = sanitize_non_empty(Some(v.as_str()), record.dependency_group.as_str());
    }
    record.frame_id = record.frame_id.or_else(|| first_value_u64(&record.metadata, &[
        "/frame_id", "/frame", "/frame_index", "/metadata/frame_id", "/metadata/frame", "/metadata/frame_index",
    ]));
    record.frame_budget_ms = record.frame_budget_ms.or_else(|| first_value_f64(&record.metadata, &[
        "/frame_budget_ms", "/budget/frame_ms", "/metadata/frame_budget_ms", "/metadata/budget/frame_ms",
    ]));
    record.gpu_wait_ms = record.gpu_wait_ms.or_else(|| first_value_f64(&record.metadata, &[
        "/gpu_wait_ms", "/waited_gpu_ms", "/metadata/gpu_wait_ms", "/metadata/waited_gpu_ms",
    ]));
    if record.wait_reason.as_deref().map(str::trim).unwrap_or_default().is_empty() {
        record.wait_reason = first_value_str(&record.metadata, &[
            "/wait_reason", "/blocked_reason", "/block_reason", "/metadata/wait_reason", "/metadata/blocked_reason", "/metadata/block_reason",
        ]);
    }
    if record.async_mode.as_deref().map(str::trim).unwrap_or_default().is_empty() {
        record.async_mode = first_value_str(&record.metadata, &[
            "/async_mode", "/scheduling_mode", "/metadata/async_mode", "/metadata/scheduling_mode",
        ]);
    }

    let metadata_phase = first_value_str(&record.metadata, &["/phase", "/state_label", "/status", "/metadata/phase", "/metadata/state_label", "/metadata/status"]);
    let phase = lower_join(&[
        Some(record.status.as_str()),
        Some(record.detail.as_str()),
        metadata_phase.as_deref(),
    ]);
    let category = record.category.to_ascii_lowercase();
    let wait_reason = record.wait_reason.as_deref().unwrap_or_default().to_ascii_lowercase();
    let async_mode = record.async_mode.as_deref().unwrap_or_default().to_ascii_lowercase();

    record.scheduled |= contains_any(&phase, &["scheduled", "queued", "queue", "accepted"]);
    record.blocked |= contains_any(&phase, &["blocked", "waiting", "stalled"]) || contains_any(&wait_reason, &["blocked", "waiting", "dependency", "residency", "barrier"]);
    record.polling |= contains_any(&phase, &["poll", "polling"]) || contains_any(&category, &["poll"]);
    record.waited_on_gpu |= record.gpu_wait_ms.unwrap_or_default() > 0.0 || contains_any(&wait_reason, &["gpu", "fence", "present", "upload", "queue"]);
    record.stayed_async |= first_value_bool(&record.metadata, &["/stayed_async", "/async", "/metadata/stayed_async", "/metadata/async"]).unwrap_or(false)
        || contains_any(&async_mode, &["async", "engine_jobs", "job", "ticket", "poll"])
        || contains_any(&category, &["shader.compile", "asset.decode", "texture.upload", "streaming", "residency", "renderprep", "render-prep"]);

    if let (Some(elapsed), Some(frame_budget)) = (record.elapsed_ms, record.frame_budget_ms) {
        record.exceeded_frame_budget |= elapsed > frame_budget.max(0.001);
    }
    record.exceeded_frame_budget |= first_value_bool(&record.metadata, &[
        "/exceeded_frame_budget", "/frame_budget_exceeded", "/metadata/exceeded_frame_budget", "/metadata/frame_budget_exceeded",
    ]).unwrap_or(false);
}

fn first_value_str(value: &Value, paths: &[&str]) -> Option<String> {
    paths
        .iter()
        .filter_map(|path| value.pointer(path).and_then(Value::as_str))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .next()
}

fn first_value_u64(value: &Value, paths: &[&str]) -> Option<u64> {
    paths.iter().find_map(|path| {
        value.pointer(path).and_then(|v| v.as_u64().or_else(|| v.as_str()?.trim().parse::<u64>().ok()))
    })
}

fn first_value_f64(value: &Value, paths: &[&str]) -> Option<f64> {
    paths.iter().find_map(|path| value.pointer(path).and_then(value_to_f64_ms))
}

fn first_value_bool(value: &Value, paths: &[&str]) -> Option<bool> {
    paths.iter().find_map(|path| {
        value.pointer(path).and_then(|v| {
            v.as_bool().or_else(|| {
                let raw = v.as_str()?.trim().to_ascii_lowercase();
                match raw.as_str() {
                    "1" | "true" | "yes" | "y" | "on" => Some(true),
                    "0" | "false" | "no" | "n" | "off" => Some(false),
                    _ => None,
                }
            })
        })
    })
}

fn lower_join(parts: &[Option<&str>]) -> String {
    parts.iter().filter_map(|v| *v).collect::<Vec<_>>().join(" ").to_ascii_lowercase()
}

fn is_high_frequency_zero_cost_event(category: &str, source: &str, name: &str, elapsed_ms: Option<f64>) -> bool {
    if elapsed_ms.unwrap_or(0.0) > 0.0 {
        return false;
    }
    let category = category.to_ascii_lowercase();
    let source = source.to_ascii_lowercase();
    let name = name.to_ascii_lowercase();
    category == "event"
        && (source == "raw-device" || source == "event_bus" || source == "cursor")
        && (name.starts_with("winit.mouse_")
            || name == "winit.mouse_delta"
            || name == "winit.mouse_move"
            || name == "winit.key"
            || name == "winit.text_char")
}

fn recently_completed_duplicate_terminal(state: &ProfilerState, id: &str) -> bool {
    state.completed.iter().rev().take(256).any(|record| record.id == id)
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

pub(crate) struct ProfilerRuntime {
    pub(crate) cfg: ProfilerConfig,
    scheduler: Option<HostJobScheduler>,
    state: Mutex<ProfilerState>,
}

impl ProfilerRuntime {
    pub(crate) fn new(cfg: ProfilerConfig, scheduler: Option<HostJobScheduler>) -> Self {
        Self {
            cfg,
            scheduler,
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
            TOPIC_ENGINE_TASK_EVENT => {
                if let Err(e) = self.record_engine_task_event_locked(&mut state, parsed) {
                    state.malformed_events = state.malformed_events.saturating_add(1);
                    Self::push_diag_locked(
                        &self.cfg,
                        &mut state,
                        "warn",
                        "bad_engine_task_event",
                        format!("bad engine task event: {e}"),
                        None,
                    );
                }
            }
            TOPIC_ENGINE_JOB_EVENT => {
                let event = engine_job_envelope_to_profiler_event(parsed);
                if let Err(e) = self.record_engine_task_event_locked(&mut state, event) {
                    state.malformed_events = state.malformed_events.saturating_add(1);
                    Self::push_diag_locked(
                        &self.cfg,
                        &mut state,
                        "warn",
                        "bad_engine_job_event",
                        format!("bad engine job event: {e}"),
                        None,
                    );
                }
            }
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

        let mut record = JobRecord {
            id: id.clone(),
            name,
            category,
            source: wire.source.unwrap_or_else(|| "event".to_owned()),
            lane: sanitize_non_empty(wire.lane.as_deref(), "unspecified"),
            priority: sanitize_non_empty(wire.priority.as_deref(), "unspecified"),
            dependency_group: sanitize_non_empty(wire.dependency_group.as_deref(), "unspecified"),
            frame_id: wire.frame_id,
            status: "running".to_owned(),
            detail: wire.detail.unwrap_or_default(),
            scheduled: true,
            blocked: false,
            polling: false,
            waited_on_gpu: false,
            stayed_async: false,
            exceeded_frame_budget: false,
            frame_budget_ms: wire.frame_budget_ms,
            gpu_wait_ms: wire.gpu_wait_ms,
            wait_reason: wire.wait_reason,
            async_mode: wire.async_mode,
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
        refresh_record_classification(&mut record);

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
            if recently_completed_duplicate_terminal(state, &id) {
                return Ok(());
            }
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
        if let Some(v) = wire.lane.as_deref() {
            active.record.lane = sanitize_non_empty(Some(v), active.record.lane.as_str());
        }
        if let Some(v) = wire.priority.as_deref() {
            active.record.priority = sanitize_non_empty(Some(v), active.record.priority.as_str());
        }
        if let Some(v) = wire.dependency_group.as_deref() {
            active.record.dependency_group = sanitize_non_empty(Some(v), active.record.dependency_group.as_str());
        }
        active.record.frame_id = wire.frame_id.or(active.record.frame_id);
        active.record.frame_budget_ms = wire.frame_budget_ms.or(active.record.frame_budget_ms);
        active.record.gpu_wait_ms = wire.gpu_wait_ms.or(active.record.gpu_wait_ms);
        active.record.wait_reason = wire.wait_reason.or_else(|| active.record.wait_reason.clone());
        active.record.async_mode = wire.async_mode.or_else(|| active.record.async_mode.clone());
        refresh_record_classification(&mut active.record);

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
                "lane": wire.lane,
                "priority": wire.priority,
                "dependency_group": wire.dependency_group,
                "frame_id": wire.frame_id,
                "frame_budget_ms": wire.frame_budget_ms,
                "gpu_wait_ms": wire.gpu_wait_ms,
                "wait_reason": wire.wait_reason,
                "async_mode": wire.async_mode,
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
            if let Some(v) = wire.lane.as_deref() {
                active.record.lane = sanitize_non_empty(Some(v), active.record.lane.as_str());
            }
            if let Some(v) = wire.priority.as_deref() {
                active.record.priority = sanitize_non_empty(Some(v), active.record.priority.as_str());
            }
            if let Some(v) = wire.dependency_group.as_deref() {
                active.record.dependency_group = sanitize_non_empty(Some(v), active.record.dependency_group.as_str());
            }
            active.record.frame_id = wire.frame_id.or(active.record.frame_id);
            active.record.frame_budget_ms = wire.frame_budget_ms.or(active.record.frame_budget_ms);
            active.record.gpu_wait_ms = wire.gpu_wait_ms.or(active.record.gpu_wait_ms);
            active.record.wait_reason = wire.wait_reason.or(active.record.wait_reason.clone());
            active.record.async_mode = wire.async_mode.or(active.record.async_mode.clone());
            if let Some(extra) = wire.metadata {
                active.record.metadata = merge_metadata(active.record.metadata.clone(), extra);
            }
            refresh_record_classification(&mut active.record);
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
            lane: wire.lane,
            priority: wire.priority,
            dependency_group: wire.dependency_group,
            frame_id: wire.frame_id,
            frame_budget_ms: wire.frame_budget_ms,
            gpu_wait_ms: wire.gpu_wait_ms,
            wait_reason: wire.wait_reason,
            async_mode: wire.async_mode,
        };
        let begin_value = serde_json::to_value(begin_to_json(begin)).map_err(|e| e.to_string())?;
        self.record_begin_value_locked(state, begin_value)?;
        Ok(())
    }

    fn record_engine_task_event_locked(&self, state: &mut ProfilerState, value: Value) -> Result<(), String> {
        let id = value
            .get("task_id")
            .or_else(|| value.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "engine task event has no task_id".to_owned())?;
        let phase = value
            .get("phase")
            .and_then(Value::as_str)
            .unwrap_or("Running")
            .to_ascii_lowercase();
        let category = sanitize_non_empty(value.get("category").and_then(Value::as_str), "engine_task");
        let source = sanitize_non_empty(value.get("source").and_then(Value::as_str), "engine.task.event");
        let label = sanitize_non_empty(
            value.get("name").and_then(Value::as_str),
            id.as_str(),
        );
        let detail = value
            .get("detail")
            .or_else(|| value.get("status"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let progress = value.get("progress_01").and_then(Value::as_f64);
        let budget = value
            .get("budget_ms")
            .and_then(Value::as_f64)
            .unwrap_or_else(|| self.default_budget_for(category.as_str()))
            .max(0.001);

        if matches!(phase.as_str(), "completed" | "failed" | "cancelled" | "canceled") {
            let status = if phase == "cancelled" || phase == "canceled" {
                "cancelled"
            } else {
                phase.as_str()
            };
            let end_payload = json!({
                "id": id,
                "status": status,
                "detail": detail,
                "metadata": value,
            });
            self.record_end_value_locked(state, end_payload)?;
            return Ok(());
        }

        if let Some(active) = state.active.get_mut(&id) {
            active.record.status = phase;
            active.record.name = label;
            active.record.detail = detail;
            active.record.progress = progress;
            active.record.budget_ms = budget;
            active.record.metadata = merge_metadata(active.record.metadata.clone(), value);
            refresh_record_classification(&mut active.record);
            return Ok(());
        }

        let begin = json!({
            "id": id,
            "name": label,
            "category": category,
            "source": source,
            "detail": detail,
            "budget_ms": budget,
            "lane": value.get("lane").cloned(),
            "priority": value.get("priority").cloned(),
            "dependency_group": value.get("dependency_group").cloned(),
            "frame_id": value.get("frame_id").cloned(),
            "frame_budget_ms": value.get("frame_budget_ms").cloned(),
            "gpu_wait_ms": value.get("gpu_wait_ms").cloned(),
            "wait_reason": value.get("wait_reason").cloned().or_else(|| value.get("blocked_reason").cloned()),
            "async_mode": value.get("async_mode").cloned(),
            "metadata": value,
        });
        self.record_begin_value_locked(state, begin)
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
        let elapsed_ms = event_elapsed_ms(&value);
        let budget = value
            .get("budget_ms")
            .and_then(Value::as_f64)
            .unwrap_or_else(|| self.default_budget_for("custom_event"))
            .max(0.001);
        if is_high_frequency_zero_cost_event(&category, &source, &name, elapsed_ms) {
            return;
        }
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
            lane: "unspecified".to_owned(),
            priority: "unspecified".to_owned(),
            dependency_group: "unspecified".to_owned(),
            frame_id: None,
            status: "completed".to_owned(),
            detail,
            scheduled: false,
            blocked: false,
            polling: false,
            waited_on_gpu: false,
            stayed_async: false,
            exceeded_frame_budget: false,
            frame_budget_ms: None,
            gpu_wait_ms: None,
            wait_reason: None,
            async_mode: None,
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
        refresh_record_classification(&mut record);
        self.complete_job_locked(state, record.clone());
        self.record_breakdown_parts_locked(state, &record);
    }

    fn record_breakdown_parts_locked(&self, state: &mut ProfilerState, parent: &JobRecord) {
        let Some(breakdown) = parent
            .metadata
            .get("breakdown")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return;
        };

        for (idx, (part_name, elapsed_ms)) in parse_breakdown_parts(breakdown).into_iter().enumerate() {
            let budget_ms = parent.budget_ms.max(0.001);
            let now = unix_ms();
            let id = format!("{}::part::{}", parent.id, idx + 1);
            let mut metadata = json!({
                "schema": "newengine.profiler.breakdown_part.v1",
                "parent_id": parent.id.clone(),
                "parent_name": parent.name.clone(),
                "part": part_name.clone(),
                "elapsed_ms": elapsed_ms,
                "source_event": parent.metadata.clone(),
            });
            trim_payload_preview(&mut metadata, self.cfg.diagnostics.max_payload_preview_bytes);
            let mut part_record = JobRecord {
                id,
                name: format!("{}/{}", parent.name, part_name),
                category: format!("{}.breakdown", parent.category),
                source: parent.source.clone(),
                lane: parent.lane.clone(),
                priority: parent.priority.clone(),
                dependency_group: parent.dependency_group.clone(),
                frame_id: parent.frame_id,
                status: "completed".to_owned(),
                detail: format!("breakdown part from '{}'", parent.name),
                scheduled: parent.scheduled,
                blocked: parent.blocked,
                polling: parent.polling,
                waited_on_gpu: parent.waited_on_gpu,
                stayed_async: parent.stayed_async,
                exceeded_frame_budget: false,
                frame_budget_ms: parent.frame_budget_ms,
                gpu_wait_ms: None,
                wait_reason: parent.wait_reason.clone(),
                async_mode: parent.async_mode.clone(),
                started_unix_ms: now.saturating_sub(elapsed_ms.round().max(0.0) as u128),
                ended_unix_ms: Some(now),
                elapsed_ms: Some(elapsed_ms),
                budget_ms,
                load: Some(elapsed_ms / budget_ms),
                progress: None,
                payload_bytes: None,
                output_bytes: None,
                error: None,
                metadata,
            };
            refresh_record_classification(&mut part_record);
            self.complete_job_locked(state, part_record);
        }
    }

    fn complete_job_locked(&self, state: &mut ProfilerState, mut record: JobRecord) {
        refresh_record_classification(&mut record);
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


    pub(crate) fn flush_report_service(&self, reason: &str) -> Result<Value, String> {
        if self.cfg.scheduling.service_flush_mode.eq_ignore_ascii_case("sync") {
            self.flush_report(reason)
        } else {
            self.flush_report_async(reason)
        }
    }

    pub(crate) fn flush_report_async(&self, reason: &str) -> Result<Value, String> {
        let (request_id, job_id, requested_unix_ms) = {
            let mut state = self.lock_state();
            let request_id = state.local_id();
            let job_id = format!("{request_id}.engine-jobs-flush");
            (request_id, job_id, unix_ms())
        };

        let job_reason = format!("{reason}.engine_jobs");
        let job_request = json!({
            "schema": "newengine.jobs.service_call.request.v1",
            "job_id": job_id.clone(),
            "name": "North Star Profiler report flush",
            "owner": PROFILER_SERVICE_ID,
            "category": "profiler.report.flush",
            "lane": "plugin",
            "priority": "background",
            "can_pause": false,
            "can_cancel": true,
            "target": {
                "gateway": ENGINE_PROFILER_GATEWAY_ID,
                "method": METHOD_FLUSH_REPORT_SYNC_V1,
                "payload_json": {
                    "schema": "newengine.profiler.flush_report.request.v1",
                    "reason": job_reason,
                    "request_id": request_id.clone()
                }
            }
        });

        self.record_flush_request(FlushRequestRecord {
            request_id: request_id.clone(),
            job_id: job_id.clone(),
            reason: reason.to_owned(),
            scheduling_mode: format!("{ENGINE_JOBS_GATEWAY_ID}/{JOBS_INVOKE_SERVICE_V1}"),
            status: "scheduling".to_owned(),
            requested_unix_ms,
            completed_unix_ms: None,
            engine_jobs_response: None,
            error: None,
        });

        if self.cfg.scheduling.prefer_engine_jobs {
            if let Some(scheduler) = self.scheduler {
                match scheduler.invoke_service_job(job_request.clone()) {
                    Ok(response) => {
                        let accepted = response.get("accepted").and_then(Value::as_bool).unwrap_or(true);
                        if accepted {
                            self.record_flush_request(FlushRequestRecord {
                                request_id: request_id.clone(),
                                job_id: job_id.clone(),
                                reason: reason.to_owned(),
                                scheduling_mode: format!("{ENGINE_JOBS_GATEWAY_ID}/{JOBS_INVOKE_SERVICE_V1}"),
                                status: "scheduled".to_owned(),
                                requested_unix_ms,
                                completed_unix_ms: None,
                                engine_jobs_response: Some(response.clone()),
                                error: None,
                            });
                        } else {
                            let error = response
                                .get("detail")
                                .and_then(Value::as_str)
                                .unwrap_or("engine.jobs rejected profiler service-call job")
                                .to_owned();
                            self.record_flush_request(FlushRequestRecord {
                                request_id: request_id.clone(),
                                job_id: job_id.clone(),
                                reason: reason.to_owned(),
                                scheduling_mode: format!("{ENGINE_JOBS_GATEWAY_ID}/{JOBS_INVOKE_SERVICE_V1}"),
                                status: "rejected".to_owned(),
                                requested_unix_ms,
                                completed_unix_ms: Some(unix_ms()),
                                engine_jobs_response: Some(response.clone()),
                                error: Some(error),
                            });
                        }
                        return Ok(json!({
                            "schema": "newengine.profiler.flush_report.async_result.v1",
                            "accepted": accepted,
                            "mode": "engine_jobs",
                            "engine_jobs_gateway": ENGINE_JOBS_GATEWAY_ID,
                            "engine_jobs_method": JOBS_INVOKE_SERVICE_V1,
                            "request_id": request_id,
                            "job_id": job_id,
                            "response": response,
                        }));
                    }
                    Err(e) => {
                        self.record_flush_request(FlushRequestRecord {
                            request_id: request_id.clone(),
                            job_id: job_id.clone(),
                            reason: reason.to_owned(),
                            scheduling_mode: format!("{ENGINE_JOBS_GATEWAY_ID}/{JOBS_INVOKE_SERVICE_V1}"),
                            status: "rejected".to_owned(),
                            requested_unix_ms,
                            completed_unix_ms: Some(unix_ms()),
                            engine_jobs_response: None,
                            error: Some(e.clone()),
                        });
                        return Ok(json!({
                            "schema": "newengine.profiler.flush_report.async_result.v1",
                            "accepted": false,
                            "mode": "engine_jobs_required",
                            "engine_jobs_gateway": ENGINE_JOBS_GATEWAY_ID,
                            "engine_jobs_method": JOBS_INVOKE_SERVICE_V1,
                            "request_id": request_id,
                            "job_id": job_id,
                            "error": e,
                        }));
                    }
                }
            } else if self.cfg.scheduling.require_engine_jobs {
                let error = "engine.jobs scheduler is unavailable; profiler-owned background fallback is not allowed".to_owned();
                self.record_flush_request(FlushRequestRecord {
                    request_id: request_id.clone(),
                    job_id: job_id.clone(),
                    reason: reason.to_owned(),
                    scheduling_mode: "engine_jobs_unavailable".to_owned(),
                    status: "rejected".to_owned(),
                    requested_unix_ms,
                    completed_unix_ms: Some(unix_ms()),
                    engine_jobs_response: None,
                    error: Some(error.clone()),
                });
                return Ok(json!({
                    "schema": "newengine.profiler.flush_report.async_result.v1",
                    "accepted": false,
                    "mode": "engine_jobs_required",
                    "engine_jobs_gateway": ENGINE_JOBS_GATEWAY_ID,
                    "engine_jobs_method": JOBS_INVOKE_SERVICE_V1,
                    "request_id": request_id,
                    "job_id": job_id,
                    "error": error,
                }));
            }
        }

        let error = "async profiler flush requires engine.jobs; no profiler-owned background fallback is allowed".to_owned();
        self.record_flush_request(FlushRequestRecord {
            request_id: request_id.clone(),
            job_id: job_id.clone(),
            reason: reason.to_owned(),
            scheduling_mode: "engine_jobs_required".to_owned(),
            status: "rejected".to_owned(),
            requested_unix_ms,
            completed_unix_ms: Some(unix_ms()),
            engine_jobs_response: None,
            error: Some(error.clone()),
        });
        Ok(json!({
            "schema": "newengine.profiler.flush_report.async_result.v1",
            "accepted": false,
            "mode": "engine_jobs_required",
            "engine_jobs_gateway": ENGINE_JOBS_GATEWAY_ID,
            "engine_jobs_method": JOBS_INVOKE_SERVICE_V1,
            "request_id": request_id,
            "job_id": job_id,
            "error": error,
        }))
    }

    pub(crate) fn flush_status(&self) -> Value {
        let state = self.lock_state();
        json!({
            "schema": "newengine.profiler.flush_status.v1",
            "reports_written": state.reports_written,
            "reports_in_progress": state.reports_in_progress,
            "reports_scheduled": state.reports_scheduled,
            "reports_failed": state.reports_failed,
            "last_report_paths": state.last_report_paths.clone(),
            "recent_flush_requests": state.flush_requests.iter().rev().take(128).collect::<Vec<_>>(),
            "scheduling": self.cfg.scheduling.clone(),
        })
    }

    fn record_flush_request(&self, record: FlushRequestRecord) {
        let mut state = self.lock_state();
        let mut should_count_status = true;
        if let Some(existing) = state.flush_requests.iter_mut().rev().find(|it| it.request_id == record.request_id) {
            // A fast engine.jobs worker may finish before the scheduling call returns.
            // In that case, keep the terminal state and only attach the scheduler response.
            if matches!(existing.status.as_str(), "completed" | "failed") && record.status == "scheduled" {
                if existing.engine_jobs_response.is_none() {
                    existing.engine_jobs_response = record.engine_jobs_response;
                }
                existing.scheduling_mode = record.scheduling_mode;
                should_count_status = false;
            } else {
                let old_status = existing.status.clone();
                *existing = record.clone();
                should_count_status = old_status != record.status;
            }
        } else {
            state.flush_requests.push_back(record.clone());
            while state.flush_requests.len() > 256 {
                state.flush_requests.pop_front();
            }
        }

        if should_count_status {
            match record.status.as_str() {
                "scheduled" => state.reports_scheduled = state.reports_scheduled.saturating_add(1),
                "failed" | "rejected" => state.reports_failed = state.reports_failed.saturating_add(1),
                _ => {}
            }
        }
        if let Some(error) = record.error.clone() {
            Self::push_diag_locked(
                &self.cfg,
                &mut state,
                if record.status == "rejected" { "warn" } else { "error" },
                "profiler_flush_schedule_status",
                format!("profiler report flush request '{}' status='{}': {}", record.request_id, record.status, error),
                Some(record.job_id.clone()),
            );
        }
    }


    pub(crate) fn mark_flush_request_completed(&self, request_id: &str, error: Option<String>) {
        let mut state = self.lock_state();
        let mut failed = false;
        if let Some(record) = state.flush_requests.iter_mut().rev().find(|it| it.request_id == request_id) {
            record.completed_unix_ms = Some(unix_ms());
            if let Some(error) = error {
                record.status = "failed".to_owned();
                record.error = Some(error);
                failed = true;
            } else {
                record.status = "completed".to_owned();
            }
        }
        if failed {
            state.reports_failed = state.reports_failed.saturating_add(1);
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
            "reports_in_progress": state.reports_in_progress,
            "reports_scheduled": state.reports_scheduled,
            "reports_failed": state.reports_failed,
            "recent_flush_requests": state.flush_requests.iter().rev().take(64).collect::<Vec<_>>(),
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
            "reports_written": state.reports_written,
            "reports_in_progress": state.reports_in_progress,
            "reports_scheduled": state.reports_scheduled,
            "reports_failed": state.reports_failed,
            "scheduling": self.cfg.scheduling.clone(),
            "recent_flush_requests": state.flush_requests.iter().rev().take(64).collect::<Vec<_>>(),
            "recent_diagnostics": state.diagnostics.iter().rev().take(512).collect::<Vec<_>>(),
        })
    }

    fn default_budget_for(&self, category: &str) -> f64 {
        match category {
            "service_call" => self.cfg.budgets.service_call_ms,
            "plugin_lifecycle" => self.cfg.budgets.plugin_lifecycle_ms,
            "task_status" => self.cfg.budgets.task_status_ms,
            "profiler.report.flush" => self.cfg.scheduling.flush_job_budget_ms,
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
