use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use crate::util::unix_ms;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JobBeginWire {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) label: Option<String>,
    #[serde(default)]
    pub(crate) category: Option<String>,
    #[serde(default)]
    pub(crate) source: Option<String>,
    #[serde(default)]
    pub(crate) detail: Option<String>,
    #[serde(default)]
    pub(crate) budget_ms: Option<f64>,
    #[serde(default)]
    pub(crate) payload_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JobEndWire {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) detail: Option<String>,
    #[serde(default)]
    pub(crate) error: Option<String>,
    #[serde(default)]
    pub(crate) output_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct JobStatusWire {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) phase: Option<String>,
    #[serde(default)]
    pub(crate) label: Option<String>,
    #[serde(default)]
    pub(crate) detail: Option<String>,
    #[serde(default)]
    pub(crate) current: Option<u64>,
    #[serde(default)]
    pub(crate) total: Option<u64>,
    #[serde(default)]
    pub(crate) budget_ms: Option<f64>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProfilerDiagnostic {
    pub(crate) level: String,
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) job_id: Option<String>,
    pub(crate) at_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct JobRecord {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) source: String,
    pub(crate) status: String,
    pub(crate) detail: String,
    pub(crate) started_unix_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ended_unix_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) elapsed_ms: Option<f64>,
    pub(crate) budget_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) load: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) progress: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) output_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    pub(crate) metadata: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveJob {
    pub(crate) record: JobRecord,
    pub(crate) started_at: Instant,
}

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct CategoryStats {
    pub(crate) count: u64,
    pub(crate) failed: u64,
    pub(crate) slow: u64,
    pub(crate) total_elapsed_ms: f64,
    pub(crate) max_elapsed_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReportPaths {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) archive: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) archive_created_unix_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) archive_created_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) archive_manifest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) json_latest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) json_timestamped: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) markdown_latest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) markdown_timestamped: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) csv_latest: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) csv_timestamped: Option<BTreeMap<String, String>>,
}

#[derive(Debug)]
pub(crate) struct ProfilerState {
    pub(crate) run_started: Instant,
    pub(crate) run_started_unix_ms: u128,
    pub(crate) next_local_id: u64,
    pub(crate) events_seen: u64,
    pub(crate) malformed_events: u64,
    pub(crate) active: HashMap<String, ActiveJob>,
    pub(crate) completed: VecDeque<JobRecord>,
    pub(crate) diagnostics: VecDeque<ProfilerDiagnostic>,
    pub(crate) reports_written: u64,
    pub(crate) shutdown_report_written: bool,
    pub(crate) last_report_paths: Option<ReportPaths>,
}

impl ProfilerState {
    pub(crate) fn new() -> Self {
        Self {
            run_started: Instant::now(),
            run_started_unix_ms: unix_ms(),
            next_local_id: 1,
            events_seen: 0,
            malformed_events: 0,
            active: HashMap::new(),
            completed: VecDeque::new(),
            diagnostics: VecDeque::new(),
            reports_written: 0,
            shutdown_report_written: false,
            last_report_paths: None,
        }
    }

    pub(crate) fn local_id(&mut self) -> String {
        let id = self.next_local_id;
        self.next_local_id = self.next_local_id.saturating_add(1);
        format!("profiler-local-{id}")
    }
}

