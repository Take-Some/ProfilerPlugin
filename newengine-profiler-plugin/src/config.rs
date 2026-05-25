use serde::{Deserialize, Serialize};

use crate::constants::DEFAULT_CONFIG_JSON;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ProfilerConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_true")]
    pub(crate) ignore_self: bool,
    #[serde(default)]
    pub(crate) capture: CaptureConfig,
    #[serde(default)]
    pub(crate) budgets: BudgetConfig,
    #[serde(default)]
    pub(crate) diagnostics: DiagnosticsConfig,
    #[serde(default)]
    pub(crate) report: ReportConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct CaptureConfig {
    #[serde(default = "default_true")]
    pub(crate) service_calls: bool,
    #[serde(default = "default_true")]
    pub(crate) plugin_lifecycle: bool,
    #[serde(default = "default_true")]
    pub(crate) task_status_events: bool,
    #[serde(default = "default_true")]
    pub(crate) custom_events: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct BudgetConfig {
    #[serde(default = "default_service_budget_ms")]
    pub(crate) service_call_ms: f64,
    #[serde(default = "default_plugin_lifecycle_budget_ms")]
    pub(crate) plugin_lifecycle_ms: f64,
    #[serde(default = "default_custom_budget_ms")]
    pub(crate) custom_job_ms: f64,
    #[serde(default = "default_custom_budget_ms")]
    pub(crate) task_status_ms: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct DiagnosticsConfig {
    #[serde(default = "default_slow_job_warn_ms")]
    pub(crate) slow_job_warn_ms: f64,
    #[serde(default = "default_stale_active_job_ms")]
    pub(crate) stale_active_job_ms: f64,
    #[serde(default = "default_max_recent_jobs")]
    pub(crate) max_recent_jobs: usize,
    #[serde(default = "default_max_recent_diagnostics")]
    pub(crate) max_recent_diagnostics: usize,
    #[serde(default = "default_max_payload_preview_bytes")]
    pub(crate) max_payload_preview_bytes: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ReportConfig {
    #[serde(default = "default_true")]
    pub(crate) write_on_shutdown: bool,
    #[serde(default = "default_true")]
    pub(crate) write_json: bool,
    #[serde(default = "default_true")]
    pub(crate) write_markdown: bool,
    #[serde(default = "default_true")]
    pub(crate) write_archive: bool,
    #[serde(default = "default_true")]
    pub(crate) include_latest_in_archive: bool,
    #[serde(default = "default_archive_prefix")]
    pub(crate) archive_prefix: String,
    #[serde(default = "default_report_directory")]
    pub(crate) directory: String,
    #[serde(default = "default_latest_json")]
    pub(crate) latest_json: String,
    #[serde(default = "default_latest_markdown")]
    pub(crate) latest_markdown: String,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        serde_json::from_str(DEFAULT_CONFIG_JSON).unwrap_or_else(|_| Self {
            enabled: true,
            ignore_self: true,
            capture: CaptureConfig::default(),
            budgets: BudgetConfig::default(),
            diagnostics: DiagnosticsConfig::default(),
            report: ReportConfig::default(),
        })
    }
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            service_calls: true,
            plugin_lifecycle: true,
            task_status_events: true,
            custom_events: true,
        }
    }
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            service_call_ms: default_service_budget_ms(),
            plugin_lifecycle_ms: default_plugin_lifecycle_budget_ms(),
            custom_job_ms: default_custom_budget_ms(),
            task_status_ms: default_custom_budget_ms(),
        }
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            slow_job_warn_ms: default_slow_job_warn_ms(),
            stale_active_job_ms: default_stale_active_job_ms(),
            max_recent_jobs: default_max_recent_jobs(),
            max_recent_diagnostics: default_max_recent_diagnostics(),
            max_payload_preview_bytes: default_max_payload_preview_bytes(),
        }
    }
}

impl Default for ReportConfig {
    fn default() -> Self {
        Self {
            write_on_shutdown: true,
            write_json: true,
            write_markdown: true,
            write_archive: true,
            include_latest_in_archive: true,
            archive_prefix: default_archive_prefix(),
            directory: default_report_directory(),
            latest_json: default_latest_json(),
            latest_markdown: default_latest_markdown(),
        }
    }
}

fn default_true() -> bool { true }
fn default_service_budget_ms() -> f64 { 8.0 }
fn default_plugin_lifecycle_budget_ms() -> f64 { 16.67 }
fn default_custom_budget_ms() -> f64 { 16.67 }
fn default_slow_job_warn_ms() -> f64 { 16.67 }
fn default_stale_active_job_ms() -> f64 { 1000.0 }
fn default_max_recent_jobs() -> usize { 4096 }
fn default_max_recent_diagnostics() -> usize { 1024 }
fn default_max_payload_preview_bytes() -> usize { 2048 }
fn default_archive_prefix() -> String { "profiler_report".to_owned() }
fn default_report_directory() -> String { "cache/profiler".to_owned() }
fn default_latest_json() -> String { "profiler_report_latest.json".to_owned() }
fn default_latest_markdown() -> String { "profiler_report_latest.md".to_owned() }

