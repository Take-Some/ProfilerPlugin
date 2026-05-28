pub(crate) const PROFILER_PLUGIN_ID: &str = "engine.profiler.starprofiler";
pub(crate) const PROFILER_PLUGIN_NAME: &str = "StarProfiler";
pub(crate) const ENGINE_PROFILER_GATEWAY_ID: &str = "engine.profiler";
pub(crate) const PROFILER_PROVIDER_GATEWAY_ID: &str = "engine.profiler.starprofiler";
pub(crate) const PROFILER_SERVICE_ID: &str = "profiler.api";
pub(crate) const PROFILER_BACKEND_CAPABILITY_ID: &str = "profiler.backend";

pub(crate) const ENGINE_JOBS_GATEWAY_ID: &str = "engine.jobs";
pub(crate) const JOBS_INVOKE_SERVICE_V1: &str = "job.invoke_service_v1";

pub(crate) const METHOD_INFO_JSON: &str = "info_json";
pub(crate) const METHOD_JOB_BEGIN_JSON_V1: &str = "profiler.job_begin_json_v1";
pub(crate) const METHOD_JOB_END_JSON_V1: &str = "profiler.job_end_json_v1";
pub(crate) const METHOD_JOB_STATUS_JSON_V1: &str = "profiler.job_status_json_v1";
pub(crate) const METHOD_SNAPSHOT_JSON_V1: &str = "profiler.snapshot_json_v1";
pub(crate) const METHOD_DIAGNOSTICS_JSON_V1: &str = "profiler.diagnostics_json_v1";
pub(crate) const METHOD_FLUSH_REPORT_V1: &str = "profiler.flush_report_v1";
pub(crate) const METHOD_FLUSH_REPORT_ASYNC_V1: &str = "profiler.flush_report_async_v1";
pub(crate) const METHOD_FLUSH_REPORT_SYNC_V1: &str = "profiler.flush_report_sync_v1";
pub(crate) const METHOD_FLUSH_STATUS_JSON_V1: &str = "profiler.flush_status_json_v1";
pub(crate) const METHOD_SHUTDOWN_V1: &str = "shutdown_v1";

pub(crate) const TOPIC_JOB_BEGIN: &str = "newengine.diagnostics.job.begin.v1";
pub(crate) const TOPIC_JOB_END: &str = "newengine.diagnostics.job.end.v1";
pub(crate) const TOPIC_JOB_STATUS: &str = "newengine.diagnostics.job.status.v1";
pub(crate) const TOPIC_ENGINE_TASK_EVENT: &str = "engine.task.event.v1";
pub(crate) const TOPIC_ENGINE_JOB_EVENT: &str = "engine.jobs.event.v1";

pub(crate) const CT_JSON: &str = "application/json";
pub(crate) const CT_JSON_MERGE_PATCH: &str = "application/merge-patch+json";
pub(crate) const CONFIG_FORMAT_VERSION: u32 = 1;

pub(crate) static DEFAULT_CONFIG_JSON: &str = include_str!("../assets/default_config.json");
pub(crate) static SERVICE_DESCRIPTION_JSON: &str = include_str!("../assets/service_description.json");
