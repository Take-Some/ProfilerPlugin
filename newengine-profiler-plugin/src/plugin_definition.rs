#![forbid(unsafe_op_in_unsafe_fn)]

use newengine_plugin_api::{BackendServiceSpec, CapabilityKind, CapabilityRole};
use newengine_plugin_api::prelude::*;

use crate::constants::{
    ENGINE_PROFILER_GATEWAY_ID, PROFILER_BACKEND_CAPABILITY_ID, PROFILER_PLUGIN_ID,
    PROFILER_PLUGIN_NAME, PROFILER_PROVIDER_GATEWAY_ID, PROFILER_SERVICE_ID,
    SERVICE_DESCRIPTION_JSON,
};

const HOST_EVENTS_REQUIREMENT_JSON: &str = r#"{"schema":"newengine.profiler.event_requirements.v1","topics":["engine.task.event.v1","engine.jobs.event.v1","newengine.diagnostics.job.begin.v1","newengine.diagnostics.job.end.v1","newengine.diagnostics.job.status.v1","newengine.diagnostics.profiler.sample.v1"]}"#;

const JOB_SCHEDULER_REQUIREMENT_JSON: &str = r#"{"schema":"newengine.profiler.job_scheduler_requirement.v1","gateway":"engine.jobs","methods":["job.invoke_service_v1","job.start_v1","job.progress_event_v1","job.status_json_v1"],"purpose":"execute profiler report build/write work on engine.jobs provider workers instead of hidden plugin background load"}"#;

const PROFILER_SERVICES: &[PluginServiceDefinition] = &[
    plugin_service(PROFILER_SERVICE_ID, 1, SERVICE_DESCRIPTION_JSON),
];

const PROFILER_BACKEND_ROUTES: &[PluginBackendRouteDefinition] = &[optional_backend_route(
    PROFILER_BACKEND_CAPABILITY_ID,
    BackendServiceSpec::new(
        "profiler",
        ENGINE_PROFILER_GATEWAY_ID,
        PROFILER_SERVICE_ID,
        PROFILER_BACKEND_CAPABILITY_ID,
    ),
    Some(PROFILER_PROVIDER_GATEWAY_ID),
    Some("starprofiler"),
    None,
    100,
    &[],
    &[],
    &[],
)];

const PROFILER_CAPABILITIES: &[PluginCapabilityDefinition] = &[
    PluginCapabilityDefinition {
        id: "host.events.v1",
        role: CapabilityRole::Requires,
        kind: CapabilityKind::EventsV1,
        version: 1,
        describe_json: HOST_EVENTS_REQUIREMENT_JSON,
    },
    PluginCapabilityDefinition {
        id: "engine.jobs",
        role: CapabilityRole::Requires,
        kind: CapabilityKind::ServiceV1,
        version: 1,
        describe_json: JOB_SCHEDULER_REQUIREMENT_JSON,
    },
];

const PLUGIN_DEFINITION: PluginDefinition = PluginDefinition {
    id: PROFILER_PLUGIN_ID,
    name: PROFILER_PLUGIN_NAME,
    version: env!("CARGO_PKG_VERSION"),
    kind: PluginKind::Runtime,
    services: PROFILER_SERVICES,
    backend_routes: PROFILER_BACKEND_ROUTES,
    capabilities: PROFILER_CAPABILITIES,
};

pub(crate) fn descriptor() -> PluginDescriptor {
    PLUGIN_DEFINITION.descriptor()
}
