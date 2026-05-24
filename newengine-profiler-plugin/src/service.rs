use abi_stable::std_types::{RResult, RString};
use abi_stable::StableAbi;
use newengine_plugin_api::{Blob, CapabilityId, EventSinkV1, MethodName, ServiceV1};
use serde_json::json;

use crate::constants::*;
use crate::runtime::RUNTIME;
use crate::util::json_blob;

#[derive(Default, StableAbi)]
#[repr(C)]
pub(crate) struct ProfilerService;

impl ServiceV1 for ProfilerService {
    fn id(&self) -> CapabilityId {
        CapabilityId::from(PROFILER_SERVICE_ID)
    }

    fn describe(&self) -> RString {
        RString::from(SERVICE_DESCRIPTION_JSON)
    }

    fn call(&self, method: MethodName, payload: Blob) -> RResult<Blob, RString> {
        let Some(rt) = RUNTIME.get() else {
            return RResult::RErr(RString::from("profiler runtime not initialized"));
        };

        let result = match method.as_str() {
            METHOD_INFO_JSON => Ok(json!({
                "schema": "newengine.profiler.info.v1",
                "plugin_id": PROFILER_PLUGIN_ID,
                "service_id": PROFILER_SERVICE_ID,
                "gateway": ENGINE_PROFILER_GATEWAY_ID,
                "version": env!("CARGO_PKG_VERSION"),
                "enabled": rt.cfg.enabled,
            })),
            METHOD_JOB_BEGIN_JSON_V1 => rt.record_begin_value(payload.as_slice()),
            METHOD_JOB_END_JSON_V1 => rt.record_end_value(payload.as_slice()),
            METHOD_JOB_STATUS_JSON_V1 => rt.record_status_value(payload.as_slice()),
            METHOD_SNAPSHOT_JSON_V1 => Ok(rt.snapshot()),
            METHOD_DIAGNOSTICS_JSON_V1 => Ok(rt.diagnostics()),
            METHOD_FLUSH_REPORT_V1 => rt.flush_report("service.flush_report"),
            METHOD_SHUTDOWN_V1 => rt.flush_report("service.shutdown_v1"),
            _ => Err(format!("unknown profiler method: {method}")),
        };

        match result {
            Ok(value) => json_blob(value),
            Err(e) => RResult::RErr(RString::from(e)),
        }
    }
}

#[derive(Default, StableAbi)]
#[repr(C)]
pub(crate) struct ProfilerEventSink;

impl EventSinkV1 for ProfilerEventSink {
    fn on_event(&mut self, topic: RString, payload: Blob) {
        if let Some(rt) = RUNTIME.get() {
            rt.on_event(topic.as_str(), payload.as_slice());
        }
    }
}

