use abi_stable::std_types::{RResult, RString};
use abi_stable::StableAbi;
use newengine_plugin_api::{Blob, CapabilityId, EventSinkV1, MethodName, ServiceV1};
use serde_json::{json, Value};

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
            METHOD_FLUSH_STATUS_JSON_V1 => Ok(rt.flush_status()),
            METHOD_FLUSH_REPORT_V1 => rt.flush_report_service(&flush_reason(payload.as_slice(), "service.flush_report_v1")),
            METHOD_FLUSH_REPORT_ASYNC_V1 => rt.flush_report_async(&flush_reason(payload.as_slice(), "service.flush_report_async_v1")),
            METHOD_FLUSH_REPORT_SYNC_V1 => {
                let request = flush_request(payload.as_slice(), "service.flush_report_sync_v1");
                let result = rt.flush_report(&request.reason);
                if let Some(request_id) = request.request_id {
                    match &result {
                        Ok(_) => rt.mark_flush_request_completed(&request_id, None),
                        Err(e) => rt.mark_flush_request_completed(&request_id, Some(e.clone())),
                    }
                }
                result
            },
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



struct FlushServiceRequest {
    reason: String,
    request_id: Option<String>,
}

fn flush_reason(payload: &[u8], fallback: &str) -> String {
    flush_request(payload, fallback).reason
}

fn flush_request(payload: &[u8], fallback: &str) -> FlushServiceRequest {
    if payload.is_empty() {
        return FlushServiceRequest { reason: fallback.to_owned(), request_id: None };
    }
    let parsed = serde_json::from_slice::<Value>(payload).ok();
    let reason = parsed
        .as_ref()
        .and_then(|value| value.get("reason").and_then(Value::as_str).map(str::to_owned))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_owned());
    let request_id = parsed
        .as_ref()
        .and_then(|value| value.get("request_id").and_then(Value::as_str).map(str::trim))
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    FlushServiceRequest { reason, request_id }
}
