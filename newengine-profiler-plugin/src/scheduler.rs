use abi_stable::std_types::{RResult, RString};
use newengine_plugin_api::{Blob, CapabilityId, HostApiV1, MethodName};
use serde_json::Value;

use crate::constants::{ENGINE_JOBS_GATEWAY_ID, JOBS_INVOKE_SERVICE_V1};

pub(crate) type CallServiceV1 = extern "C" fn(CapabilityId, MethodName, Blob) -> RResult<Blob, RString>;

#[derive(Clone, Copy)]
pub(crate) struct HostJobScheduler {
    call_service_v1: CallServiceV1,
}

impl HostJobScheduler {
    pub(crate) fn from_host(host: &HostApiV1) -> Self {
        Self {
            call_service_v1: host.call_service_v1,
        }
    }

    pub(crate) fn invoke_service_job(&self, request: Value) -> Result<Value, String> {
        let bytes = serde_json::to_vec(&request)
            .map_err(|e| format!("serialize engine.jobs service-call request failed: {e}"))?;
        let blob = (self.call_service_v1)(
            CapabilityId::from(ENGINE_JOBS_GATEWAY_ID),
            MethodName::from(JOBS_INVOKE_SERVICE_V1),
            Blob::from(bytes),
        )
        .into_result()
        .map_err(|e| e.to_string())?;

        serde_json::from_slice::<Value>(blob.as_slice())
            .map_err(|e| format!("engine.jobs returned non-json service-call response: {e}"))
    }
}
