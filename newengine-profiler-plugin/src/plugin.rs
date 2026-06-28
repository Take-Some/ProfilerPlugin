use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{RResult, RString, RVec};
use abi_stable::StableAbi;
use newengine_plugin_api::{
    ConfigApplyResultV1, ConfigBlobV1, ConfigDiagLevelV1, ConfigDiagV1, ConfigPatchV1,
    EventSinkV1Dyn, EventSinkV1_TO, HostApiV1, PluginDescriptor,
    PluginModule, ServiceV1Dyn, ServiceV1_TO,
};
use serde_json::Value;
use std::sync::Arc;

#[path = "plugin_definition.rs"]
mod plugin_definition;

use crate::config::ProfilerConfig;
use crate::constants::*;
use crate::runtime::{ProfilerRuntime, RUNTIME};
use crate::scheduler::HostJobScheduler;
use crate::service::{ProfilerEventSink, ProfilerService};
use crate::util::{config_diag, merge_patch};

#[derive(Default, StableAbi)]
#[repr(C)]
pub(crate) struct ProfilerPlugin {
    initialized: bool,
}

impl ProfilerPlugin {
    fn descriptor_impl(&self) -> PluginDescriptor {
        plugin_definition::descriptor()
    }

    fn init_with_cfg(&mut self, host: HostApiV1, cfg: ProfilerConfig) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }

        let scheduler = HostJobScheduler::from_host(&host);
        let rt = Arc::new(ProfilerRuntime::new(cfg.clone(), Some(scheduler)));
        let _ = RUNTIME.set(rt);

        let service: ServiceV1Dyn<'static> = ServiceV1_TO::from_value(ProfilerService, TD_Opaque);
        (host.register_service_v1)(service)
            .into_result()
            .map_err(|e| e.to_string())?;

        let sink: EventSinkV1Dyn<'static> = EventSinkV1_TO::from_value(ProfilerEventSink, TD_Opaque);
        (host.subscribe_events_v1)(sink)
            .into_result()
            .map_err(|e| e.to_string())?;

        (host.log_info)(RString::from(format!(
            "profiler: registered service='{}' gateway='{}' report_dir='{}' diagnostics='detailed-status' flush_mode='{}' jobs_required={}",
            PROFILER_SERVICE_ID,
            ENGINE_PROFILER_GATEWAY_ID,
            &cfg.report.directory,
            &cfg.scheduling.service_flush_mode,
            cfg.scheduling.require_engine_jobs,
        )));

        self.initialized = true;
        Ok(())
    }

    fn parse_defaults() -> Result<Value, String> {
        serde_json::from_str(DEFAULT_CONFIG_JSON)
            .map_err(|e| format!("profiler default config is invalid JSON: {e}"))
    }

    fn cfg_from_value(v: &Value) -> Result<ProfilerConfig, String> {
        serde_json::from_value::<ProfilerConfig>(v.clone())
            .map_err(|e| format!("profiler config is invalid: {e}"))
    }
}

impl PluginModule for ProfilerPlugin {
    fn descriptor(&self) -> PluginDescriptor { self.descriptor_impl() }

    fn config_defaults(&self) -> RResult<ConfigBlobV1, RString> {
        let defaults = match Self::parse_defaults() {
            Ok(v) => v,
            Err(e) => return RResult::RErr(RString::from(e)),
        };
        if let Err(e) = Self::cfg_from_value(&defaults) {
            return RResult::RErr(RString::from(e));
        }
        RResult::ROk(ConfigBlobV1 {
            content_type: RString::from(CT_JSON),
            bytes: RVec::from(DEFAULT_CONFIG_JSON.as_bytes().to_vec()),
            format_version: CONFIG_FORMAT_VERSION,
        })
    }

    fn config_apply_patches(&self, base: &ConfigBlobV1, patches: RVec<ConfigPatchV1>) -> RResult<ConfigApplyResultV1, RString> {
        if base.content_type.as_str() != CT_JSON {
            return RResult::RErr(RString::from("unsupported profiler config content_type"));
        }
        let mut cur = match serde_json::from_slice::<Value>(base.bytes.as_slice()) {
            Ok(v) => v,
            Err(e) => return RResult::RErr(RString::from(format!("profiler config parse failed: {e}"))),
        };
        let mut diags = RVec::new();
        let mut changed = false;

        for patch in patches.iter() {
            if patch.content_type.as_str() != CT_JSON && patch.content_type.as_str() != CT_JSON_MERGE_PATCH {
                return RResult::RErr(RString::from("unsupported profiler patch content_type"));
            }
            let pv = match serde_json::from_slice::<Value>(patch.bytes.as_slice()) {
                Ok(v) => v,
                Err(e) => return RResult::RErr(RString::from(format!("profiler patch parse failed: {e}"))),
            };
            cur = merge_patch(cur, &pv);
            changed = true;
        }

        if let Err(e) = Self::cfg_from_value(&cur) {
            diags.push(config_diag(ConfigDiagLevelV1::Error, "invalid_config", e.clone()));
            return RResult::RErr(RString::from(e));
        }

        let bytes = match serde_json::to_vec_pretty(&cur) {
            Ok(v) => RVec::from(v),
            Err(e) => return RResult::RErr(RString::from(e.to_string())),
        };

        RResult::ROk(ConfigApplyResultV1 {
            effective: ConfigBlobV1 {
                content_type: RString::from(CT_JSON),
                bytes,
                format_version: CONFIG_FORMAT_VERSION,
            },
            diags,
            changed,
        })
    }

    fn config_supports_live_update(&self) -> bool { false }

    fn config_update_live(&mut self, _effective: &ConfigBlobV1) -> RResult<RVec<ConfigDiagV1>, RString> {
        RResult::RErr(RString::from("profiler live config update is not supported yet"))
    }

    fn init(&mut self, host: HostApiV1, effective: ConfigBlobV1) -> RResult<(), RString> {
        if effective.content_type.as_str() != CT_JSON {
            return RResult::RErr(RString::from("unsupported profiler config content_type"));
        }
        let value = match serde_json::from_slice::<Value>(effective.bytes.as_slice()) {
            Ok(v) => v,
            Err(e) => return RResult::RErr(RString::from(e.to_string())),
        };
        let cfg = match Self::cfg_from_value(&value) {
            Ok(v) => v,
            Err(e) => return RResult::RErr(RString::from(e)),
        };
        match self.init_with_cfg(host, cfg) {
            Ok(()) => RResult::ROk(()),
            Err(e) => RResult::RErr(RString::from(e)),
        }
    }

    fn start(&mut self) -> RResult<(), RString> { RResult::ROk(()) }
    fn fixed_update(&mut self, _dt: f32) -> RResult<(), RString> { RResult::ROk(()) }
    fn update(&mut self, _dt: f32) -> RResult<(), RString> { RResult::ROk(()) }
    fn render(&mut self, _dt: f32) -> RResult<(), RString> { RResult::ROk(()) }

    fn shutdown(&mut self) {
        if let Some(rt) = RUNTIME.get() {
            if rt.cfg.report.write_on_shutdown {
                if rt.cfg.scheduling.shutdown_flush_mode.eq_ignore_ascii_case("engine_jobs") {
                    let _ = rt.flush_report_async("plugin.shutdown");
                } else {
                    let _ = rt.flush_report("plugin.shutdown");
                }
            }
        }
        self.initialized = false;
    }
}

