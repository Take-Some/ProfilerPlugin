use abi_stable::std_types::{RResult, RString};
use newengine_plugin_api::{Blob, ConfigDiagLevelV1, ConfigDiagV1};
use serde_json::{json, Map, Value};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::records::JobBeginWire;

pub(crate) fn begin_to_json(wire: JobBeginWire) -> Value {
    json!({
        "id": wire.id,
        "name": wire.name,
        "label": wire.label,
        "category": wire.category,
        "source": wire.source,
        "detail": wire.detail,
        "budget_ms": wire.budget_ms,
        "payload_bytes": wire.payload_bytes,
        "metadata": wire.metadata,
    })
}

pub(crate) fn merge_patch(mut base: Value, patch: &Value) -> Value {
    match patch {
        Value::Object(pobj) => {
            if !base.is_object() {
                base = Value::Object(Map::new());
            }
            let bobj = base.as_object_mut().expect("base object ensured");
            for (k, pv) in pobj.iter() {
                if pv.is_null() {
                    bobj.remove(k);
                } else {
                    let cur = bobj.remove(k).unwrap_or(Value::Null);
                    bobj.insert(k.clone(), merge_patch(cur, pv));
                }
            }
            Value::Object(bobj.clone())
        }
        _ => patch.clone(),
    }
}

pub(crate) fn config_diag(level: ConfigDiagLevelV1, code: &str, message: String) -> ConfigDiagV1 {
    ConfigDiagV1 {
        level,
        code: RString::from(code),
        message: RString::from(message),
        path: RString::from(""),
        patch_name: None.into(),
    }
}

pub(crate) fn json_blob(value: Value) -> RResult<Blob, RString> {
    match serde_json::to_vec(&value) {
        Ok(bytes) => RResult::ROk(Blob::from(bytes)),
        Err(e) => RResult::RErr(RString::from(e.to_string())),
    }
}

pub(crate) fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_millis()
}

pub(crate) fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

pub(crate) fn sanitize_non_empty(value: Option<&str>, fallback: &str) -> String {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}

pub(crate) fn merge_metadata(base: Value, extra: Value) -> Value {
    match (base, extra) {
        (Value::Object(mut a), Value::Object(b)) => {
            for (k, v) in b {
                a.insert(k, v);
            }
            Value::Object(a)
        }
        (a, b) => json!({ "base": a, "extra": b }),
    }
}

pub(crate) fn trim_payload_preview(value: &mut Value, max_bytes: usize) {
    match value {
        Value::String(s) if s.len() > max_bytes => {
            s.truncate(max_bytes);
            s.push_str("...<trimmed>");
        }
        Value::Array(arr) => {
            for v in arr {
                trim_payload_preview(v, max_bytes);
            }
        }
        Value::Object(obj) => {
            for (_, v) in obj.iter_mut() {
                trim_payload_preview(v, max_bytes);
            }
        }
        _ => {}
    }
}

pub(crate) fn write_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create parent '{}' failed: {e}", parent.display()))?;
    }
    std::fs::write(path, bytes).map_err(|e| format!("write '{}' failed: {e}", path.display()))
}

pub(crate) fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub(crate) fn format_json_scalar(value: &Value) -> String {
    if let Some(v) = value.as_f64() {
        format!("{v:.3}")
    } else if let Some(v) = value.as_u64() {
        v.to_string()
    } else if let Some(v) = value.as_i64() {
        v.to_string()
    } else if let Some(v) = value.as_bool() {
        v.to_string()
    } else if let Some(v) = value.as_str() {
        v.to_owned()
    } else {
        value.to_string()
    }
}

pub(crate) fn escape_md(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}
