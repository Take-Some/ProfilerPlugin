use std::env;
use std::fs;
use std::path::PathBuf;

pub fn run() {
    // Build scripts must stay deterministic and self-contained.
    // The outer build pipeline may set NEWENGINE_PLUGIN_BUILD_TYPE to keep the
    // runtime DLL suffix stable even when Cargo maps dev/debug to target/debug.
    println!("cargo:rerun-if-env-changed=NEWENGINE_PLUGIN_BUILD_TYPE");
    println!("cargo:rerun-if-env-changed=NORTHSTAR_PLUGIN_INSTALL_NAME");

    let target = env::var("TARGET").unwrap_or_default();
    let is_windows = target.contains("windows");
    let is_msvc = target.contains("msvc");

    let pkg_name = env::var("CARGO_PKG_NAME").unwrap_or_else(|_| "plugin".to_owned());
    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_owned());
    let pkg_desc = env::var("CARGO_PKG_DESCRIPTION").unwrap_or_else(|_| "NorthStar Engine plugin".to_owned());
    let pkg_authors = env::var("CARGO_PKG_AUTHORS").unwrap_or_else(|_| "NorthStar".to_owned());

    let build_type = env::var("NEWENGINE_PLUGIN_BUILD_TYPE")
        .ok()
        .and_then(|raw| normalize_build_type(&raw))
        .unwrap_or_else(cargo_profile_build_type);

    let install_name = env::var("NORTHSTAR_PLUGIN_INSTALL_NAME")
        .ok()
        .map(|raw| sanitize_install_name(&raw))
        .filter(|raw| !raw.is_empty())
        .unwrap_or(pkg_name);

    let stem = format!("{install_name}-{pkg_version}-{build_type}");
    let dll_name = format!("{stem}.dll");

    if is_windows && is_msvc {
        emit_msvc_cdylib_args(&dll_name);
    }

    if is_windows {
        embed_windows_version_info(
            &stem,
            &dll_name,
            &pkg_version,
            &pkg_desc,
            &pkg_authors,
            "NewEngine plugin",
        );
    }
}

fn sanitize_install_name(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        .collect::<String>()
}

fn emit_msvc_cdylib_args(dll_name: &str) {
    println!("cargo:rustc-cdylib-link-arg=/OUT:{dll_name}");
    println!("cargo:rustc-cdylib-link-arg=/NOIMPLIB");
    println!("cargo:rustc-cdylib-link-arg=/DEBUG:NONE");
    println!("cargo:rustc-cdylib-link-arg=/OPT:REF");
    println!("cargo:rustc-cdylib-link-arg=/OPT:ICF");
}

fn normalize_build_type(raw: &str) -> Option<String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "dev" => Some("dev".to_owned()),
        "debug" => Some("debug".to_owned()),
        "release" => Some("release".to_owned()),
        "test" => Some("test".to_owned()),
        "bench" => Some("bench".to_owned()),
        "" => None,
        other => Some(other.to_owned()),
    }
}

fn cargo_profile_build_type() -> String {
    normalize_build_type(&env::var("PROFILE").unwrap_or_else(|_| "debug".to_owned()))
        .unwrap_or_else(|| "dev".to_owned())
}

fn embed_windows_version_info(
    internal_stem: &str,
    dll_name: &str,
    pkg_version: &str,
    pkg_desc: &str,
    pkg_authors: &str,
    product_name: &str,
) {
    let (maj, min, pat, bld) = parse_semver_4(pkg_version);
    let company = first_author_or(pkg_authors, "Take Some()");

    let rc = format!(
        r#"#include <windows.h>

#define VER_FILEVERSION             {maj},{min},{pat},{bld}
#define VER_FILEVERSION_STR         "{maj}.{min}.{pat}.{bld}\0"

#define VER_PRODUCTVERSION          {maj},{min},{pat},{bld}
#define VER_PRODUCTVERSION_STR      "{maj}.{min}.{pat}.{bld}\0"

VS_VERSION_INFO VERSIONINFO
 FILEVERSION     VER_FILEVERSION
 PRODUCTVERSION  VER_PRODUCTVERSION
 FILEFLAGSMASK   0x3fL
 FILEFLAGS       0x0L
 FILEOS          0x40004L
 FILETYPE        0x2L
 FILESUBTYPE     0x0L
BEGIN
    BLOCK "StringFileInfo"
    BEGIN
        BLOCK "040904B0"
        BEGIN
            VALUE "CompanyName",      "{company}\0"
            VALUE "FileDescription",  "{pkg_desc}\0"
            VALUE "FileVersion",      "{pkg_version}\0"
            VALUE "InternalName",     "{internal_stem}\0"
            VALUE "OriginalFilename", "{dll_name}\0"
            VALUE "ProductName",      "{product_name}\0"
            VALUE "ProductVersion",   "{pkg_version}\0"
            VALUE "LegalCopyright",   "Copyright (c) {company}\0"
        END
    END
    BLOCK "VarFileInfo"
    BEGIN
        VALUE "Translation", 0x0409, 1200
    END
END
"#,
        maj = maj,
        min = min,
        pat = pat,
        bld = bld,
        company = escape_rc(&company),
        pkg_desc = escape_rc(pkg_desc),
        pkg_version = escape_rc(pkg_version),
        internal_stem = escape_rc(internal_stem),
        dll_name = escape_rc(dll_name),
        product_name = escape_rc(product_name),
    );

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let rc_path = out_dir.join("plugin_versioninfo.rc");
    fs::write(&rc_path, rc).expect("failed to write rc");
    let _ = embed_resource::compile(rc_path.to_str().unwrap(), embed_resource::NONE);
}

fn parse_semver_4(v: &str) -> (u16, u16, u16, u16) {
    let mut core = v;
    if let Some(i) = core.find('+') {
        core = &core[..i];
    }
    if let Some(i) = core.find('-') {
        core = &core[..i];
    }

    let mut it = core.split('.');
    let a = it.next().and_then(|s| s.parse::<u16>().ok()).unwrap_or(0);
    let b = it.next().and_then(|s| s.parse::<u16>().ok()).unwrap_or(0);
    let c = it.next().and_then(|s| s.parse::<u16>().ok()).unwrap_or(0);
    (a, b, c, 0)
}

fn first_author_or(authors: &str, fallback: &str) -> String {
    let first = authors.split(';').next().unwrap_or("").trim();
    if first.is_empty() {
        fallback.to_owned()
    } else {
        match first.find('<') {
            Some(i) => first[..i].trim().to_owned(),
            None => first.to_owned(),
        }
    }
}

fn escape_rc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}
