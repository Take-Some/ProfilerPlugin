mod northstar_plugin_build {
    include!("../../build_support/plugin_cdylib_build.rs");
}

fn main() {
    northstar_plugin_build::run();
}
