fn main() {
    println!("cargo:rerun-if-env-changed=VOTING_STUDIO_SERVER_URL");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        tauri_build::try_build(tauri_build::Attributes::new().app_manifest(
            tauri_build::AppManifest::new().commands(&["get_configuration", "connect_server"]),
        ))
        .expect("Failed to prepare the Windows application");
    }
}
