use serde::Serialize;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tauri::{
    menu::{MenuBuilder, SubmenuBuilder},
    webview::{DownloadEvent, NewWindowResponse},
    AppHandle, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder,
};
use tauri_plugin_dialog::DialogExt;
use voting_studio::{allowed_navigation, load_configuration, save_configuration, validate_server};

#[derive(Default)]
struct Connecting(AtomicBool);

struct ConnectionGuard<'a>(&'a AtomicBool);
impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Serialize)]
struct SetupConfiguration {
    server_url: String,
    warning: String,
}

fn config_path(app: &AppHandle) -> Result<PathBuf, String> {
    app.path()
        .app_config_dir()
        .map(|path| path.join("config.json"))
        .map_err(|_| "The settings folder is unavailable.".into())
}

fn require_setup(window: &WebviewWindow) -> Result<(), String> {
    // Defense in depth in addition to the local-only command capability.
    let url = window
        .url()
        .map_err(|_| "Connection settings are unavailable.")?;
    if window.label() != "setup"
        || url.scheme() != "https"
        || url.host_str() != Some("tauri.localhost")
    {
        return Err("Only the connection screen may change these settings.".into());
    }
    Ok(())
}

#[tauri::command]
fn get_configuration(app: AppHandle, window: WebviewWindow) -> Result<SetupConfiguration, String> {
    require_setup(&window)?;
    let fallback = option_env!("VOTING_STUDIO_SERVER_URL")
        .unwrap_or("")
        .to_string();
    match load_configuration(&config_path(&app)?) {
        Ok(Some(config)) => Ok(SetupConfiguration {
            server_url: config.server_url,
            warning: String::new(),
        }),
        Ok(None) => Ok(SetupConfiguration {
            server_url: fallback,
            warning: String::new(),
        }),
        Err(warning) => Ok(SetupConfiguration {
            server_url: fallback,
            warning,
        }),
    }
}

#[tauri::command]
async fn connect_server(
    app: AppHandle,
    window: WebviewWindow,
    state: tauri::State<'_, Connecting>,
    server_url: String,
) -> Result<(), String> {
    require_setup(&window)?;
    if state
        .inner()
        .0
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err("A connection is already in progress.".into());
    }
    let _guard = ConnectionGuard(&state.inner().0);
    let server = validate_server(&server_url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "Could not prepare a secure connection.")?;
    let mut response = client
        .get(
            server
                .join("api/desktop/status")
                .map_err(|_| "Invalid server address.")?,
        )
        .send()
        .await
        .map_err(|_| "Cannot connect. Check the server address and network, then try again.")?;
    if !response.status().is_success() {
        return Err("The server is unavailable or needs the Voting Studio desktop update.".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "The connection was interrupted. Try again.")?
    {
        if bytes.len() + chunk.len() > 4096 {
            return Err("This address did not return a Voting Studio response.".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let status: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| "This address is not a compatible Voting Studio server.")?;
    if status["application"] != "radio-zimbabwe-voting-studio" || status["desktop_api"] != 1 {
        return Err("This address is not a compatible Voting Studio server.".into());
    }
    save_configuration(&config_path(&app)?, &server)?;
    if let Some(old) = app.get_webview_window("workspace") {
        old.destroy()
            .map_err(|_| "Close the existing workspace and try again.")?;
    }
    let navigation_origin = server.clone();
    let download_origin = server.clone();
    let workspace = WebviewWindowBuilder::new(&app, "workspace", WebviewUrl::External(server))
        .title("Radio Zimbabwe Voting Studio")
        .inner_size(1360.0, 900.0).min_inner_size(900.0, 600.0)
        .incognito(true)
        .on_navigation(move |target| allowed_navigation(&navigation_origin, target))
        .on_new_window(|_, _| NewWindowResponse::Deny)
        .on_download(move |webview, event| {
            match event {
                DownloadEvent::Requested { url, destination } => {
                    if !allowed_navigation(&download_origin, &url) || url.path() != "/api/workspace/export" {
                        return false;
                    }
                    // Reserve a unique CSV filename in Downloads. Avoid a blocking
                    // save dialog on WebView2's UI callback thread.
                    let reserved = webview.app_handle().path().download_dir().ok()
                        .and_then(|directory| tempfile::Builder::new()
                            .prefix("radio-zimbabwe-chart-").suffix(".csv")
                            .tempfile_in(directory).ok())
                        .and_then(|file| file.keep().ok());
                    if let Some((file, path)) = reserved {
                        drop(file);
                        *destination = path;
                        true
                    } else {
                        webview.dialog().message("Could not save the chart. Check that your Downloads folder is available.").show(|_| {});
                        false
                    }
                }
                DownloadEvent::Finished { path, success, .. } => {
                    if success {
                        if let Some(path) = path {
                            webview.dialog().message(format!("Chart saved to {}", path.display())).show(|_| {});
                        }
                    } else {
                        if let Some(path) = path { let _ = std::fs::remove_file(path); }
                        webview.dialog().message("The chart download failed. Check your connection and try again.").show(|_| {});
                    }
                    true
                }
                _ => true,
            }
        })
        .build().map_err(|_| "The application window could not open. Restart Voting Studio and try again.")?;
    workspace
        .set_focus()
        .map_err(|_| "Could not focus the workspace.")?;
    window
        .hide()
        .map_err(|_| "Could not close the connection screen.")?;
    Ok(())
}

fn show_setup(app: &AppHandle) {
    if let Some(setup) = app.get_webview_window("setup") {
        let _ = setup.show();
        let _ = setup.unminimize();
        let _ = setup.set_focus();
    }
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            if let Some(workspace) = app.get_webview_window("workspace") {
                let _ = workspace.unminimize();
                let _ = workspace.show();
                let _ = workspace.set_focus();
            } else {
                show_setup(app);
            }
        }))
        .plugin(tauri_plugin_dialog::init())
        .manage(Connecting::default())
        .invoke_handler(tauri::generate_handler![get_configuration, connect_server])
        .setup(|app| {
            let connection = SubmenuBuilder::new(app, "Application")
                .text("connection", "Connection settings…")
                .text("reload", "Reload workspace")
                .separator()
                .text("quit", "Exit")
                .build()?;
            let edit = SubmenuBuilder::new(app, "Edit")
                .cut()
                .copy()
                .paste()
                .select_all()
                .build()?;
            app.set_menu(
                MenuBuilder::new(app)
                    .item(&connection)
                    .item(&edit)
                    .build()?,
            )?;
            Ok(())
        })
        .on_menu_event(|app, event| match event.id().as_ref() {
            "connection" => show_setup(app),
            "reload" => {
                if let Some(window) = app.get_webview_window("workspace") {
                    let _ = window.reload();
                }
            }
            "quit" => app.exit(0),
            _ => {}
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                if window.label() == "setup" {
                    if let Some(workspace) = window.app_handle().get_webview_window("workspace") {
                        api.prevent_close();
                        let _ = window.hide();
                        let _ = workspace.set_focus();
                        return;
                    }
                }
                window.app_handle().exit(0);
            }
        })
        .run(tauri::generate_context!())
        .expect("Voting Studio could not start");
}
