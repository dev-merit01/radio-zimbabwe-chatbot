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
use tauri_plugin_dialog::{DialogExt, MessageDialogButtons};
use tauri_plugin_updater::UpdaterExt;
use airvote::{allowed_navigation, load_configuration, save_configuration, validate_server};

#[derive(Default)]
struct Connecting(AtomicBool);

// Prevent overlapping automatic and user-initiated checks/installations.
#[derive(Default)]
struct Updating(AtomicBool);

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
    // A clean Windows installation needs no GitHub variable or manual URL entry.
    // The build variable still allows a trusted alternate server for local testing.
    let fallback = option_env!("VOTING_STUDIO_SERVER_URL")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("https://airvote.up.railway.app/")
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
        return Err("The server is unavailable or needs the AirVote desktop update.".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "The connection was interrupted. Try again.")?
    {
        if bytes.len() + chunk.len() > 4096 {
            return Err("This address did not return a AirVote response.".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let status: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| "This address is not a compatible AirVote server.")?;
    if status["application"] != "radio-zimbabwe-voting-studio" || status["desktop_api"] != 1 {
        return Err("This address is not a compatible AirVote server.".into());
    }
    save_configuration(&config_path(&app)?, &server)?;
    if let Some(old) = app.get_webview_window("workspace") {
        old.destroy()
            .map_err(|_| "Close the existing workspace and try again.")?;
    }
    let navigation_origin = server.clone();
    let download_origin = server.clone();
    let workspace = WebviewWindowBuilder::new(&app, "workspace", WebviewUrl::External(server))
        .title("AirVote")
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
                            .prefix("airvote-chart-").suffix(".csv")
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
        .build().map_err(|_| "The application window could not open. Restart AirVote and try again.")?;
    workspace
        .set_focus()
        .map_err(|_| "Could not focus the workspace.")?;
    window
        .hide()
        .map_err(|_| "Could not close the connection screen.")?;
    Ok(())
}

// Only user-requested connection settings expose the advanced URL field.
#[tauri::command]
fn reveal_setup(window: WebviewWindow) -> Result<(), String> {
    require_setup(&window)?;
    window.show().map_err(|_| "Could not show connection recovery.".to_string())?;
    window.set_focus().map_err(|_| "Could not focus connection recovery.".to_string())
}

fn show_setup(app: &AppHandle) {
    if let Some(setup) = app.get_webview_window("setup") {
        let _ = setup.eval("window.airvoteShowSettings?.();");
        let _ = setup.show();
        let _ = setup.unminimize();
        let _ = setup.set_focus();
    }
}

// The native menu reuses the authorised navigation already present in the
// station workspace. Do not pass user-supplied values into WebView evaluation.
fn open_workspace_page(app: &AppHandle, page: &str) {
    let script = match page {
        "overview" => r#"document.querySelector('[data-page="overview"]')?.click();"#,
        "incoming" => r#"document.querySelector('[data-page="incoming"]')?.click();"#,
        "review" => r#"document.querySelector('[data-page="review"]')?.click();"#,
        "catalogue" => r#"document.querySelector('[data-page="catalogue"]')?.click();"#,
        "archives" => r#"document.querySelector('[data-page="archives"]')?.click();"#,
        "administration" => r#"document.querySelector('[data-page="administration"]')?.click();"#,
        "activity" => r#"document.querySelector('[data-page="activity"]')?.click();"#,
        "connections" => r#"document.querySelector('[data-page="connections"]')?.click();"#,
        _ => return,
    };
    if let Some(window) = app.get_webview_window("workspace") {
        let _ = window.eval(script);
        let _ = window.set_focus();
    } else {
        show_setup(app);
    }
}

// Checks run only in trusted native Rust; remote station pages get no updater IPC permissions.
fn check_for_updates(app: &AppHandle, manual: bool) {
    if option_env!("AIRVOTE_UPDATER_PUBLIC_KEY")
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .is_none()
    {
        if manual {
            app.dialog()
                .message("Secure updates have not been configured for this AirVote build.")
                .show(|_| {});
        }
        return;
    }

    if app
        .state::<Updating>()
        .0
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        if manual {
            app.dialog().message("An update check is already in progress.").show(|_| {});
        }
        return;
    }

    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let result = async {
            let updater = handle.updater().map_err(|error| error.to_string())?;
            updater.check().await.map_err(|error| error.to_string())
        }
        .await;

        match result {
            Ok(Some(update)) => {
                let dialog_handle = handle.clone();
                handle.dialog()
                    .message(format!(
                        "AirVote {} is available. Install the signed update now? The application will close during installation.",
                        update.version
                    ))
                    .buttons(MessageDialogButtons::OkCancelCustom(
                        "Install update".into(),
                        "Later".into(),
                    ))
                    .show(move |approved| {
                        if !approved {
                            dialog_handle.state::<Updating>().0.store(false, Ordering::Release);
                            return;
                        }
                        let install_handle = dialog_handle.clone();
                        tauri::async_runtime::spawn(async move {
                            // Tauri verifies the signature before launching the Windows installer.
                            // On Windows the updater itself closes the running application.
                            if let Err(_error) = update.download_and_install(|_, _| {}, || {}).await {
                                install_handle.state::<Updating>().0.store(false, Ordering::Release);
                                install_handle.dialog()
                                    .message("The AirVote update could not be installed. Your existing version is unchanged; use Help > Check for updates to retry.")
                                    .show(|_| {});
                            }
                        });
                    });
            }
            Ok(None) => {
                handle.state::<Updating>().0.store(false, Ordering::Release);
                if manual {
                    handle.dialog().message("AirVote is up to date.").show(|_| {});
                }
            }
            Err(_) => {
                handle.state::<Updating>().0.store(false, Ordering::Release);
                if manual {
                    handle.dialog()
                        .message("Could not check for AirVote updates. Check your internet connection and try again.")
                        .show(|_| {});
                }
            }
        }
    });
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            if let Some(workspace) = app.get_webview_window("workspace") {
                let _ = workspace.unminimize();
                let _ = workspace.show();
                let _ = workspace.set_focus();
            }
            // During the first connection the setup webview is deliberately hidden.
        }))
        .plugin(tauri_plugin_dialog::init())
        .manage(Connecting::default())
        .manage(Updating::default())
        .invoke_handler(tauri::generate_handler![get_configuration, connect_server, reveal_setup])
        .setup(|app| {
            // The signing public key is injected at compile time by the signed release
            // workflow. An unsigned development build cannot enable auto-updates.
            if let Some(key) = option_env!("AIRVOTE_UPDATER_PUBLIC_KEY")
                .map(str::trim)
                .filter(|key| !key.is_empty())
            {
                app.handle().plugin(tauri_plugin_updater::Builder::new().pubkey(key).build())?;
            }
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
            let view = SubmenuBuilder::new(app, "View")
                .text("nav_overview", "Weekly overview")
                .text("nav_view_incoming", "Incoming votes")
                .text("nav_view_review", "Review queue")
                .text("nav_view_catalogue", "Song catalogue")
                .text("nav_view_archives", "Chart archives")
                .separator()
                .text("view_reload", "Refresh workspace")
                .text("view_fullscreen", "Toggle full screen")
                .build()?;
            let stations = SubmenuBuilder::new(app, "Stations")
                .text("stations_switch", "Switch station…")
                .separator()
                .text("planned_stations_manage", "Manage stations")
                .text("planned_stations_channels", "WhatsApp & SMS numbers")
                .text("planned_stations_status", "Station connection status")
                .build()?;
            let votes = SubmenuBuilder::new(app, "Votes")
                .text("nav_votes_incoming", "Incoming votes")
                .text("nav_votes_review", "Review & verify songs")
                .separator()
                .text("planned_votes_history", "Search vote history")
                .text("planned_votes_flagged", "Flagged submissions")
                .build()?;
            let songs = SubmenuBuilder::new(app, "Songs")
                .text("nav_songs_catalogue", "Song catalogue")
                .separator()
                .text("planned_songs_import", "Import songs")
                .text("planned_songs_ai", "AI matching rules")
                .text("planned_songs_duplicates", "Duplicate management")
                .build()?;
            let charts = SubmenuBuilder::new(app, "Charts")
                .text("nav_charts_live", "Live weekly Top 20")
                .text("nav_charts_archives", "Chart archives")
                .separator()
                .text("planned_charts_finalise", "Finalise a chart")
                .text("planned_charts_year_end", "Year-end chart")
                .build()?;
            let reports = SubmenuBuilder::new(app, "Reports")
                .text("planned_reports_weekly", "Weekly voting summary")
                .text("planned_reports_stations", "Compare stations")
                .text("planned_reports_export", "Export reports")
                .build()?;
            let tools = SubmenuBuilder::new(app, "Tools")
                .text("nav_tools_admin", "Admin control")
                .text("nav_tools_activity", "Activity log")
                .text("nav_tools_connections", "Connection dashboard")
                .separator()
                .text("planned_tools_integrations", "Integrations (Bird / OpenAI)")
                .text("planned_tools_users", "Users & permissions")
                .text("planned_tools_preferences", "Preferences")
                .build()?;
            let help = SubmenuBuilder::new(app, "Help")
                .text("check_updates", "Check for updates…")
                .separator()
                .text("planned_help_guide", "User guide")
                .text("planned_help_shortcuts", "Keyboard shortcuts")
                .separator()
                .text("help_about", "About AirVote")
                .build()?;
            app.set_menu(
                MenuBuilder::new(app)
                    .item(&connection)
                    .item(&edit)
                    .item(&view)
                    .item(&stations)
                    .item(&votes)
                    .item(&songs)
                    .item(&charts)
                    .item(&reports)
                    .item(&tools)
                    .item(&help)
                    .build()?,
            )?;
            check_for_updates(app.handle(), false);
            Ok(())
        })
        .on_menu_event(|app, event| match event.id().as_ref() {
            "connection" => show_setup(app),
            "check_updates" => check_for_updates(app, true),
            "reload" | "view_reload" => {
                if let Some(window) = app.get_webview_window("workspace") {
                    let _ = window.reload();
                }
            }
            "view_fullscreen" => {
                if let Some(window) = app.get_webview_window("workspace") {
                    if let Ok(fullscreen) = window.is_fullscreen() {
                        let _ = window.set_fullscreen(!fullscreen);
                    }
                }
            }
            "nav_overview" | "nav_charts_live" => open_workspace_page(app, "overview"),
            "nav_view_incoming" | "nav_votes_incoming" => open_workspace_page(app, "incoming"),
            "nav_view_review" | "nav_votes_review" => open_workspace_page(app, "review"),
            "nav_view_catalogue" | "nav_songs_catalogue" => open_workspace_page(app, "catalogue"),
            "nav_view_archives" | "nav_charts_archives" => open_workspace_page(app, "archives"),
            "nav_tools_admin" => open_workspace_page(app, "administration"),
            "nav_tools_activity" => open_workspace_page(app, "activity"),
            "nav_tools_connections" => open_workspace_page(app, "connections"),
            "stations_switch" => {
                if let Some(window) = app.get_webview_window("workspace") {
                    let _ = window.eval("document.getElementById('station-select')?.focus();");
                    let _ = window.set_focus();
                } else {
                    show_setup(app);
                }
            }
            "help_about" => {
                app.dialog()
                    .message(format!("AirVote {}\\nMulti-station voting workspace.", env!("CARGO_PKG_VERSION")))
                    .show(|_| {});
            }
            id if id.starts_with("planned_") => {
                app.dialog()
                    .message("This command is planned for a future AirVote update. Existing workspace features remain available.")
                    .show(|_| {});
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
        .expect("AirVote could not start");
}
