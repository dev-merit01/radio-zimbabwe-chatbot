//! Platform-independent connection policy and settings. No voting data is stored here.
use serde::{Deserialize, Serialize};
use std::{fs, io::Write, path::Path};
use url::Url;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub server_url: String,
}

pub fn validate_server(value: &str) -> Result<Url, String> {
    if value.chars().any(|c| c.is_control() || c == '\\') || value.len() > 2048 {
        return Err("Enter a valid HTTPS server address.".into());
    }
    let url = Url::parse(value.trim()).map_err(|_| "Enter a valid HTTPS server address.")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.port() == Some(0)
    {
        return Err("Use only the HTTPS server address, without a path, password or query.".into());
    }
    // Never confuse a user-supplied remote server with Tauri's privileged local origin.
    if matches!(url.host_str(), Some("tauri.localhost" | "ipc.localhost")) {
        return Err("That address is reserved by the application.".into());
    }
    Ok(url)
}

pub fn allowed_navigation(server: &Url, target: &Url) -> bool {
    target.scheme() == "https"
        && target.username().is_empty()
        && target.password().is_none()
        && target.origin() == server.origin()
}

pub fn load_configuration(path: &Path) -> Result<Option<Configuration>, String> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("Saved settings could not be read. Enter your server address again.".into()),
    };
    let config: Configuration = serde_json::from_slice(&data)
        .map_err(|_| "Saved settings are damaged. Enter your server address again.")?;
    let server = validate_server(&config.server_url)?;
    Ok(Some(Configuration { server_url: server.to_string() }))
}

pub fn save_configuration(path: &Path, server: &Url) -> Result<(), String> {
    let directory = path.parent().ok_or("Settings folder is unavailable.")?;
    fs::create_dir_all(directory).map_err(|_| "Could not create the settings folder.")?;
    let mut file = tempfile::NamedTempFile::new_in(directory)
        .map_err(|_| "Could not save the server address.")?;
    let config = Configuration { server_url: server.to_string() };
    serde_json::to_writer(&mut file, &config).map_err(|_| "Could not save the server address.")?;
    file.flush().map_err(|_| "Could not save the server address.")?;
    file.as_file().sync_all().map_err(|_| "Could not save the server address.")?;
    file.persist(path).map_err(|_| "Could not replace the saved settings.")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_https_origins_and_normalizes_them() {
        assert_eq!(validate_server(" https://STATION.example:443 ").unwrap().as_str(), "https://station.example/");
        assert!(validate_server("https://station.example:8443/").is_ok());
    }

    #[test]
    fn rejects_unsafe_or_non_origin_addresses() {
        for value in ["http://station.example", "file:///tmp/test", "javascript:alert(1)",
            "https://user:pass@station.example", "https://station.example/path", "https://station.example/?secret=x",
            "https://station.example/#x", "https://station.example\n", "https://station.example\\evil",
            "https://tauri.localhost", "https://ipc.localhost", "https://station.example:0", ""] {
            assert!(validate_server(value).is_err(), "{value}");
        }
    }

    #[test]
    fn navigation_is_restricted_to_the_configured_origin() {
        let server = validate_server("https://station.example").unwrap();
        assert!(allowed_navigation(&server, &Url::parse("https://station.example/accounts/login/?next=/").unwrap()));
        for value in ["https://other.example/", "https://station.example:444/", "http://station.example/", "https://tauri.localhost/", "file:///etc/passwd"] {
            assert!(!allowed_navigation(&server, &Url::parse(value).unwrap()));
        }
    }

    #[test]
    fn settings_survive_replacement_and_store_only_the_server() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("settings/config.json");
        assert_eq!(load_configuration(&path).unwrap(), None);
        for address in ["https://first.example", "https://second.example"] {
            let server = validate_server(address).unwrap();
            save_configuration(&path, &server).unwrap();
            assert_eq!(load_configuration(&path).unwrap().unwrap().server_url, server.as_str());
        }
        let saved: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved.as_object().unwrap().len(), 1);
        fs::write(&path, b"broken").unwrap();
        assert!(load_configuration(&path).is_err());
    }
}
