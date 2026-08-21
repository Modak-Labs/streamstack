//! Saved endpoints, so connection details are named instead of retyped.
//!
//! `--endpoint`. A profile is only a set of defaults for the global flags: an
//! explicit flag or environment variable always wins, so nothing here can
//! change what an existing command line does.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

use crate::stream::{Endpoint, ProtocolArg};

/// The file's contents. Unknown keys are rejected so a typo is reported rather
/// than silently ignored.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Used when no `--profile` is given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, Profile>,
}

/// One saved endpoint. Every field is optional: a profile may set only what it
/// wants to change.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<ProtocolArg>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http2: Option<bool>,
}

/// Where the config lives: `$PICO_CONFIG`, else the platform's config
/// directory (`~/.config/pico/config.toml` on Linux, `~/Library/Application
/// Support/pico/config.toml` on macOS).
pub fn path() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("PICO_CONFIG") {
        return Ok(PathBuf::from(path));
    }
    dirs::config_dir()
        .map(|dir| dir.join("pico").join("config.toml"))
        .ok_or_else(|| "no config directory on this platform; set PICO_CONFIG".to_owned())
}

/// Read the config, treating a missing file as an empty one.
pub fn load() -> Result<Config, String> {
    let path = path()?;
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Config::default()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

fn save(config: &Config) -> Result<PathBuf, String> {
    let path = path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    let text = toml::to_string_pretty(config).map_err(|e| format!("cannot encode config: {e}"))?;
    std::fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))?;
    // This file will hold credentials if authentication is ever added, and a
    // config that starts private cannot be accidentally widened later.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(path)
}

/// Look up the profile to apply: the named one, else the configured default.
///
/// A `--profile` that does not exist is an error. A missing default is not,
/// since that is just an unconfigured machine.
pub fn selected(name: Option<&str>) -> Result<Profile, String> {
    let config = load()?;
    let Some(name) = name.or(config.default_profile.as_deref()) else {
        return Ok(Profile::default());
    };
    config
        .profiles
        .get(name)
        .cloned()
        .ok_or_else(|| format!("no profile named `{name}`"))
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Create or update a profile.
    Set(SetArgs),
    /// Show one profile (the default profile when none is named).
    Get { profile: Option<String> },
    /// List profiles, marking the default.
    Ls,
    /// Delete a profile.
    Rm { profile: String },
    /// Choose the profile used when `--profile` is absent.
    Use { profile: String },
    /// Print the config file's location.
    Path,
}

/// `set` saves the global connection flags rather than defining its own, so
/// `pico --endpoint URL config set prod` stores exactly what
/// `pico --endpoint URL <command>` would have used.
#[derive(Debug, Args)]
pub struct SetArgs {
    pub profile: String,

    /// Clear the profile's HTTP/2 setting (`--http2` sets it).
    #[arg(long)]
    pub no_http2: bool,
}

pub fn run(command: ConfigCommand, flags: &Endpoint) -> Result<i32, String> {
    match command {
        ConfigCommand::Path => {
            println!("{}", path()?.display());
        }
        ConfigCommand::Ls => {
            let config = load()?;
            if config.profiles.is_empty() {
                eprintln!("no profiles; add one with `pico --endpoint <url> config set <name>`");
                return Ok(0);
            }
            for (name, profile) in &config.profiles {
                let default = if config.default_profile.as_deref() == Some(name.as_str()) {
                    " (default)"
                } else {
                    ""
                };
                println!("{name}{default}\t{}", describe(profile));
            }
        }
        ConfigCommand::Get { profile } => {
            let config = load()?;
            let name = profile.or(config.default_profile.clone());
            let Some(name) = name else {
                return Err("no profile named and no default set".to_owned());
            };
            let found = config
                .profiles
                .get(&name)
                .ok_or_else(|| format!("no profile named `{name}`"))?;
            println!("{name}\t{}", describe(found));
        }
        ConfigCommand::Set(args) => {
            if flags.endpoint.is_none()
                && flags.protocol.is_none()
                && !flags.http2
                && !args.no_http2
            {
                return Err(
                    "nothing to save; pass --endpoint, --protocol, --http2 or --no-http2"
                        .to_owned(),
                );
            }
            let mut config = load()?;
            let profile = config.profiles.entry(args.profile.clone()).or_default();
            // Only the given fields change, so `set` can adjust one value
            // without restating the rest.
            if flags.endpoint.is_some() {
                profile.endpoint = flags.endpoint.clone();
            }
            if flags.protocol.is_some() {
                profile.protocol = flags.protocol;
            }
            if flags.http2 {
                profile.http2 = Some(true);
            } else if args.no_http2 {
                profile.http2 = None;
            }
            // The first profile is almost certainly the one to use.
            if config.default_profile.is_none() {
                config.default_profile = Some(args.profile.clone());
            }
            let path = save(&config)?;
            eprintln!("saved profile `{}` to {}", args.profile, path.display());
        }
        ConfigCommand::Rm { profile } => {
            let mut config = load()?;
            if config.profiles.remove(&profile).is_none() {
                return Err(format!("no profile named `{profile}`"));
            }
            if config.default_profile.as_deref() == Some(profile.as_str()) {
                config.default_profile = None;
            }
            save(&config)?;
            eprintln!("removed profile `{profile}`");
        }
        ConfigCommand::Use { profile } => {
            let mut config = load()?;
            if !config.profiles.contains_key(&profile) {
                return Err(format!("no profile named `{profile}`"));
            }
            config.default_profile = Some(profile.clone());
            save(&config)?;
            eprintln!("default profile is now `{profile}`");
        }
    }
    Ok(0)
}

fn describe(profile: &Profile) -> String {
    let mut parts = Vec::new();
    if let Some(endpoint) = &profile.endpoint {
        parts.push(endpoint.clone());
    }
    if let Some(protocol) = profile.protocol {
        parts.push(format!("{protocol:?}").to_lowercase());
    }
    if profile.http2 == Some(true) {
        parts.push("http2".to_owned());
    }
    if parts.is_empty() {
        return "(empty)".to_owned();
    }
    parts.join(" ")
}
