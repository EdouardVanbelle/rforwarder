use std::io::BufReader;
use std::fs::File;
use std::env;

use log;

use serde::{Deserialize, Serialize};
use serde_yaml_ng::{self};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub listen: String,
    pub command: Command,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub timeout: u32,
}

fn load_config(file: File) -> Option<Config> {
    let reader = BufReader::new(file);

    let deserialized_config: Config = serde_yaml_ng::from_reader(reader).expect("Error reading config");

    log::debug!("Config loaded: {:?}", deserialized_config);
    Some(deserialized_config)
}

pub fn load() -> Option<Config> {


    let mut paths: Vec<String> = vec![String::from("/etc")];

    match env::var("HOME") {
        Ok(p) => paths.push(p),
        _ => (),
    }

    match env::home_dir() {
        Some(p) => paths.push(p.display().to_string()),
        _ => (),
    }

    match env::current_dir() {
        Ok(p) => paths.push(p.display().to_string()),
        _ => (),
    }

    let configname = "forwarder.conf";
    let mut configfile: Option<File> = None;
    for p in paths.iter() {
        match File::open(format!("{}/{}", p, configname)) {
            Ok(f) => {
                log::debug!("config {} found in path {}", configname, p);
                configfile = Some(f);
                break;
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    log::debug!("{}/{} not found, trying next", p, configname)
                }
                _ => log::debug!("unable to open config {}/{}: {}", p, configname, e),
            },
        };
    }

    if configfile.is_none() {
        return Option::None;
    }

    load_config( configfile.unwrap())
}