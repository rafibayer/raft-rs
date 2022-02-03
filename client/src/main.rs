mod cache;
mod cli;
pub mod lib;

use core::node::config::Config;
use std::{
    error::Error,
    fs::{self, File},
    io::Write,
};

use clap::Parser;
use simple_logger::SimpleLogger;

use crate::cli::ClientArgs;
use crate::{cache::ClientCache};
use crate::lib::Client;

const CACHE_PATH: &str = "./client_data/cache.json";

fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();

    let args = ClientArgs::parse();

    let client_data = match args.config {
        Some(config_path) => {
            log::info!("Searching for config at provided path \"{config_path}\"");
            let cache = ClientCache { config: config_path.into() };
            let mut cache_file = File::create(CACHE_PATH).unwrap();
            cache_file.write_all(&serde_json::to_vec(&cache).unwrap()).unwrap();
            cache
        }
        None => {
            log::trace!("Searching for existing config in cache: {CACHE_PATH:?}");
            serde_json::from_str(
                &fs::read_to_string(CACHE_PATH)
                    .expect("No cache found, please set the cluster config path with --config"),
            )
            .unwrap()
        }
    };

    let config =
        serde_json::from_str::<Config>(&fs::read_to_string(client_data.config).unwrap()).unwrap();
    log::trace!("Creating client from config: {config:#?}");

    println!("{:?}", args.command.execute(&mut Client::new(config.cluster))?);

    Ok(())
}
