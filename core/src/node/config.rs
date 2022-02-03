use std::{collections::HashMap, net::SocketAddr, time::Duration};

use log::LevelFilter;
use serde::{Deserialize, Serialize};

use crate::raft::NodeID;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub cluster: HashMap<NodeID, SocketAddr>,

    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,

    pub heartbeat_interval_ms: u64,

    pub log_level: LogLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum LogLevel {
    Off,
    Trace,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Off => LevelFilter::Off,
            LogLevel::Trace => LevelFilter::Trace,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Error => LevelFilter::Error,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cluster: HashMap::from_iter(vec![(0, "127.0.0.1:8080".parse().unwrap())]),
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 350,
            heartbeat_interval_ms: 50,
            log_level: LogLevel::Info,
        }
    }
}

pub(crate) struct InternalConfig {
    pub cluster: HashMap<NodeID, SocketAddr>,
    pub election_timeout_range: std::ops::RangeInclusive<Duration>,
    pub heartbeat_interval: Duration,
}

impl From<Config> for InternalConfig {
    fn from(cfg: Config) -> Self {
        InternalConfig {
            cluster: cfg.cluster,
            election_timeout_range: Duration::from_millis(cfg.election_timeout_min_ms)
                ..=Duration::from_millis(cfg.election_timeout_max_ms),
            heartbeat_interval: Duration::from_millis(cfg.heartbeat_interval_ms),
        }
    }
}
