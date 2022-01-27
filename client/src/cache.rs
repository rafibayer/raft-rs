use std::path::PathBuf;

use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientCache {
    pub config: PathBuf
}