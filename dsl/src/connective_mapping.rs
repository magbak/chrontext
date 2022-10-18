use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectiveMapping {
    #[serde(flatten)]
    pub map: HashMap<String, String>,
}
