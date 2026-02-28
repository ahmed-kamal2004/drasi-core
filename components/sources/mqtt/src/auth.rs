use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMode {
    None,
    UsernamePassword { username: String, password: String },
}
