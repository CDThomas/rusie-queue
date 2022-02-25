use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Message {
    SendSignInEmail {
        email: String,
        name: String,
        code: String,
    },
    // ...
}
