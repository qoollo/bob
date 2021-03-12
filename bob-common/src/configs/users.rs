use crate::prelude::*;

use tokio::fs::read_to_string;

const DEFAULT_NAME: &str = "default";
const DEFAULT_PASSWORD: &str = "default";

#[derive(Debug, Serialize, Deserialize)]
pub struct Users {
    users: Vec<User>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    #[serde(default = "default_name")]
    name: String,
    #[serde(default = "default_password")]
    password: String,
    #[serde(default)]
    http_perms: Access,
    #[serde(default)]
    grpc_perms: Access,
}

impl Users {
    pub fn into_inner(self) -> Vec<User> {
        self.users
    }

    pub async fn from_file(path: impl AsRef<Path>) -> AnyResult<Self> {
        let s = read_to_string(path.as_ref())
            .await
            .with_context(|| format!("failed to read file to string: {:?}", path.as_ref()))?;
        serde_yaml::from_str(&s)
            .with_context(|| format!("failed to deserialize config: {:?}", path.as_ref()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Access {
    Read,
    Write,
}

impl Default for Access {
    fn default() -> Self {
        Access::Read
    }
}

fn default_name() -> String {
    DEFAULT_NAME.to_string()
}

fn default_password() -> String {
    DEFAULT_PASSWORD.to_string()
}

#[cfg(test)]
mod tests {
    use super::Users;

    #[tokio::test]
    async fn test_configs_users_read() {
        let users = Users::from_file(format!(
            "{}/../config-examples/users.yaml",
            env!("CARGO_MANIFEST_DIR")
        ))
        .await
        .unwrap();
        assert_eq!(users.users.len(), 1);
    }
}
