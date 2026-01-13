#[derive(Debug, Clone)]
pub struct Account {
    pub username: String,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            username: "default".into(),
        }
    }
}
