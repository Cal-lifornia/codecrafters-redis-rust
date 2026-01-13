#[derive(Debug, Clone)]
pub struct Account {
    pub username: String,
    pub flags: Vec<String>,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            username: "default".into(),
            flags: vec![],
        }
    }
}
