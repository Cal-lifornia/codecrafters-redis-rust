#[macro_export]
macro_rules! cmd_struct {
    ($name:ident) => {
        pub struct $name<Writer: AsyncWrite + Unpin> {
            ctx: Context<Writer>,
            args: Vec<String>,
        }
    };
}
