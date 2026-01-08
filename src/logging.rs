use tracing::{Level, level_filters::LevelFilter};
use tracing_subscriber::{Layer, layer::SubscriberExt};

pub fn init() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(true)
            .with_file(true)
            .with_line_number(true)
            .with_filter(LevelFilter::from_level(Level::DEBUG)),
    );
    tracing::subscriber::set_global_default(subscriber)?;
    tracing::info!("started logging");
    Ok(())
}
