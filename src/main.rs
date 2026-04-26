mod db;
mod feature_loader;
mod features;
mod import;
mod phash_index;
mod scan;
mod serve;
mod util;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "photo-org", version, about = "Rust photo organizer rewrite")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Scan {
        #[arg(long = "scan-db")]
        scan_db: PathBuf,
        #[arg(long = "src", required = true)]
        src: Vec<PathBuf>,
    },
    Import {
        #[arg(long)]
        db: PathBuf,
        #[arg(long = "scan-db")]
        scan_db: Option<PathBuf>,
        #[arg(long = "src")]
        src: Vec<PathBuf>,
        #[arg(long)]
        dest: PathBuf,
        #[arg(long, default_value_t = 10)]
        phash_threshold: u32,
        #[arg(long, default_value_t = 10)]
        akaze_min_matches: usize,
    },
    Initcache {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        dest: PathBuf,
        #[arg(long, default_value_t = 10)]
        phash_threshold: u32,
        #[arg(long, default_value_t = 10)]
        akaze_min_matches: usize,
    },
    Serve {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        dest: PathBuf,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 8080)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::from_default_env()
        .add_directive("warn".parse()?)
        .add_directive("photo_org=info".parse()?);

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Scan { scan_db, src } => scan::run(&scan_db, &src)?,
        Commands::Import {
            db,
            scan_db,
            src,
            dest,
            phash_threshold,
            akaze_min_matches,
        } => import::run(
            &db,
            scan_db.as_ref(),
            &src,
            &dest,
            phash_threshold,
            akaze_min_matches,
        )?,
        Commands::Initcache {
            db,
            dest,
            phash_threshold,
            akaze_min_matches,
        } => import::initcache(&db, &dest, phash_threshold, akaze_min_matches)?,
        Commands::Serve {
            db,
            dest,
            host,
            port,
        } => serve::run(db, dest, host, port).await?,
    }

    Ok(())
}
