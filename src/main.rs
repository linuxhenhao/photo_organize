mod db;
mod feature_loader;
mod features;
mod import;
mod interrupt;
mod phash_index;
mod scan;
mod serve;
mod util;

use anyhow::Result;
use clap::{ArgGroup, Parser, Subcommand};
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
    /// Discover source files into a scan database
    Scan {
        /// Path to the source scan database
        #[arg(long = "scan-db")]
        scan_db: PathBuf,
        /// Source directory to scan. Repeatable
        #[arg(long = "src", required = true)]
        src: Vec<PathBuf>,
    },
    /// Copy canonical files into the target library
    #[command(group(
        ArgGroup::new("import_input")
            .required(true)
            .multiple(true)
            .args(["scan_db", "src"])
    ))]
    Import {
        /// Path to catalog.db
        #[arg(long)]
        db: PathBuf,
        /// Existing source scan database. Required unless --src is given
        #[arg(long = "scan-db")]
        scan_db: Option<PathBuf>,
        /// Source directory to scan before import. Repeatable. Required unless --scan-db is given
        #[arg(long = "src")]
        src: Vec<PathBuf>,
        /// Target library directory
        #[arg(long)]
        dest: PathBuf,
        /// Enable pHash/AKAZE near-duplicate grouping. Default: exact-hash only
        #[arg(long)]
        visual_dedup: bool,
        #[arg(long, default_value_t = 14)]
        phash_threshold: u32,
        #[arg(long, default_value_t = 10)]
        akaze_min_matches: usize,
    },
    /// Adopt an existing target library into catalog.db
    Initcache {
        /// Path to catalog.db
        #[arg(long)]
        db: PathBuf,
        /// Existing target library directory
        #[arg(long)]
        dest: PathBuf,
        #[arg(long, default_value_t = 14)]
        phash_threshold: u32,
        #[arg(long, default_value_t = 10)]
        akaze_min_matches: usize,
    },
    /// Run the local duplicate-resolution web UI
    Serve {
        /// Path to catalog.db
        #[arg(long)]
        db: PathBuf,
        /// Target library directory
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

    interrupt::install_handler()?;
    interrupt::reset();

    let cli = Cli::parse();
    match cli.command {
        Commands::Scan { scan_db, src } => scan::run(&scan_db, &src)?,
        Commands::Import {
            db,
            scan_db,
            src,
            dest,
            visual_dedup,
            phash_threshold,
            akaze_min_matches,
        } => import::run(
            &db,
            scan_db.as_ref(),
            &src,
            &dest,
            visual_dedup,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_requires_scan_db_or_src() {
        let err = Cli::try_parse_from(["photo-org", "import", "--db", "c.db", "--dest", "lib"])
            .expect_err("import without --scan-db or --src should fail at parse time");
        let msg = err.to_string();
        assert!(
            msg.contains("scan-db") && msg.contains("src"),
            "unexpected clap error: {msg}"
        );
    }

    #[test]
    fn import_accepts_src_without_scan_db() {
        Cli::try_parse_from([
            "photo-org",
            "import",
            "--db",
            "c.db",
            "--dest",
            "lib",
            "--src",
            "inbox",
        ])
        .expect("import --src should be enough");
    }

    #[test]
    fn import_accepts_scan_db_without_src() {
        Cli::try_parse_from([
            "photo-org",
            "import",
            "--db",
            "c.db",
            "--dest",
            "lib",
            "--scan-db",
            "scan.db",
        ])
        .expect("import --scan-db should be enough");
    }
}
