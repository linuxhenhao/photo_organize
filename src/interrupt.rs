use anyhow::{Result, bail};
use std::sync::Once;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

static INTERRUPTED: AtomicBool = AtomicBool::new(false);
static INSTALL_HANDLER: Once = Once::new();
static NOTIFY: OnceLock<Notify> = OnceLock::new();

fn notifier() -> &'static Notify {
    NOTIFY.get_or_init(Notify::new)
}

pub fn install_handler() -> Result<()> {
    let mut install_result = Ok(());
    INSTALL_HANDLER.call_once(|| {
        install_result = ctrlc::set_handler(|| {
            let first = !INTERRUPTED.swap(true, Ordering::SeqCst);
            notifier().notify_waiters();
            if first {
                tracing::warn!("interrupt received; finishing in-flight work before stopping");
            } else {
                tracing::warn!("interrupt still pending; waiting for the next shutdown point");
            }
        })
        .map_err(anyhow::Error::from);
    });
    install_result
}

pub fn reset() {
    INTERRUPTED.store(false, Ordering::SeqCst);
}

pub fn requested() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

pub fn check() -> Result<()> {
    if requested() {
        bail!("interrupted");
    }
    Ok(())
}

pub async fn wait() {
    if requested() {
        return;
    }
    notifier().notified().await;
}

#[cfg(test)]
pub fn request_for_test() {
    INTERRUPTED.store(true, Ordering::SeqCst);
    notifier().notify_waiters();
}
