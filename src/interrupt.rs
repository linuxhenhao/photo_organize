use anyhow::{Result, bail};
use std::sync::Once;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

static INTERRUPTED: AtomicBool = AtomicBool::new(false);
static INSTALL_HANDLER: Once = Once::new();
static NOTIFY: OnceLock<Notify> = OnceLock::new();

#[cfg(test)]
static IN_INTERRUPT_TEST: AtomicBool = AtomicBool::new(false);

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
    if !INTERRUPTED.load(Ordering::SeqCst) {
        return false;
    }
    #[cfg(test)]
    {
        IN_INTERRUPT_TEST.load(Ordering::SeqCst)
    }
    #[cfg(not(test))]
    true
}

pub fn check() -> Result<()> {
    if !requested() {
        return Ok(());
    }
    bail!("interrupted");
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

#[cfg(test)]
pub fn run_with_requested_interrupt<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    enter_interrupt_test();
    request_for_test();
    let result = f();
    release_interrupt_test();
    result
}

#[cfg(test)]
pub(crate) fn enter_interrupt_test() {
    while IN_INTERRUPT_TEST
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::thread::yield_now();
    }
}

#[cfg(test)]
pub(crate) fn release_interrupt_test() {
    reset();
    IN_INTERRUPT_TEST.store(false, Ordering::Release);
}
