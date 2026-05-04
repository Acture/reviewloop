//! OS-native desktop notifications for job lifecycle events.
//!
//! All entry points are infallible from the caller's perspective: failures
//! degrade to a `tracing::warn!` and the caller continues unaffected.

use crate::config::NotificationsConfig;
use tracing::warn;

#[derive(Debug, Clone, Copy)]
pub enum NotificationKind {
    Completed,
    FailedNeedsManual,
    Timeout,
    TickError,
}

impl NotificationKind {
    fn title(self) -> &'static str {
        match self {
            NotificationKind::Completed => "ReviewLoop: review ready",
            NotificationKind::FailedNeedsManual => "ReviewLoop: manual intervention required",
            NotificationKind::Timeout => "ReviewLoop: job timed out",
            NotificationKind::TickError => "ReviewLoop: daemon tick failed",
        }
    }
}

pub fn notify(
    cfg: &NotificationsConfig,
    kind: NotificationKind,
    paper_id: Option<&str>,
    job_id: Option<&str>,
    body: Option<&str>,
) {
    if !cfg.enabled {
        return;
    }

    let body_str = build_body(cfg, paper_id, job_id, body);
    let title = kind.title();

    if let Err(err) = notify_rust::Notification::new()
        .summary(title)
        .body(&body_str)
        .show()
    {
        warn!(error = %err, "desktop notification failed");
    }
}

fn build_body(
    cfg: &NotificationsConfig,
    paper_id: Option<&str>,
    job_id: Option<&str>,
    body: Option<&str>,
) -> String {
    if cfg.summary_only {
        return paper_id.unwrap_or("").to_string();
    }

    let mut parts: Vec<String> = Vec::new();

    if let Some(pid) = paper_id {
        if !pid.is_empty() {
            parts.push(pid.to_string());
        }
    }

    if let Some(jid) = job_id {
        if !jid.is_empty() {
            let short = if jid.len() > 8 { &jid[..8] } else { jid };
            parts.push(format!("job {short}"));
        }
    }

    if let Some(b) = body {
        if !b.is_empty() {
            parts.push(b.to_string());
        }
    }

    parts.join(" · ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NotificationsConfig;

    #[test]
    fn notify_disabled_does_not_panic() {
        let cfg = NotificationsConfig {
            enabled: false,
            summary_only: false,
        };
        // Must not panic even though no OS notification daemon may be running
        notify(
            &cfg,
            NotificationKind::Completed,
            Some("main"),
            Some("abc123"),
            None,
        );
        notify(
            &cfg,
            NotificationKind::TickError,
            None,
            None,
            Some("some error"),
        );
    }
}
