use crate::backend::{BackendError, SubmitProgress, SubmitReceipt, SubmitStep};
use serde::Deserialize;
use std::path::Path;
use tokio::process::Command;

/// One-line JSON report the fallback script prints: on stdout when it succeeds, on
/// stderr (exit code 1) when it fails. It describes the provider's answer in the same
/// terms as the primary backend, so both routes share one outcome policy.
#[derive(Debug, Deserialize)]
struct FallbackOutput {
    success: bool,
    token: Option<String>,
    error: Option<String>,
    /// Whether `confirm-upload` (the request that creates the submission) was sent.
    /// Only an explicit `false` proves the provider never saw the paper; anything else
    /// may have been accepted.
    submitted: Option<bool>,
    /// The step the script reached: `upload_init`, `upload` or `confirm`.
    stage: Option<String>,
    /// HTTP status of the provider's answer to that step, when one arrived.
    status: Option<u16>,
    /// The provider answered 429.
    rate_limited: Option<bool>,
    /// The provider answered 2xx with `success: false`: it refused the paper.
    rejected: Option<bool>,
    /// The provider's `Retry-After`, in seconds.
    retry_after_secs: Option<i64>,
}

impl FallbackOutput {
    fn failure(&self, detail: String) -> BackendError {
        // Once confirm-upload was sent, only its own answer settles the outcome; an
        // answer reported for an earlier step says nothing about the submission.
        let answer_counts =
            self.submitted != Some(true) || self.stage.as_deref() == Some("confirm");
        if answer_counts && self.rate_limited == Some(true) {
            return BackendError::RateLimited {
                message: self.error.clone().unwrap_or(detail),
                retry_after: self
                    .retry_after_secs
                    .and_then(|secs| chrono::Duration::try_seconds(secs.max(0))),
            };
        }
        // A refusal (4xx, or 2xx with `success: false`) means the provider created nothing.
        let refused = self.rejected == Some(true)
            || self
                .status
                .is_some_and(|status| (400..500).contains(&status));
        if self.submitted == Some(false) || (answer_counts && refused) {
            BackendError::Command(detail)
        } else {
            BackendError::OutcomeUnknown(detail)
        }
    }

    fn record_stage(&self, progress: &SubmitProgress) {
        if let Some(step) = self.stage.as_deref().and_then(SubmitStep::parse) {
            progress.enter(step);
        }
    }
}

fn last_report(text: &str) -> Option<FallbackOutput> {
    text.lines()
        .rev()
        .find_map(|line| serde_json::from_str(line.trim()).ok())
}

/// Submit through the provider's web form with `script`, uploading `pdf_path` as
/// `file_name` with the same email and venue the primary backend sends.
pub async fn submit_with_node_playwright(
    script_path: &Path,
    base_url: &str,
    pdf_path: &Path,
    file_name: &str,
    email: &str,
    venue: Option<&str>,
    progress: &SubmitProgress,
) -> Result<SubmitReceipt, BackendError> {
    let mut cmd = Command::new("node");
    cmd.arg(script_path)
        .arg("--base-url")
        .arg(base_url)
        .arg("--pdf")
        .arg(pdf_path)
        .arg("--filename")
        .arg(file_name)
        .arg("--email")
        .arg(email)
        // A caller that gives up on the fallback (timeout) must not leave the browser
        // driving the provider's form in the background.
        .kill_on_drop(true);

    if let Some(venue) = venue
        && !venue.trim().is_empty()
    {
        cmd.arg("--venue").arg(venue);
    }

    // A missing script, or node failing to start, never reached the provider.
    if !tokio::fs::metadata(script_path)
        .await
        .is_ok_and(|meta| meta.is_file())
    {
        return Err(BackendError::Command(format!(
            "fallback script not found: {}",
            script_path.display()
        )));
    }
    let output = cmd
        .output()
        .await
        .map_err(|e| BackendError::Command(format!("failed to execute node fallback: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let detail = format!("fallback exited with status {}: {}", output.status, stderr);
        return Err(match last_report(&stderr) {
            Some(report) => {
                report.record_stage(progress);
                report.failure(detail)
            }
            None => BackendError::OutcomeUnknown(detail),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let parsed = last_report(&stdout).ok_or_else(|| {
        BackendError::OutcomeUnknown(format!(
            "fallback exited successfully without a JSON report; output={stdout}"
        ))
    })?;
    parsed.record_stage(progress);

    if !parsed.success {
        let detail = parsed
            .error
            .clone()
            .unwrap_or_else(|| "fallback returned success=false".to_string());
        return Err(parsed.failure(detail));
    }

    let token = parsed.token.ok_or_else(|| {
        BackendError::OutcomeUnknown("fallback reported success without a token".to_string())
    })?;

    Ok(SubmitReceipt { token })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(json: &str) -> FallbackOutput {
        last_report(json).expect("a JSON report")
    }

    #[test]
    fn rate_limited_report_maps_to_rate_limited_with_retry_after() {
        let err = report(
            r#"{"success":false,"submitted":false,"stage":"upload_init","status":429,"rate_limited":true,"retry_after_secs":90,"error":"slow down"}"#,
        )
        .failure("detail".to_string());
        let BackendError::RateLimited {
            message,
            retry_after,
        } = err
        else {
            panic!("expected RateLimited, got {err:?}");
        };
        assert_eq!(message, "slow down");
        assert_eq!(retry_after, Some(chrono::Duration::seconds(90)));
    }

    #[test]
    fn client_error_after_confirm_is_a_definitive_rejection() {
        let err = report(r#"{"success":false,"submitted":true,"stage":"confirm","status":422}"#)
            .failure("detail".to_string());
        assert!(matches!(err, BackendError::Command(_)), "{err:?}");
    }

    #[test]
    fn server_error_or_silence_after_confirm_is_unknown() {
        for json in [
            r#"{"success":false,"submitted":true,"stage":"confirm","status":502}"#,
            r#"{"success":false,"submitted":true,"stage":"confirm"}"#,
            r#"{"success":false}"#,
        ] {
            let err = report(json).failure("detail".to_string());
            assert!(
                matches!(err, BackendError::OutcomeUnknown(_)),
                "{json}: {err:?}"
            );
        }
    }

    #[test]
    fn success_false_at_confirm_is_a_definitive_rejection() {
        let err = report(
            r#"{"success":false,"submitted":true,"stage":"confirm","status":200,"rejected":true}"#,
        )
        .failure("detail".to_string());
        assert!(matches!(err, BackendError::Command(_)), "{err:?}");
    }

    #[test]
    fn an_answer_to_another_step_after_confirm_is_not_the_confirm_answer() {
        for json in [
            r#"{"success":false,"submitted":true,"stage":"upload","status":403}"#,
            r#"{"success":false,"submitted":true,"stage":"upload_init","status":429,"rate_limited":true}"#,
            r#"{"success":false,"submitted":true,"stage":"upload","rejected":true}"#,
        ] {
            let err = report(json).failure("detail".to_string());
            assert!(
                matches!(err, BackendError::OutcomeUnknown(_)),
                "{json}: {err:?}"
            );
        }
    }

    #[test]
    fn nothing_sent_is_definitive() {
        let err = report(r#"{"success":false,"submitted":false,"stage":"upload","status":503}"#)
            .failure("detail".to_string());
        assert!(matches!(err, BackendError::Command(_)), "{err:?}");
    }

    #[test]
    fn reported_stage_is_recorded() {
        let progress = SubmitProgress::default();
        report(r#"{"success":false,"stage":"upload"}"#).record_stage(&progress);
        assert_eq!(progress.current(), Some(SubmitStep::Upload));
        report(r#"{"success":false,"stage":"bogus"}"#).record_stage(&progress);
        assert_eq!(progress.current(), Some(SubmitStep::Upload));
    }
}
