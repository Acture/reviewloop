use crate::backend::{BackendError, SubmitReceipt};
use serde::Deserialize;
use std::path::Path;
use tokio::process::Command;

/// One-line JSON report the fallback script prints: on stdout when it succeeds, on
/// stderr (exit code 1) when it fails.
#[derive(Debug, Deserialize)]
struct FallbackOutput {
    success: bool,
    token: Option<String>,
    error: Option<String>,
    /// Whether the script got as far as submitting the form. Only an explicit `false`
    /// proves the provider never saw the paper; anything else may have been accepted.
    submitted: Option<bool>,
}

impl FallbackOutput {
    fn failure(&self, detail: String) -> BackendError {
        if self.submitted == Some(false) {
            BackendError::Command(detail)
        } else {
            BackendError::OutcomeUnknown(detail)
        }
    }
}

fn last_report(text: &str) -> Option<FallbackOutput> {
    text.lines()
        .rev()
        .find_map(|line| serde_json::from_str(line.trim()).ok())
}

pub async fn submit_with_node_playwright(
    script_path: &Path,
    base_url: &str,
    pdf_path: &Path,
    email: &str,
    venue: Option<&str>,
) -> Result<SubmitReceipt, BackendError> {
    let mut cmd = Command::new("node");
    cmd.arg(script_path)
        .arg("--base-url")
        .arg(base_url)
        .arg("--pdf")
        .arg(pdf_path)
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
            Some(report) => report.failure(detail),
            None => BackendError::OutcomeUnknown(detail),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let parsed = last_report(&stdout).ok_or_else(|| {
        BackendError::OutcomeUnknown(format!(
            "fallback exited successfully without a JSON report; output={stdout}"
        ))
    })?;

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
