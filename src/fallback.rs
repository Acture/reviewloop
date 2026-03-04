use crate::backend::{BackendError, SubmitReceipt};
use serde::Deserialize;
use std::path::Path;
use tokio::process::Command;

#[derive(Debug, Deserialize)]
struct FallbackOutput {
    success: bool,
    token: Option<String>,
    error: Option<String>,
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
        .arg(email);

    if let Some(venue) = venue {
        if !venue.trim().is_empty() {
            cmd.arg("--venue").arg(venue);
        }
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| BackendError::Command(format!("failed to execute node fallback: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        return Err(BackendError::Command(format!(
            "fallback exited with status {}: {}",
            output.status, stderr
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let parsed: FallbackOutput = serde_json::from_str(stdout.trim()).map_err(|e| {
        BackendError::Command(format!(
            "failed to parse fallback output as JSON: {e}; output={stdout}"
        ))
    })?;

    if !parsed.success {
        return Err(BackendError::Command(
            parsed
                .error
                .unwrap_or_else(|| "fallback returned success=false".to_string()),
        ));
    }

    let token = parsed
        .token
        .ok_or_else(|| BackendError::Command("fallback response missing token".to_string()))?;

    Ok(SubmitReceipt {
        token,
        backend_submission_ref: None,
    })
}
