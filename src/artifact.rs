use crate::model::Job;
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use std::{fs, path::Path};

pub fn write_review_artifacts(
    state_dir: &Path,
    job: &Job,
    token: &str,
    raw_json: &Value,
) -> Result<(String, String, String)> {
    let artifact_dir = state_dir.join("artifacts").join(&job.id);
    fs::create_dir_all(&artifact_dir)
        .with_context(|| format!("failed to create artifact dir: {}", artifact_dir.display()))?;

    let review_json = serde_json::to_string_pretty(raw_json)?;
    let summary_md = render_summary_markdown(raw_json);
    let meta_json = serde_json::to_string_pretty(&serde_json::json!({
        "job_id": job.id,
        "paper_id": job.paper_id,
        "backend": job.backend,
        "token": token,
        "generated_at": Utc::now().to_rfc3339(),
        "pdf_path": job.pdf_path,
        "pdf_hash": job.pdf_hash,
    }))?;

    let review_json_path = artifact_dir.join("review.json");
    let summary_md_path = artifact_dir.join("review.md");
    let meta_json_path = artifact_dir.join("meta.json");

    fs::write(&review_json_path, &review_json)
        .with_context(|| format!("failed to write {}", review_json_path.display()))?;
    fs::write(&summary_md_path, &summary_md)
        .with_context(|| format!("failed to write {}", summary_md_path.display()))?;
    fs::write(&meta_json_path, &meta_json)
        .with_context(|| format!("failed to write {}", meta_json_path.display()))?;

    Ok((review_json, summary_md, meta_json))
}

pub fn render_summary_markdown(raw_json: &Value) -> String {
    let mut out = String::new();
    out.push_str("# Review Summary\n\n");

    if let Some(title) = raw_json.get("title").and_then(Value::as_str) {
        out.push_str(&format!("- **Title**: {title}\n"));
    }
    if let Some(venue) = raw_json.get("venue").and_then(Value::as_str) {
        out.push_str(&format!("- **Venue**: {venue}\n"));
    }
    if let Some(score) = raw_json.get("numerical_score") {
        out.push_str(&format!("- **Estimated Score**: {score}\n"));
    }
    out.push('\n');

    if let Some(sections) = raw_json.get("sections").and_then(Value::as_object) {
        for key in [
            "summary",
            "strengths",
            "weaknesses",
            "detailed_comments",
            "questions",
            "assessment",
            "full_review",
        ] {
            if let Some(value) = sections.get(key).and_then(Value::as_str) {
                let heading = key.replace('_', " ");
                out.push_str(&format!("## {}\n\n{}\n\n", title_case(&heading), value));
            }
        }
    } else if let Some(content) = raw_json.get("content").and_then(Value::as_str) {
        out.push_str("## Content\n\n");
        out.push_str(content);
        out.push('\n');
    } else {
        out.push_str("## Raw JSON\n\n```json\n");
        out.push_str(&serde_json::to_string_pretty(raw_json).unwrap_or_default());
        out.push_str("\n```\n");
    }

    out
}

fn title_case(input: &str) -> String {
    input
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
