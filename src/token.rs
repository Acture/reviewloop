use regex::Regex;

pub fn extract_review_token(text: &str) -> Option<String> {
    // Preferred: token inside paperreview.ai review URL.
    let url_re = Regex::new(r"https?://paperreview\.ai/review\?token=([A-Za-z0-9_-]+)").ok()?;
    if let Some(cap) = url_re.captures(text) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }

    // Fallback: explicit token patterns seen in notification mails.
    let token_re = Regex::new(r"(?i)(?:token|access\s*token)[:\s]+([A-Za-z0-9_-]{12,})").ok()?;
    if let Some(cap) = token_re.captures(text) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }

    None
}

pub fn extract_token_with_pattern(text: &str, pattern: &str) -> Option<String> {
    let re = Regex::new(pattern).ok()?;
    let cap = re.captures(text)?;
    cap.get(1).map(|m| m.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::extract_review_token;

    #[test]
    fn extracts_token_from_review_url() {
        let text = "Your review is ready: https://paperreview.ai/review?token=abc123_xyz";
        assert_eq!(extract_review_token(text).as_deref(), Some("abc123_xyz"));
    }

    #[test]
    fn extracts_token_from_label() {
        let text = "Access token: stanford_token_98765";
        assert_eq!(
            extract_review_token(text).as_deref(),
            Some("stanford_token_98765")
        );
    }

    #[test]
    fn extracts_token_with_custom_regex() {
        let text = "backend=stanford token=abcDEF123";
        assert_eq!(
            super::extract_token_with_pattern(text, r"token=([A-Za-z0-9_-]+)").as_deref(),
            Some("abcDEF123")
        );
    }
}
