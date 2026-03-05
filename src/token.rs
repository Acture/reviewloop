use regex::Regex;

pub fn extract_review_token(text: &str) -> Option<String> {
    let normalized = normalize_email_encoded_text(text);
    for candidate in [normalized.as_str(), text] {
        if let Some(token) = extract_from_candidate(candidate) {
            return Some(token);
        }
    }

    None
}

fn extract_from_candidate(text: &str) -> Option<String> {
    // Preferred: token inside paperreview.ai review URL, including encoded variants.
    let paperreview_url_re =
        Regex::new(r"(?i)paperreview\.ai/review\?token(?:=|%3d)([A-Za-z0-9_-]+)").ok()?;
    if let Some(cap) = paperreview_url_re.captures(text) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }

    // Secondary: any token query parameter in encoded/raw URLs.
    let token_query_re = Regex::new(r"(?i)(?:[?&]|(?:^|\W))token(?:=|%3d)([A-Za-z0-9_-]{12,})")
        .ok()?;
    if let Some(cap) = token_query_re.captures(text) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }

    // Fallback: explicit token labels in plain text mails.
    let token_label_re =
        Regex::new(r"(?i)(?:token|access\s*token)[:\s]+([A-Za-z0-9_-]{12,})").ok()?;
    if let Some(cap) = token_label_re.captures(text) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }

    None
}

fn normalize_email_encoded_text(text: &str) -> String {
    let qp = decode_quoted_printable_ascii(text.as_bytes());
    qp.replace("&amp;", "&")
}

fn decode_quoted_printable_ascii(input: &[u8]) -> String {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'=' {
            // Soft line break: =\r\n or =\n
            if i + 2 < input.len() && input[i + 1] == b'\r' && input[i + 2] == b'\n' {
                i += 3;
                continue;
            }
            if i + 1 < input.len() && input[i + 1] == b'\n' {
                i += 2;
                continue;
            }
            // Hex escape: =3D style
            if i + 2 < input.len()
                && input[i + 1].is_ascii_digit()
                && let (Some(h), Some(l)) = (hex_nibble(input[i + 1]), hex_nibble(input[i + 2]))
            {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(input[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
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
    fn extracts_token_from_google_safe_redirect_q_param() {
        let text = "https://www.google.com/url?hl=zh-CN&q=https://paperreview.ai/review?token%3Dabc123_xyz&source=gmail";
        assert_eq!(extract_review_token(text).as_deref(), Some("abc123_xyz"));
    }

    #[test]
    fn extracts_token_from_quoted_printable_email_fragment() {
        let text = "https://paperreview.ai/review?token=3Dabc123xyz_789";
        assert_eq!(extract_review_token(text).as_deref(), Some("abc123xyz_789"));
    }

    #[test]
    fn extracts_token_from_quoted_printable_wrapped_line() {
        let text = "https://paperreview.ai/review?token=abc123xyz_=\r\n789token";
        assert_eq!(
            super::normalize_email_encoded_text(text),
            "https://paperreview.ai/review?token=abc123xyz_789token"
        );
        assert_eq!(extract_review_token(text).as_deref(), Some("abc123xyz_789token"));
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

    #[test]
    fn prefers_review_url_when_multiple_token_like_strings_exist() {
        let text = "Access token: old_token_123 https://paperreview.ai/review?token=new_token_456";
        assert_eq!(extract_review_token(text).as_deref(), Some("new_token_456"));
    }

    #[test]
    fn returns_none_when_no_token_found() {
        assert_eq!(extract_review_token("hello world"), None);
    }

    #[test]
    fn extracts_token_from_real_gmail_saved_html() {
        let html = r#"<a href="https://paperreview.ai/review?token=exnyCz8RbzZ459LJaQola7kT6eBHun-3M7ALe8wpFSw"
 style="display:block" target="_blank"
 data-saferedirecturl="https://www.google.com/url?hl=zh-CN&amp;q=https://paperreview.ai/review?token%3DexnyCz8RbzZ459LJaQola7kT6eBHun-3M7ALe8wpFSw&amp;source=gmail">View Review</a>"#;
        assert_eq!(
            extract_review_token(html).as_deref(),
            Some("exnyCz8RbzZ459LJaQola7kT6eBHun-3M7ALe8wpFSw")
        );
    }
}
