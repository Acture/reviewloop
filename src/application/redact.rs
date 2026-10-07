use serde_json::Value;

pub(crate) const REDACTED: &str = "[redacted]";

/// Replace every occurrence of each non-empty token in `text`.
pub(crate) fn redact_text(text: &str, tokens: &[&str]) -> String {
    tokens
        .iter()
        .filter(|token| !token.is_empty())
        .fold(text.to_string(), |text, token| {
            text.replace(token, REDACTED)
        })
}

/// Redact `tokens` inside every string and object key of `value`, and
/// replace the value of every object key named `token`.
pub(crate) fn redact_value(value: Value, tokens: &[&str]) -> Value {
    match value {
        Value::String(text) => Value::String(redact_text(&text, tokens)),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| redact_value(item, tokens))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| {
                    let item = if key == "token" {
                        Value::String(REDACTED.to_string())
                    } else {
                        redact_value(item, tokens)
                    };
                    (redact_text(&key, tokens), item)
                })
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_tokens_in_strings_keys_and_token_fields() {
        let value = json!({
            "token": "anything",
            "url": "https://example.test/api/review/tok-123",
            "nested": [{"note": "tok-123 twice old-9"}, 7],
            "by_tok-123": true,
        });
        assert_eq!(
            redact_value(value, &["tok-123", "old-9"]),
            json!({
                "token": "[redacted]",
                "url": "https://example.test/api/review/[redacted]",
                "nested": [{"note": "[redacted] twice [redacted]"}, 7],
                "by_[redacted]": true,
            })
        );
    }

    #[test]
    fn empty_or_missing_tokens_leave_text_alone() {
        assert_eq!(redact_text("abc", &[""]), "abc");
        assert_eq!(redact_text("abc", &[]), "abc");
    }
}
