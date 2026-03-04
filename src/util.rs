use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

pub fn sha256_file(path: &Path) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("failed to open file: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read file: {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

pub fn to_rfc3339(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339()
}

pub fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid RFC3339 timestamp: {value}"))?
        .with_timezone(&Utc))
}

pub fn compute_next_poll_at(
    now: DateTime<Utc>,
    schedule_minutes: &[u64],
    attempt: u32,
    jitter_percent: u8,
) -> DateTime<Utc> {
    let minutes = if schedule_minutes.is_empty() {
        10_u64
    } else {
        let idx = usize::min(attempt as usize, schedule_minutes.len() - 1);
        schedule_minutes[idx]
    };

    let base_secs = (minutes * 60) as i64;
    if jitter_percent == 0 {
        return now + Duration::seconds(base_secs);
    }

    let jitter_bound = (base_secs as f64 * (jitter_percent as f64 / 100.0)).round() as i64;
    if jitter_bound <= 0 {
        return now + Duration::seconds(base_secs);
    }

    let jitter = rand::thread_rng().gen_range(-jitter_bound..=jitter_bound);
    let delay_secs = i64::max(base_secs + jitter, 60);
    now + Duration::seconds(delay_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_starts_at_ten_minutes() {
        let now = Utc::now();
        let next = compute_next_poll_at(now, &[10, 20, 40, 60], 0, 0);
        assert_eq!((next - now).num_minutes(), 10);
    }

    #[test]
    fn backoff_caps_to_last_entry() {
        let now = Utc::now();
        let next = compute_next_poll_at(now, &[10, 20, 40, 60], 99, 0);
        assert_eq!((next - now).num_minutes(), 60);
    }
}
