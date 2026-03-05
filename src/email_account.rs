use crate::config::Config;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailAccount {
    pub id: String,
    pub provider: String,
    pub email: String,
    pub token_path: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmailAccountStore {
    pub active_account_id: Option<String>,
    pub accounts: Vec<EmailAccount>,
}

pub fn store_path(config: &Config) -> PathBuf {
    config.state_dir().join("email_accounts.json")
}

pub fn load_store(config: &Config) -> Result<EmailAccountStore> {
    let path = store_path(config);
    if !path.exists() {
        return Ok(EmailAccountStore::default());
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read account store {}", path.display()))?;
    let parsed: EmailAccountStore = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse account store {}", path.display()))?;
    Ok(parsed)
}

pub fn save_store(config: &Config, store: &EmailAccountStore) -> Result<()> {
    let path = store_path(config);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create account store directory {}",
                parent.display()
            )
        })?;
    }
    let raw = serde_json::to_string_pretty(store)?;
    std::fs::write(&path, raw)
        .with_context(|| format!("failed to write account store {}", path.display()))?;
    Ok(())
}

pub fn list_accounts(config: &Config) -> Result<Vec<EmailAccount>> {
    let store = load_store(config)?;
    Ok(store.accounts)
}

pub fn active_account(config: &Config) -> Result<Option<EmailAccount>> {
    let store = load_store(config)?;
    let Some(active_id) = store.active_account_id else {
        return Ok(None);
    };
    Ok(store.accounts.into_iter().find(|a| a.id == active_id))
}

pub fn active_email(config: &Config) -> Result<Option<String>> {
    Ok(active_account(config)?.map(|a| a.email))
}

pub fn upsert_account(
    config: &Config,
    provider: &str,
    email: &str,
    token_path: &Path,
) -> Result<EmailAccount> {
    let mut store = load_store(config)?;
    let now = Utc::now().to_rfc3339();
    let token_path_str = token_path.to_string_lossy().to_string();

    let account = if let Some(existing) = store
        .accounts
        .iter_mut()
        .find(|a| a.provider == provider && a.email.eq_ignore_ascii_case(email))
    {
        existing.token_path = token_path_str;
        existing.updated_at = now.clone();
        existing.clone()
    } else {
        let account = EmailAccount {
            id: Uuid::new_v4().to_string(),
            provider: provider.to_string(),
            email: email.to_string(),
            token_path: token_path_str,
            created_at: now.clone(),
            updated_at: now,
        };
        store.accounts.push(account.clone());
        account
    };

    store.active_account_id = Some(account.id.clone());
    save_store(config, &store)?;
    Ok(account)
}

pub fn switch_account(config: &Config, selector: &str) -> Result<EmailAccount> {
    let mut store = load_store(config)?;
    let Some(found) = store
        .accounts
        .iter()
        .find(|a| a.id == selector || a.email.eq_ignore_ascii_case(selector))
        .cloned()
    else {
        return Err(anyhow!("email account not found: {selector}"));
    };
    store.active_account_id = Some(found.id.clone());
    save_store(config, &store)?;
    Ok(found)
}

pub fn remove_account(config: &Config, selector: Option<&str>) -> Result<Option<EmailAccount>> {
    let mut store = load_store(config)?;
    if store.accounts.is_empty() {
        return Ok(None);
    }

    let remove_idx = if let Some(sel) = selector {
        store
            .accounts
            .iter()
            .position(|a| a.id == sel || a.email.eq_ignore_ascii_case(sel))
            .ok_or_else(|| anyhow!("email account not found: {sel}"))?
    } else if let Some(active_id) = &store.active_account_id {
        store
            .accounts
            .iter()
            .position(|a| &a.id == active_id)
            .ok_or_else(|| anyhow!("active account not found in store"))?
    } else {
        0
    };

    let removed = store.accounts.remove(remove_idx);
    if store.active_account_id.as_deref() == Some(removed.id.as_str()) {
        store.active_account_id = store.accounts.first().map(|a| a.id.clone());
    }
    save_store(config, &store)?;
    Ok(Some(removed))
}

pub fn resolve_submission_email(
    config: &Config,
    backend: &str,
    explicit_email: Option<&str>,
) -> Result<String> {
    if let Some(email) = explicit_email
        && !email.trim().is_empty()
    {
        return Ok(email.to_string());
    }

    match backend {
        "stanford" => {
            if !config.providers.stanford.email.trim().is_empty() {
                return Ok(config.providers.stanford.email.clone());
            }
            if let Some(email) = active_email(config)? {
                return Ok(email);
            }
            Err(anyhow!(
                "no email available for backend=stanford. set providers.stanford.email or run `reviewloop email login`"
            ))
        }
        _ => Ok(explicit_email.unwrap_or_default().to_string()),
    }
}
