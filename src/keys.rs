//! Загрузка подписывающего ключа бота.
//!
//! Ключ читается ТОЛЬКО из локального файла (как у solana-keygen) или из
//! переменной окружения FEEBOT_KEYPAIR (base58). Он никогда не логируется
//! и не хранится в конфиге/репозитории.

use anyhow::{anyhow, Context, Result};
use solana_sdk::signature::{Keypair, Signer};
use std::path::Path;

/// Грузит keypair. Поддерживает два формата файла:
///   1) JSON-массив 64 байт (формат solana-keygen / `id.json`);
///   2) строку base58 секретного ключа.
/// Либо, если `path` пуст, берёт base58 из env FEEBOT_KEYPAIR.
pub fn load_keypair(path: &str) -> Result<Keypair> {
    if path.is_empty() {
        let b58 = std::env::var("FEEBOT_KEYPAIR")
            .map_err(|_| anyhow!("keypair_path пуст и нет env FEEBOT_KEYPAIR"))?;
        return from_base58(b58.trim());
    }

    let p = Path::new(path);
    let raw = std::fs::read_to_string(p)
        .with_context(|| format!("не читается keypair-файл {}", p.display()))?;
    let raw = raw.trim();

    if raw.starts_with('[') {
        let bytes: Vec<u8> = serde_json::from_str(raw).context("битый JSON-массив ключа")?;
        Keypair::from_bytes(&bytes).map_err(|e| anyhow!("плохой keypair: {e}"))
    } else {
        from_base58(raw)
    }
}

fn from_base58(s: &str) -> Result<Keypair> {
    let bytes = bs58::decode(s).into_vec().context("ключ не декодится из base58")?;
    Keypair::from_bytes(&bytes).map_err(|e| anyhow!("плохой keypair: {e}"))
}

/// Публичный адрес ключа — единственное, что можно светить в логах.
pub fn pubkey_str(kp: &Keypair) -> String {
    kp.pubkey().to_string()
}
