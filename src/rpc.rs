//! Тонкая обёртка над RpcClient + хелперы чтения состояния.

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
pub struct Rpc {
    pub client: Arc<RpcClient>,
    pub commitment: CommitmentConfig,
    http_url: String,
    rq: reqwest::Client,
}

impl Rpc {
    pub fn new(http: &str, commitment: &str) -> Self {
        let c = commitment_from_str(commitment);
        Self {
            client: Arc::new(RpcClient::new_with_commitment(http.to_string(), c)),
            commitment: c,
            http_url: http.to_string(),
            rq: reqwest::Client::new(),
        }
    }

    /// Сырой JSON-RPC (для методов, где нужен разбор ответа руками, напр. шаблон tx).
    pub async fn raw(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let resp = self.rq.post(&self.http_url).json(&body).send().await?;
        let status = resp.status();
        // Читаем текст и парсим вручную: на rate-limit/ошибке RPC отдаёт не-JSON
        // (HTML/пусто), и resp.json() падал опаковым "error decoding response body".
        let text = resp.text().await?;
        let v: serde_json::Value = serde_json::from_str(&text).map_err(|_| {
            let snip: String = text.chars().take(120).collect();
            anyhow!("rpc {method}: не-JSON ответ (HTTP {status}): {snip}")
        })?;
        if let Some(e) = v.get("error") {
            return Err(anyhow!("rpc {method}: {e}"));
        }
        Ok(v.get("result").cloned().unwrap_or(serde_json::Value::Null))
    }

    pub async fn latest_blockhash(&self) -> Result<Hash> {
        Ok(self.client.get_latest_blockhash().await?)
    }

    pub async fn slot(&self) -> Result<u64> {
        Ok(self.client.get_slot().await?)
    }

    pub async fn lamports(&self, who: &Pubkey) -> Result<u64> {
        Ok(self.client.get_balance(who).await?)
    }

    /// Сырые данные аккаунта (None, если аккаунта нет).
    pub async fn account(&self, key: &Pubkey) -> Result<Option<Account>> {
        match self.client.get_account_with_commitment(key, self.commitment).await {
            Ok(r) => Ok(r.value),
            Err(e) => Err(anyhow!("get_account {key}: {e}")),
        }
    }

    /// Баланс SPL-токен-аккаунта (ui amount в базовых единицах, u64).
    pub async fn token_amount(&self, token_account: &Pubkey) -> Result<u64> {
        let bal = self.client.get_token_account_balance(token_account).await?;
        bal.amount.parse::<u64>().map_err(|e| anyhow!("amount parse: {e}"))
    }

    /// Все токен-аккаунты владельца под конкретной token-программой.
    /// Через `getTokenAccountsByOwner` (owner-scoped, лёгкий) — а НЕ
    /// `getProgramAccounts` (тяжёлый, гейтится на free/Operator RPC и у Helius
    /// просит getProgramAccountsV2). Возвращает (адрес, mint, amount).
    pub async fn token_accounts(
        &self,
        owner: &Pubkey,
        token_program: &Pubkey,
    ) -> Result<Vec<TokenAccount>> {
        let res = self
            .raw(
                "getTokenAccountsByOwner",
                serde_json::json!([
                    owner.to_string(),
                    {"programId": token_program.to_string()},
                    {"encoding": "base64"}
                ]),
            )
            .await?;
        let mut out = Vec::new();
        if let Some(arr) = res["value"].as_array() {
            for it in arr {
                let Some(addr) = it["pubkey"].as_str().and_then(|s| Pubkey::from_str(s).ok()) else {
                    continue;
                };
                let Some(data_b64) = it["account"]["data"].get(0).and_then(|d| d.as_str()) else {
                    continue;
                };
                let Ok(data) = B64.decode(data_b64) else { continue };
                if let Some((mint, amount)) = parse_token_account(&data) {
                    out.push(TokenAccount { address: addr, mint, amount });
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct TokenAccount {
    pub address: Pubkey,
    pub mint: Pubkey,
    pub amount: u64,
}

/// Минимальный парс SPL/Token-2022 account: mint(0..32), owner(32..64),
/// amount(64..72). У token-2022 дальше идут расширения, но первые 72 байта
/// совпадают с классическим layout.
fn parse_token_account(data: &[u8]) -> Option<(Pubkey, u64)> {
    if data.len() < 72 {
        return None;
    }
    let mint = Pubkey::try_from(&data[0..32]).ok()?;
    let amount = u64::from_le_bytes(data[64..72].try_into().ok()?);
    Some((mint, amount))
}

pub fn commitment_from_str(s: &str) -> CommitmentConfig {
    match s {
        "finalized" => CommitmentConfig::finalized(),
        "confirmed" => CommitmentConfig::confirmed(),
        _ => CommitmentConfig::processed(),
    }
}

pub fn pk(s: &str) -> Result<Pubkey> {
    Pubkey::from_str(s).map_err(|e| anyhow!("плохой pubkey {s}: {e}"))
}
