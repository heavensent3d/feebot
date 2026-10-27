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

    /// Отправить тело JSON-RPC с ретраями на 429. Бесплатный тир отдаёт
    /// «Too Many Requests» регулярно, и без ретраев это выглядело как «данных
    /// нет»: скрейп coin_fee падал и уводил ВПОЛНЕ ЖИВУЮ монету в
    /// no_template-кулдаун на 180 секунд. Rate-limit — это «спроси позже», а не
    /// «такого нет».
    async fn post_with_retry(&self, body: &serde_json::Value) -> Result<(reqwest::StatusCode, String)> {
        let mut delay = std::time::Duration::from_millis(250);
        for attempt in 0..4 {
            let resp = self.rq.post(&self.http_url).json(body).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if status.as_u16() != 429 {
                return Ok((status, text));
            }
            if attempt < 3 {
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
        Err(anyhow!("rpc: 429 после 4 попыток (rate-limit держится)"))
    }

    /// Сырой JSON-RPC (для методов, где нужен разбор ответа руками, напр. шаблон tx).
    pub async fn raw(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let (status, text) = self.post_with_retry(&body).await?;
        // Парсим вручную: на ошибке RPC отдаёт не-JSON (HTML/пусто), и
        // resp.json() падал опаковым "error decoding response body".
        let v: serde_json::Value = serde_json::from_str(&text).map_err(|_| {
            let snip: String = text.chars().take(120).collect();
            anyhow!("rpc {method}: не-JSON ответ (HTTP {status}): {snip}")
        })?;
        if let Some(e) = v.get("error") {
            return Err(anyhow!("rpc {method}: {e}"));
        }
        Ok(v.get("result").cloned().unwrap_or(serde_json::Value::Null))
    }

    /// Пакетный JSON-RPC: массив запросов ОДНИМ POST. Нужен там, где иначе пришлось
    /// бы слать десятки вызовов подряд и упираться в rate-limit (скрейп coin_fee).
    /// Возвращает результаты В ПОРЯДКЕ запросов; элемент = None, если по нему
    /// пришла ошибка или пусто (ответ батча приходит В ПРОИЗВОЛЬНОМ порядке, поэтому
    /// раскладываем по `id`).
    pub async fn batch(
        &self,
        calls: &[(&str, serde_json::Value)],
    ) -> Result<Vec<Option<serde_json::Value>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let body: Vec<serde_json::Value> = calls
            .iter()
            .enumerate()
            .map(|(i, (m, p))| serde_json::json!({"jsonrpc":"2.0","id":i,"method":m,"params":p}))
            .collect();
        let (status, text) = self
            .post_with_retry(&serde_json::Value::Array(body))
            .await?;
        let v: serde_json::Value = serde_json::from_str(&text).map_err(|_| {
            let snip: String = text.chars().take(120).collect();
            anyhow!("rpc batch: не-JSON ответ (HTTP {status}): {snip}")
        })?;
        // Батч может вернуться и одиночной ошибкой (напр. 429) вместо массива.
        let arr = match v.as_array() {
            Some(a) => a,
            None => {
                let e = v.get("error").cloned().unwrap_or(v.clone());
                return Err(anyhow!("rpc batch: {e}"));
            }
        };
        let mut out = vec![None; calls.len()];
        for item in arr {
            let Some(i) = item.get("id").and_then(|x| x.as_u64()) else { continue };
            let Some(slot) = out.get_mut(i as usize) else { continue };
            if item.get("error").is_some() {
                continue;
            }
            *slot = item
                .get("result")
                .cloned()
                .filter(|r| !r.is_null());
        }
        Ok(out)
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
