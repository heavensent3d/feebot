//! Тонкая обёртка над RpcClient + хелперы чтения состояния.

use anyhow::{anyhow, Result};
use solana_account_decoder::UiAccountEncoding;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
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
        let v: serde_json::Value = resp.json().await?;
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
    /// Возвращает (адрес аккаунта, mint, amount, decimals).
    pub async fn token_accounts(
        &self,
        owner: &Pubkey,
        token_program: &Pubkey,
    ) -> Result<Vec<TokenAccount>> {
        // filter: SPL token account data size = 165 (классический).
        // Для token-2022 размер переменный, поэтому фильтруем по owner-memcmp
        // (offset 32 = owner в token account). Проще — через RPC-метод ниже.
        let cfg = RpcProgramAccountsConfig {
            filters: Some(vec![
                RpcFilterType::Memcmp(Memcmp::new_base58_encoded(32, owner.as_ref())),
            ]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(self.commitment),
                ..Default::default()
            },
            ..Default::default()
        };
        let accs = self
            .client
            .get_program_accounts_with_config(token_program, cfg)
            .await?;

        let mut out = Vec::with_capacity(accs.len());
        for (pubkey, acc) in accs {
            if let Some(ta) = parse_token_account(&acc.data) {
                out.push(TokenAccount {
                    address: pubkey,
                    mint: ta.0,
                    amount: ta.1,
                });
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
