//! Продажа наград (токенизированных акций) и обмен AMM-монет через Jupiter v6.
//!
//! Награды приходят в Token-2022 стоках с тонкой ликвидностью, поэтому
//! продаём их агрегатором с широким слиппеджем, пачками по мере накопления.
//! Разобранный бот делал ровно это (роутер FLASHX/Jupiter + durable nonce).

use crate::config::JupiterConfig;
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;

#[derive(Clone)]
pub struct Jupiter {
    http: reqwest::Client,
    cfg: JupiterConfig,
}

impl Jupiter {
    pub fn new(cfg: JupiterConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            cfg,
        }
    }

    /// Котировка. Возвращает сырой JSON quote (его же скармливаем в /swap).
    pub async fn quote(
        &self,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount: u64,
        slippage_bps: u64,
    ) -> Result<serde_json::Value> {
        let url = &self.cfg.quote_url;
        let resp = self
            .http
            .get(url)
            .query(&[
                ("inputMint", input_mint.to_string()),
                ("outputMint", output_mint.to_string()),
                ("amount", amount.to_string()),
                ("slippageBps", slippage_bps.to_string()),
                ("restrictIntermediateTokens", "true".to_string()),
            ])
            .send()
            .await
            .context("jupiter quote send")?;
        let v: serde_json::Value = resp.json().await.context("jupiter quote json")?;
        if v.get("error").is_some() || v.get("outAmount").is_none() {
            return Err(anyhow!("jupiter quote: нет маршрута ({v})"));
        }
        Ok(v)
    }

    /// Сколько SOL (lamports) даёт quote на выходе.
    pub fn out_amount(quote: &serde_json::Value) -> u64 {
        quote
            .get("outAmount")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    }

    /// Построить и ПОДПИСАТЬ swap-транзакцию (Jupiter отдаёт v0-tx со своим
    /// blockhash; мы её пере-подписываем ключом бота).
    pub async fn swap_signed(
        &self,
        quote: &serde_json::Value,
        signer: &Keypair,
        cu_price_micro_lamports: u64,
    ) -> Result<VersionedTransaction> {
        let body = serde_json::json!({
            "quoteResponse": quote,
            "userPublicKey": signer.pubkey().to_string(),
            "wrapAndUnwrapSol": true,
            "dynamicComputeUnitLimit": true,
            "prioritizationFeeLamports": {
                "priorityLevelWithMaxLamports": {
                    "maxLamports": cu_price_micro_lamports,
                    "priorityLevel": "high"
                }
            }
        });
        let resp = self
            .http
            .post(&self.cfg.swap_url)
            .json(&body)
            .send()
            .await
            .context("jupiter swap send")?;
        let v: serde_json::Value = resp.json().await.context("jupiter swap json")?;
        let tx_b64 = v
            .get("swapTransaction")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow!("jupiter swap: нет swapTransaction ({v})"))?;
        let bytes = B64.decode(tx_b64).context("swapTransaction base64")?;
        let unsigned: VersionedTransaction =
            bincode::deserialize(&bytes).context("deserialize swap tx")?;
        // Пере-подписываем сообщение ключом бота.
        let signed = VersionedTransaction::try_new(unsigned.message, &[signer])
            .map_err(|e| anyhow!("sign swap tx: {e}"))?;
        Ok(signed)
    }

    pub fn cfg(&self) -> &JupiterConfig {
        &self.cfg
    }

    /// Текущий курс SOL в USD (Jupiter price API). Для оценки EV/капы.
    pub async fn sol_price_usd(&self) -> Result<f64> {
        const WSOL: &str = "So11111111111111111111111111111111111111112";
        let url = format!("https://lite-api.jup.ag/price/v3?ids={WSOL}");
        let v: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await
            .context("sol price send")?
            .json()
            .await
            .context("sol price json")?;
        v.get(WSOL)
            .and_then(|m| m.get("usdPrice"))
            .and_then(|x| x.as_f64())
            .ok_or_else(|| anyhow!("нет usdPrice в ответе Jupiter"))
    }
}
