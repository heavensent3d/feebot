//! Клиент публичного API otcdesks.cash.
//!
//! ⚠️ Имена полей в ответах не задокументированы публично — ниже стоят
//! наиболее вероятные варианты с алиасами. Проверь реальный JSON
//! (`curl https://otcdesks.cash/api/coins`) и поправь алиасы под факт.
//! Парсер намеренно толерантный: неизвестные поля игнорируются, отсутствующие
//! дефолтятся, чтобы бот не падал на изменении схемы.

use crate::config::OtcConfig;
use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone)]
pub struct OtcClient {
    http: reqwest::Client,
    base: String,
}

/// Одна монета лаунчера. Поля — best-effort, с алиасами под возможные ключи.
#[derive(Debug, Clone, Deserialize)]
pub struct OtcCoin {
    #[serde(alias = "mint", alias = "address")]
    pub mint: String,

    #[serde(default, alias = "symbol", alias = "ticker")]
    pub symbol: Option<String>,

    /// Mint reward-актива (стока), которым платит монета.
    #[serde(default, alias = "rewardMint", alias = "reward_mint")]
    pub reward_mint: Option<String>,

    /// Корзина для ETF-монет (ротация стоков).
    #[serde(default, alias = "rewardBasket", alias = "reward_basket")]
    pub reward_basket: Option<Vec<String>>,

    /// На что торгуется монета (SOL или сток). Влияет на расчёт.
    #[serde(default, alias = "pairMint", alias = "pair_mint")]
    pub pair_mint: Option<String>,

    /// Рыночные метрики — API отдаёт их ВЛОЖЕННО в объекте `snapshot`.
    #[serde(default)]
    pub snapshot: Option<Snapshot>,

    /// Накопленная доля держателей в SOL (в /api/coins НЕ приходит — оставлено
    /// на случай, если появится в другом эндпоинте).
    #[serde(
        default,
        alias = "pendingRewardSol",
        alias = "pending_reward_sol",
        alias = "pendingSol",
        alias = "accruedSol"
    )]
    pub pending_reward_sol: Option<f64>,

    #[serde(default, alias = "venue")]
    pub venue: Option<String>,
}

/// Вложенный snapshot из /api/coins (факт, проверено на живом ответе).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Snapshot {
    #[serde(default, alias = "marketCap", alias = "market_cap")]
    pub market_cap: Option<f64>,
    #[serde(default, alias = "volume24h", alias = "volume_24h")]
    pub volume24h: Option<f64>,
    #[serde(default)]
    pub liquidity: Option<f64>,
    #[serde(default)]
    pub holders: Option<u64>,
    #[serde(default, alias = "usdPrice")]
    pub usd_price: Option<f64>,
}

impl OtcClient {
    pub fn new(cfg: &OtcConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: cfg.api_base.trim_end_matches('/').to_string(),
        }
    }

    /// Список монет (борда лаунчера).
    pub async fn coins(&self) -> Result<Vec<OtcCoin>> {
        let url = format!("{}/api/coins", self.base);
        let resp = self.http.get(&url).send().await.context("otc coins send")?;
        let text = resp.text().await.context("otc coins body")?;
        // API может вернуть либо массив, либо {coins:[...]}.
        if let Ok(v) = serde_json::from_str::<Vec<OtcCoin>>(&text) {
            return Ok(v);
        }
        #[derive(Deserialize)]
        struct Wrap {
            #[serde(alias = "coins", alias = "data", alias = "items")]
            coins: Vec<OtcCoin>,
        }
        let w: Wrap = serde_json::from_str(&text).context("otc coins parse")?;
        Ok(w.coins)
    }

    /// Список стоков, которые pump принимает как quote (из docs: /api/pairs).
    pub async fn pairs(&self) -> Result<Vec<String>> {
        let url = format!("{}/api/pairs", self.base);
        let resp = self.http.get(&url).send().await.context("otc pairs send")?;
        let v: serde_json::Value = resp.json().await.context("otc pairs json")?;
        // Пытаемся вытащить список mint-строк из любого разумного формата.
        let mut out = Vec::new();
        match v {
            serde_json::Value::Array(a) => {
                for it in a {
                    if let Some(s) = it.as_str() {
                        out.push(s.to_string());
                    } else if let Some(m) = it.get("mint").and_then(|x| x.as_str()) {
                        out.push(m.to_string());
                    }
                }
            }
            serde_json::Value::Object(o) => {
                if let Some(serde_json::Value::Array(a)) =
                    o.get("pairs").or_else(|| o.get("data"))
                {
                    for it in a {
                        if let Some(s) = it.as_str() {
                            out.push(s.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(out)
    }
}
