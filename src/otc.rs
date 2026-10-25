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

    /// Приходит ТОЛЬКО в `/api/coins?mint=X` (в списке страниц его нет).
    /// Содержит owed / distributed / lastPaidHolders — сигналы отбора и тайминга.
    #[serde(default)]
    pub rewards: Option<OtcRewards>,

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
    /// Изменение цены за 24ч (%) — моментум-фильтр (не входить в обвал).
    #[serde(default, alias = "change24h")]
    pub change24h: Option<f64>,
}

/// Объект `rewards` из `/api/coins?mint=X` — ЭТО ГЛАВНЫЙ ИСТОЧНИК СИГНАЛА.
/// Все суммы в лампортах.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct OtcRewards {
    /// Накоплено холдерам и ждёт раздачи (ТАЙМИНГ входа: чем ближе к порогу, тем скорее раунд).
    #[serde(default)]
    pub owed: u64,
    /// Роздано холдерам за всю жизнь монеты (ФИЛЬТР КАЧЕСТВА: монета реально платит).
    #[serde(default)]
    pub distributed: u64,
    /// Заработано всего (фисы).
    #[serde(default)]
    pub earned: u64,
    /// Сколько холдеров делили ПОСЛЕДНЮЮ выплату — прямой прокси флоата:
    /// чем меньше, тем больше наша доля. Избавляет от тяжёлого getTokenLargestAccounts.
    #[serde(default, alias = "lastPaidHolders")]
    pub last_paid_holders: u64,
    /// Сколько раундов уже прошло.
    #[serde(default)]
    pub payouts: u64,
    #[serde(default, alias = "lastDistributedAt")]
    pub last_distributed_at: u64,
}

/// Состояние «котла» монеты: /api/vault?mint=X. Раунд запускается, когда
/// накопленный пот (`waitingSol`) перевалит порог (`floorSol`). Воркер OTC
/// клеймит фисы pump В ПОТ за ~40с до раздачи — значит РЕЗКИЙ РОСТ `waitingSol`
/// это прекурсор раздачи, и ловится он бесплатным HTTP (без RPC и комиссий).
#[derive(Debug, Clone, Deserialize)]
pub struct OtcVault {
    #[serde(default, alias = "waitingSol", alias = "waiting_sol")]
    pub waiting_sol: f64,
    #[serde(default, alias = "floorSol", alias = "floor_sol")]
    pub floor_sol: f64,
    /// Жива ли ещё бондинг-кривая pump у монеты.
    #[serde(default, alias = "curveExists", alias = "curve_exists")]
    pub curve_exists: bool,
    /// Монета ушла с кривой на AMM — инструкций buy/sell программы pump у неё
    /// больше нет, скрейпить coin_fee бессмысленно.
    #[serde(default)]
    pub graduated: bool,
}

impl OtcClient {
    pub fn new(cfg: &OtcConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: cfg.api_base.trim_end_matches('/').to_string(),
        }
    }

    /// Список монет (борда лаунчера). Без параметров отдаёт лишь ПЕРВУЮ страницу —
    /// на борде ~51k монет, поэтому для отбора используем `coins_by_volume`.
    pub async fn coins(&self) -> Result<Vec<OtcCoin>> {
        self.coins_page(0, 50, "volume24h").await
    }

    /// Одна страница борды с сортировкой (работает `sort=volume24h&dir=desc`).
    pub async fn coins_page(&self, page: usize, size: usize, sort: &str) -> Result<Vec<OtcCoin>> {
        let url = format!(
            "{}/api/coins?page={page}&size={size}&sort={sort}&dir=desc",
            self.base
        );
        let resp = self.http.get(&url).send().await.context("otc coins send")?;
        let text = resp.text().await.context("otc coins body")?;
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

    /// Несколько страниц, отсортированных по объёму (там, где копятся фисы).
    pub async fn coins_by_volume(&self, pages: usize, size: usize) -> Result<Vec<OtcCoin>> {
        let mut out = Vec::new();
        for p in 0..pages.max(1) {
            match self.coins_page(p, size, "volume24h").await {
                Ok(mut v) => {
                    if v.is_empty() {
                        break;
                    }
                    out.append(&mut v);
                }
                Err(e) => {
                    if p == 0 {
                        return Err(e);
                    }
                    break; // частичный список лучше, чем ничего
                }
            }
        }
        Ok(out)
    }

    /// Детали ОДНОЙ монеты — единственный путь получить объект `rewards`
    /// (owed / distributed / lastPaidHolders).
    pub async fn coin_detail(&self, mint: &str) -> Result<OtcCoin> {
        let url = format!("{}/api/coins?mint={mint}", self.base);
        let resp = self.http.get(&url).send().await.context("otc coin send")?;
        let text = resp.text().await.context("otc coin body")?;
        #[derive(Deserialize)]
        struct Wrap {
            #[serde(alias = "coins", alias = "data", alias = "items")]
            coins: Vec<OtcCoin>,
        }
        let w: Wrap = serde_json::from_str(&text).context("otc coin parse")?;
        w.coins
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("монета {mint} не найдена в /api/coins?mint="))
    }

    /// Котёл монеты — прекурсор раздачи. Дёшево (обычный HTTP), поэтому можно
    /// опрашивать раз в секунду, не трогая RPC-лимиты.
    pub async fn vault(&self, mint: &str) -> Result<OtcVault> {
        let url = format!("{}/api/vault?mint={mint}", self.base);
        let resp = self.http.get(&url).send().await.context("otc vault send")?;
        let text = resp.text().await.context("otc vault body")?;
        serde_json::from_str::<OtcVault>(&text).context("otc vault parse")
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
