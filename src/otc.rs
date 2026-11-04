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

    /// ИГРА, ПО КОТОРОЙ МОНЕТА ПЛАТИТ. Пусто = обычная раздача держателям, под
    /// которую написан бот. `"koth"` = King of the Hill: там не доля, а
    /// winner-takes-all (см. `hooks_of` в strategy.rs). Приходит ТОЛЬКО в
    /// `/api/coins?mint=X`: в пагинации поля нет вообще — проверено 04.11.2026
    /// на 26 760 записях списка, hook не встретился ни разу, при том что
    /// per-mint запрос той же монеты его отдаёт.
    #[serde(default, alias = "hook")]
    pub hook: Option<String>,

    /// Unix-время создания монеты. ГЛАВНЫЙ признак отбора: разбор 333 входов
    /// danek показал медианный возраст монеты на входе **5 минут**
    /// (25% — 3 мин, 75% — 8 мин). Фисы собираются на запуске, а не потом.
    #[serde(default, alias = "createdAt", alias = "created_at")]
    pub created_at: Option<i64>,

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
    /// Когда воркер OTC последний раз КЛЕЙМИЛ фисы в котёл. Это НАСТОЯЩИЙ
    /// прекурсор раздачи: по разобранной ленте от клейма до раздачи ~37 секунд.
    /// Полнота котла говорит лишь «выплата назрела», а воркер приходит по своему
    /// расписанию и может тянуть минутами — стартовать пролив по котлу значит
    /// сжечь бюджет впустую (проверено: 0.08 SOL за 41 секунду, раздачи не было).
    #[serde(default, alias = "lastClaimAt")]
    pub last_claim_at: u64,
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

impl OtcVault {
    /// НАСТОЯЩИЙ порог выплаты холдерам, в SOL.
    ///
    /// ⚠️ `floorSol` из API им НЕ является. Замер 25.10.2026 по 70 монетам: у 61
    /// он равен 0.002, у 9 — 0.1, тогда как сам OTC в интерфейсе монеты пишет
    /// «Fees stay here until they reach 0.10 SOL, then the whole balance is
    /// swapped at once». Фактические выплаты по 30 монетам это подтверждают:
    /// минимальная 0.0899 SOL, медиана 0.1904, 29 из 30 не ниже 0.10, и НИ ОДНОЙ
    /// около 0.002.
    ///
    /// На этом бот брал мёртвые монеты: деля на 0.002 вместо 0.10, он видел
    /// «котёл заполнен 87.8%» там, где накоплено было 0.65% от выплаты.
    /// Берём максимум из настроечного порога и того, что отдал API.
    pub fn payout_floor(&self, cfg_floor_sol: f64) -> f64 {
        self.floor_sol.max(cfg_floor_sol).max(1e-9)
    }

    /// Насколько котёл полон относительно НАСТОЯЩЕГО порога выплаты, в процентах.
    pub fn fill_pct(&self, cfg_floor_sol: f64) -> f64 {
        100.0 * self.waiting_sol / self.payout_floor(cfg_floor_sol)
    }
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

    /// Несколько страниц борды с заданной сортировкой. См. `OtcConfig::sort`:
    /// неизвестное API значение даёт её дефолтный порядок — свежие монеты с
    /// крошечным капом и 1–8 холдерами, то есть именно наш пруд.
    pub async fn coins_sorted(&self, pages: usize, size: usize, sort: &str) -> Result<Vec<OtcCoin>> {
        let mut out = Vec::new();
        for p in 0..pages.max(1) {
            match self.coins_page(p, size, sort).await {
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


    /// ВСЯ БОРДА, страницами параллельно и с дедупом по минту.
    ///
    /// Зачем это вообще нужно. Котла монеты в списочном ответе НЕТ — приходят
    /// только mint/name/symbol/image/createdAt/rewardMint/rewardBasket/
    /// rewardCycle/category. Сортировать борду по котлу API не умеет: из всех
    /// значений `sort` он принимает РОВНО ДВА, `volume24h` и `marketCap`, а
    /// любое другое (в том числе наш прежний `fresh`) молча отдаёт дефолтный
    /// порядок. Размер страницы упёрт в 60 (size=1000 вернул 60).
    ///
    /// Значит единственный способ найти монету с набранным котлом — обойти
    /// борду ЦЕЛИКОМ и посчитать котлы самому. Замер 04.11.2026: 67 714 монет,
    /// котёл ≥0.05 SOL был у ПЯТИ, ≥0.01 — у четырнадцати, и лежат они где
    /// угодно, только не на первых десяти страницах.
    ///
    /// Последовательно это 1131 запрос по ~300 мс = почти шесть минут, и к
    /// концу прохода котлы начала списка уже устарели (клеймят раз в 4-5 мин).
    /// Поэтому страницы тянем волнами по `conc` штук.
    ///
    /// Сорт берём `volume24h`: он у API настоящий, то есть порядок страниц
    /// устойчив. На дефолтном порядке (свежие) борда сдвигается от каждой новой
    /// монеты, и страницы начинают терять/дублировать записи — от дублей
    /// страхуемся множеством минтов, от потерь устойчивым сортом.
    pub async fn coins_all(&self, size: usize, conc: usize) -> Result<Vec<OtcCoin>> {
        const MAX_PAGES: usize = 2400;
        let size = size.clamp(1, 60);
        let conc = conc.clamp(1, 32);
        let mut out: Vec<OtcCoin> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut page = 0usize;
        let mut errs = 0usize;
        let mut first_err: Option<anyhow::Error> = None;
        while page < MAX_PAGES {
            let mut set: tokio::task::JoinSet<(usize, Result<Vec<OtcCoin>>)> =
                tokio::task::JoinSet::new();
            for p in page..page + conc {
                let me = self.clone();
                set.spawn(async move { (p, me.coins_page(p, size, "volume24h").await) });
            }
            let mut wave: Vec<(usize, Vec<OtcCoin>)> = Vec::new();
            // Конец борды — это ПУСТАЯ страница, а не ошибка. Ошибку на одной
            // странице из тысячи нельзя считать концом: так обход обрывался бы
            // на случайном таймауте и отдавал огрызок вместо борды.
            let mut ended = false;
            while let Some(r) = set.join_next().await {
                let Ok((p, res)) = r else { continue };
                match res {
                    Ok(v) if v.is_empty() => ended = true,
                    Ok(v) => wave.push((p, v)),
                    Err(e) => {
                        errs += 1;
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            wave.sort_by_key(|(p, _)| *p);
            for (_, v) in wave {
                for c in v {
                    if seen.insert(c.mint.clone()) {
                        out.push(c);
                    }
                }
            }
            if ended {
                break;
            }
            page += conc;
        }
        if out.is_empty() {
            return Err(first_err.unwrap_or_else(|| anyhow::anyhow!("борда вернула пусто")));
        }
        if errs > 0 {
            tracing::warn!("обход борды: страниц с ошибкой {errs}, собрано монет {}", out.len());
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
