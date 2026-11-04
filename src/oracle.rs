//! Оракул кандидатов: превращает борду OTC в список монет-целей с привязкой
//! к reward-токен-аккаунту, за балансом которого следит Lighthouse-гейт.

use crate::config::OtcConfig;
use crate::otc::OtcClient;
use crate::programs::{OTC_LAUNCHER_REWARDS, TOKEN_2022_PROGRAM};
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Candidate {
    pub mint: Pubkey,
    pub symbol: String,
    /// Сток, которым платит монета.
    pub reward_mint: Pubkey,
    /// ATA reward-кошелька под этот сток — его amount читает Lighthouse-ассерт
    /// для тайминга входа/выхода.
    pub reward_token_account: Pubkey,
    /// На что торгуется монета (None = SOL).
    pub pair_mint: Option<Pubkey>,
    pub market_cap_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub holders: Option<u64>,
    pub pending_reward_sol: Option<f64>,
    /// Изменение цены за 24ч (%) — не входим в обвал.
    pub change_24h_pct: Option<f64>,
    /// ── Ниже заполняется ТОЛЬКО через `Oracle::enrich_rewards` (per-coin запрос),
    /// т.к. объект `rewards` приходит лишь из `/api/coins?mint=X`. ──
    /// Накоплено холдерам и ждёт раздачи (SOL) — тайминг входа.
    pub owed_sol: Option<f64>,
    /// Роздано холдерам за жизнь монеты (SOL) — фильтр качества («от 0.4»).
    pub distributed_sol: Option<f64>,
    /// Сколько холдеров делили последнюю выплату — прокси флоата (меньше = наша доля больше).
    pub last_paid_holders: Option<u64>,
    /// Наша ДОЛЯ в будущей выплате (0..1), измеренная в tick_entries перед
    /// входом: buy_usd / (чужой свободный флоат + buy_usd). Нужна в позиции,
    /// чтобы посчитать ожидаемый фис из чейна.
    pub share_frac: Option<f64>,
    /// Чужой свободный флоат в USD, измеренный перед входом. Нужен в позиции,
    /// чтобы ПОДОБРАТЬ размер: доля зависит от того, сколько мы покупаем.
    pub float_usd: Option<f64>,
    /// ВСЯ линейка reward-активов монеты. Раздача НЕ создаёт ATA получателям
    /// (проверено на трёх раздачах: 12 получателей, 12 уже имели ATA, ноль
    /// инструкций создания), а линейка ротируется после каждой выплаты. Значит
    /// ATA надо держать открытыми под ВСЕ активы, иначе можно не попасть в
    /// собственную раздачу.
    pub reward_basket: Vec<Pubkey>,
    /// Сколько секунд назад была ПОСЛЕДНЯЯ раздача. Критично: монета с отличной
    /// историей, но раздававшая 17 часов назад — мёртвая, раунда за наш холд не будет.
    pub last_payout_age_secs: Option<u64>,
}

pub struct Oracle {
    pub(crate) otc: OtcClient,
    /// Сколько страниц борды тянуть (борда ~52k монет).
    pages: usize,
    page_size: usize,
    /// Чем сортировать борду; см. OtcConfig::sort — дефолт даёт свежие монеты.
    sort: String,
}

impl Oracle {
    pub fn new(cfg: &OtcConfig) -> Self {
        Self {
            otc: OtcClient::new(cfg),
            pages: cfg.pages,
            page_size: cfg.page_size,
            sort: cfg.sort.clone(),
        }
    }

    /// Когда воркер OTC последний раз клеймил фисы в котёл — ПРЕКУРСОР раздачи
    /// (от клейма до раздачи ~37с). Рост этого числа = пора лить пролив.
    pub async fn last_claim_at(&self, mint: &str) -> Option<u64> {
        let c = self.otc.coin_detail(mint).await.ok()?;
        let r = c.rewards?;
        if r.last_claim_at > 0 {
            Some(r.last_claim_at)
        } else {
            None
        }
    }

    /// Котёл монеты (waitingSol / floorSol) — прекурсор раздачи для гонки выхода.
    pub async fn vault(&self, mint: &str) -> anyhow::Result<crate::otc::OtcVault> {
        self.otc.vault(mint).await
    }

    /// Догрузить `rewards` по монете (owed / distributed / lastPaidHolders).
    /// Отдельный per-coin запрос — дёргаем только для прошедших дешёвый прескрин.
    /// Возвращает (owed_sol, distributed_sol, last_paid_holders, last_payout_age_secs).
    pub async fn enrich_rewards(&self, mint: &Pubkey) -> Option<(f64, f64, u64, Option<u64>)> {
        let c = self.otc.coin_detail(&mint.to_string()).await.ok()?;
        let r = c.rewards?;
        let age = if r.last_distributed_at > 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            Some(now.saturating_sub(r.last_distributed_at))
        } else {
            None
        };
        Some((
            r.owed as f64 / 1e9,
            r.distributed as f64 / 1e9,
            r.last_paid_holders,
            age,
        ))
    }

    /// Снять борду (страницы по объёму — там, где реально копятся фисы) и собрать кандидатов.
    pub async fn candidates(&self) -> Result<Vec<Candidate>> {
        self.candidates_with(self.pages, self.page_size, &self.sort.clone())
            .await
    }

    /// То же, но с явными параметрами выборки. Нужно для замеров: борда
    /// принимает РЕАЛЬНО только два сорта (`volume24h` и `marketCap`), всё
    /// остальное молча отдаёт дефолтный порядок, и это надо уметь сравнивать.
    pub async fn candidates_with(
        &self,
        pages: usize,
        size: usize,
        sort: &str,
    ) -> Result<Vec<Candidate>> {
        let coins = self.otc.coins_sorted(pages, size, sort).await?;
        let mut out = self.map_coins(coins);
        // ПОРЯДОК ПЕРЕБОРА = выбор позиции: tick_entries берёт ПЕРВОГО прошедшего,
        // а не лучшего. Здесь, где котёл ещё не измерен, лучшее доступное
        // приближение — число холдеров: реворд делится пропорционально токенам
        // (проверено на Takara: наши 9.78M против 37.5M конкурента дали ровно
        // 20.7% выплаты). В основном пути кандидаты приходят из `candidates_all`
        // и пересортировываются по РЕАЛЬНОМУ котлу.
        out.sort_by(|a, b| {
            let ha = a.holders.unwrap_or(u64::MAX);
            let hb = b.holders.unwrap_or(u64::MAX);
            ha.cmp(&hb)
                .then(b.volume_24h_usd.partial_cmp(&a.volume_24h_usd).unwrap_or(std::cmp::Ordering::Equal))
        });
        Ok(out)
    }

    /// Кандидаты со ВСЕЙ борды — основной источник отбора.
    ///
    /// Десять страниц «свежих» целью не были никогда: котла в списочном ответе
    /// нет, сортировать по нему борда не умеет, а монеты с набранным котлом
    /// рассыпаны по всем 67 тысячам записей (замер 04.11.2026: котёл ≥0.05 SOL
    /// у пяти монет из 67 714). Порядок здесь НЕ назначается — котлы считает и
    /// ранжирует `Bot::sweep_board` уже из чейна.
    pub async fn candidates_all(&self, size: usize, conc: usize) -> Result<Vec<Candidate>> {
        let coins = self.otc.coins_all(size, conc).await?;
        Ok(self.map_coins(coins))
    }

    /// Разбор записей борды в кандидатов. Порядок списка НЕ меняет.
    fn map_coins(&self, coins: Vec<crate::otc::OtcCoin>) -> Vec<Candidate> {
        let mut out = Vec::new();
        for c in coins {
            let Ok(mint) = Pubkey::from_str(&c.mint) else {
                continue;
            };
            // Баскет-монеты (ETF) ТОЖЕ торгуем: у них `rewardMint` = ТЕКУЩИЙ сток
            // (по индексу `rewardCycle`), и ближайший раунд платит именно им —
            // гейт выхода на него корректен. Ротация между раундами риск только
            // для долгого холда; бьём коротко (как бот кента). reward_mint есть и
            // у single, и у basket; basket[0] — страховка.
            let reward_str = c
                .reward_mint
                .clone()
                .or_else(|| c.reward_basket.as_ref().and_then(|b| b.first().cloned()));
            let Some(reward_str) = reward_str else {
                continue;
            };
            let Ok(reward_mint) = Pubkey::from_str(&reward_str) else {
                continue;
            };
            let reward_token_account =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &OTC_LAUNCHER_REWARDS,
                    &reward_mint,
                    &TOKEN_2022_PROGRAM,
                );
            let pair_mint = c.pair_mint.as_deref().and_then(|s| Pubkey::from_str(s).ok());
            let snap = c.snapshot.unwrap_or_default();

            out.push(Candidate {
                mint,
                symbol: c.symbol.unwrap_or_default(),
                reward_mint,
                reward_token_account,
                pair_mint,
                market_cap_usd: snap.market_cap,
                volume_24h_usd: snap.volume24h,
                liquidity_usd: snap.liquidity,
                holders: snap.holders,
                pending_reward_sol: c.pending_reward_sol,
                change_24h_pct: snap.change24h,
                // rewards приходят только из per-coin запроса → догружаем позже.
                owed_sol: c.rewards.as_ref().map(|r| r.owed as f64 / 1e9),
                distributed_sol: c.rewards.as_ref().map(|r| r.distributed as f64 / 1e9),
                last_paid_holders: c.rewards.as_ref().map(|r| r.last_paid_holders),
                share_frac: None,
                float_usd: None,
                reward_basket: c
                    .reward_basket
                    .as_ref()
                    .map(|b| b.iter().filter_map(|x| Pubkey::from_str(x).ok()).collect())
                    .unwrap_or_default(),
                last_payout_age_secs: None,
            });
        }
        out
    }
}
