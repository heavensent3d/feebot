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
    /// Сколько секунд назад была ПОСЛЕДНЯЯ раздача. Критично: монета с отличной
    /// историей, но раздававшая 17 часов назад — мёртвая, раунда за наш холд не будет.
    pub last_payout_age_secs: Option<u64>,
}

pub struct Oracle {
    otc: OtcClient,
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
        let coins = self
            .otc
            .coins_sorted(self.pages, self.page_size, &self.sort)
            .await?;
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
                last_payout_age_secs: None,
            });
        }
        Ok(out)
    }
}
