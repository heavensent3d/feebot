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
}

pub struct Oracle {
    otc: OtcClient,
}

impl Oracle {
    pub fn new(cfg: &OtcConfig) -> Self {
        Self {
            otc: OtcClient::new(cfg),
        }
    }

    /// Снять текущую борду и собрать кандидатов.
    pub async fn candidates(&self) -> Result<Vec<Candidate>> {
        let coins = self.otc.coins().await?;
        let mut out = Vec::new();
        for c in coins {
            let Ok(mint) = Pubkey::from_str(&c.mint) else {
                continue;
            };
            // ETF/корзины пропускаем: сток ротируется, гейт выхода на один сток
            // будет неверным (застрявший мешок). Поддержим позже.
            if c.reward_basket.as_ref().map(|b| b.len() > 1).unwrap_or(false) {
                continue;
            }
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
            });
        }
        Ok(out)
    }
}
