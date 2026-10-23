//! Инсинератор: периодически закрывает лишние токен-аккаунты и возвращает
//! ренту (~0.002 SOL с аккаунта), как sol-incinerator.
//!
//! ПРАВИЛО БЕЗОПАСНОСТИ: трогаем только КЛАССИЧЕСКИЕ SPL-аккаунты
//! (мемкоины). Reward-стоки выпущены в Token-2022 — их аккаунты не трогаем
//! вообще, плюс чтим keep_mints. По умолчанию закрываем только пустые.

use crate::config::IncineratorConfig;
use crate::programs::{TOKEN_PROGRAM, WSOL_MINT};
use crate::rpc::Rpc;
use anyhow::Result;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

pub struct Incinerator {
    cfg: IncineratorConfig,
    keep: HashSet<Pubkey>,
}

impl Incinerator {
    pub fn new(cfg: IncineratorConfig) -> Self {
        let mut keep: HashSet<Pubkey> = cfg
            .keep_mints
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        keep.insert(WSOL_MINT); // WSOL ATA не жжём — он рабочий
        Self { cfg, keep }
    }

    pub fn enabled(&self) -> bool {
        self.cfg.enabled
    }
    pub fn interval_secs(&self) -> u64 {
        self.cfg.interval_secs
    }

    /// Найти закрываемые аккаунты и собрать close-инструкции (не больше
    /// max_per_run). Стоки (Token-2022) сюда не попадают по определению —
    /// сканим только классическую token-программу.
    pub async fn plan(&self, rpc: &Rpc, owner: &Pubkey) -> Result<Vec<Instruction>> {
        let accs = rpc.token_accounts(owner, &TOKEN_PROGRAM).await?;
        let mut ixs = Vec::new();
        for ta in accs {
            if ixs.len() >= self.cfg.max_per_run {
                break;
            }
            if self.keep.contains(&ta.mint) {
                continue;
            }
            if self.cfg.only_zero_balance && ta.amount != 0 {
                continue;
            }
            // close_account(token_program, account, destination, owner, &[])
            let ix = spl_token::instruction::close_account(
                &TOKEN_PROGRAM,
                &ta.address,
                owner,
                owner,
                &[],
            )?;
            ixs.push(ix);
        }
        Ok(ixs)
    }
}
