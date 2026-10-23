//! Durable nonce: позволяет ОДИН РАЗ подписать выход и спамить его хоть час.
//!
//! Схема разобранного бота на выходе:
//!   ix0 = advance_nonce_account   (обязана быть первой)
//!   ix1 = Lighthouse assert        (гейт: раунд уже купил сток)
//!   ix2 = sell memecoin
//! Пока ассерт падает — транзакция не исполняется, значит nonce НЕ
//! расходуется, и ту же самую tx можно слать снова и снова. Как только
//! состояние reward-кошелька попадает в окно ассерта — tx исполняется
//! ровно один раз и nonce сгорает.

use crate::rpc::Rpc;
use anyhow::{anyhow, Context, Result};
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::nonce::state::Versions;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction;

#[derive(Debug, Clone)]
pub struct NonceData {
    pub authority: Pubkey,
    /// Хэш, который надо класть в recent_blockhash транзакции.
    pub blockhash: Hash,
}

/// Прочитать состояние durable-nonce аккаунта.
pub async fn read_nonce(rpc: &Rpc, nonce_account: &Pubkey) -> Result<NonceData> {
    let acc = rpc
        .account(nonce_account)
        .await?
        .ok_or_else(|| anyhow!("nonce-аккаунт {nonce_account} не найден"))?;
    let versions: Versions =
        bincode::deserialize(&acc.data).context("не парсится nonce state")?;
    let data = match versions {
        Versions::Legacy(s) | Versions::Current(s) => s,
    };
    // `State::Initialized` содержит authority + durable_nonce + fee calc.
    match *data {
        solana_sdk::nonce::state::State::Initialized(ref d) => Ok(NonceData {
            authority: d.authority,
            blockhash: d.durable_nonce.as_hash().to_owned(),
        }),
        _ => Err(anyhow!("nonce-аккаунт не инициализирован")),
    }
}

/// Инструкция advance (должна быть ПЕРВОЙ в транзакции с durable nonce).
pub fn advance_ix(nonce_account: &Pubkey, authority: &Pubkey) -> Instruction {
    system_instruction::advance_nonce_account(nonce_account, authority)
}
