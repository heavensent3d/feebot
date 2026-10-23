//! Сборка и подпись v0-транзакций (compute budget, ALT, durable nonce).

use anyhow::{anyhow, Result};
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::{v0, VersionedMessage};
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;

/// Две инструкции compute budget: лимит CU и цена (micro-lamports/CU).
pub fn compute_budget(cu_limit: u32, cu_price_micro: u64) -> [Instruction; 2] {
    [
        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
        ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro),
    ]
}

/// Собрать и подписать v0-транзакцию.
pub fn build_signed(
    payer: &Keypair,
    instructions: &[Instruction],
    blockhash: Hash,
    alts: &[AddressLookupTableAccount],
    extra_signers: &[&Keypair],
) -> Result<VersionedTransaction> {
    let msg = v0::Message::try_compile(&payer.pubkey(), instructions, alts, blockhash)
        .map_err(|e| anyhow!("compile v0 message: {e}"))?;
    let mut signers: Vec<&Keypair> = vec![payer];
    signers.extend_from_slice(extra_signers);
    VersionedTransaction::try_new(VersionedMessage::V0(msg), signers.as_slice())
        .map_err(|e| anyhow!("sign tx: {e}"))
}

/// Хелпер: собрать итоговый список инструкций
/// [compute budget] + [основные] (+ опционально tip в начале торговых).
pub fn with_budget(
    cu_limit: u32,
    cu_price_micro: u64,
    core: Vec<Instruction>,
) -> Vec<Instruction> {
    let mut v = compute_budget(cu_limit, cu_price_micro).to_vec();
    v.extend(core);
    v
}

/// Пустой набор ALT (когда lookup-таблицы не используем).
pub fn no_alts() -> Vec<AddressLookupTableAccount> {
    Vec::new()
}
