//! Lighthouse assert builder.
//!
//! Формат РЕВЕРС-ИНЖЕНЕРЕН из реальных транзакций разобранного бота, поэтому
//! байты совпадают с тем, что реально летит в mainnet.
//!
//! Пример из live-tx (data инструкции, hex):
//!   09 00 02 3342010000000000 04   → AssertTokenAccount, Amount >= 82483
//!   09 00 02 615b010000000000 03   → AssertTokenAccount, Amount <  89441
//! Два ассерта вместе дают окно amount ∈ [lo, hi).
//!
//! Раскладка:
//!   [0] = 0x09  — дискриминатор инструкции AssertTokenAccount
//!   [1] = log_level (0x00 = silent)
//!   [2] = вариант TokenAccountAssertion (0x02 = Amount)
//!   [3..11] = значение u64 LE
//!   [11] = IntegerOperator
//! Аккаунты: ровно один — проверяемый токен-аккаунт (readonly).

use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;

use crate::programs::LIGHTHOUSE_PROGRAM;

const IX_ASSERT_TOKEN_ACCOUNT: u8 = 0x09;
const IX_ASSERT_ACCOUNT_INFO: u8 = 0x05;
const LOG_SILENT: u8 = 0x00;
const VARIANT_AMOUNT: u8 = 0x02;
// AccountInfoAssertion варианты (борш-индекс): Lamports=0, DataLength=1, Owner=2,
// KnownOwner=3, ... (сверено с клиентом Lighthouse 2.0.0 и живыми байтами).
const AI_VARIANT_LAMPORTS: u8 = 0x00;
const AI_VARIANT_KNOWN_OWNER: u8 = 0x03;
const KNOWN_PROGRAM_SYSTEM: u8 = 0x00;
const EQ_EQUAL: u8 = 0x00; // EquatableOperator::Equal

/// IntegerOperator Lighthouse.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum Op {
    Eq = 0,
    NotEq = 1,
    Gt = 2,
    Lt = 3,
    Gte = 4,
    Lte = 5,
}

/// Один ассерт: amount <op> value на указанном токен-аккаунте.
pub fn assert_token_amount(token_account: &Pubkey, value: u64, op: Op) -> Instruction {
    let mut data = Vec::with_capacity(12);
    data.push(IX_ASSERT_TOKEN_ACCOUNT);
    data.push(LOG_SILENT);
    data.push(VARIANT_AMOUNT);
    data.extend_from_slice(&value.to_le_bytes());
    data.push(op as u8);

    Instruction {
        program_id: LIGHTHOUSE_PROGRAM,
        accounts: vec![AccountMeta::new_readonly(*token_account, false)],
        data,
    }
}

/// Пара ассертов amount ∈ [lo, hi) — именно так бот ловит «раунд вот-вот/только что».
/// Возвращает две инструкции, которые ставятся в начало транзакции.
pub fn assert_amount_in_range(token_account: &Pubkey, lo: u64, hi: u64) -> [Instruction; 2] {
    [
        assert_token_amount(token_account, lo, Op::Gte),
        assert_token_amount(token_account, hi, Op::Lt),
    ]
}

/// AssertAccountInfo::Lamports: lamports аккаунта <op> value. Для гейта входа —
/// value=0, Op::Eq («аккаунт ещё не создан»). Байты: 05 00 00 <u64 LE> <op>.
pub fn assert_account_lamports(account: &Pubkey, value: u64, op: Op) -> Instruction {
    let mut data = Vec::with_capacity(12);
    data.push(IX_ASSERT_ACCOUNT_INFO);
    data.push(LOG_SILENT);
    data.push(AI_VARIANT_LAMPORTS);
    data.extend_from_slice(&value.to_le_bytes());
    data.push(op as u8);
    Instruction {
        program_id: LIGHTHOUSE_PROGRAM,
        accounts: vec![AccountMeta::new_readonly(*account, false)],
        data,
    }
}

/// AssertAccountInfo::KnownOwner == System (аккаунт ещё не SPL). Байты: 05 00 03 00 00.
pub fn assert_owner_system(account: &Pubkey) -> Instruction {
    Instruction {
        program_id: LIGHTHOUSE_PROGRAM,
        accounts: vec![AccountMeta::new_readonly(*account, false)],
        data: vec![
            IX_ASSERT_ACCOUNT_INFO,
            LOG_SILENT,
            AI_VARIANT_KNOWN_OWNER,
            KNOWN_PROGRAM_SYSTEM,
            EQ_EQUAL,
        ],
    }
}
