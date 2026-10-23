//! On-chain program IDs and well-known accounts.
//!
//! Все адреса ниже сняты с реальных транзакций разобранного бота
//! (FpZAyCrxPsfoW5XWySMPyRc86mjpo8SsGdWauRZbtniR) или из docs otcdesks.cash.
//! Это факты из ончейна, их не надо угадывать.
//!
//! ВНИМАНИЕ про layout: сами адреса стабильны, но ПОРЯДОК аккаунтов и
//! дискриминаторы инструкций pump.fun/PumpSwap меняются при апгрейдах
//! программы. Их надо сверять с актуальным IDL перед заливкой денег —
//! см. pumpfun.rs / pumpswap.rs.

use solana_sdk::pubkey::Pubkey;

/// const-парсинг base58 → Pubkey (solana-sdk 2.x).
const fn pk(s: &str) -> Pubkey {
    Pubkey::from_str_const(s)
}

// ─────────────────────────── системные ───────────────────────────
pub const SYSTEM_PROGRAM: Pubkey = pk("11111111111111111111111111111111");
pub const COMPUTE_BUDGET: Pubkey = pk("ComputeBudget111111111111111111111111111111");
pub const SYSVAR_RENT: Pubkey = pk("SysvarRent111111111111111111111111111111111");

/// Классический SPL Token.
pub const TOKEN_PROGRAM: Pubkey = pk("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
/// Token-2022 — на нём выпущены все reward-стоки OTC (xStocks/Ondo/PreStocks).
pub const TOKEN_2022_PROGRAM: Pubkey = pk("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
pub const ASSOCIATED_TOKEN_PROGRAM: Pubkey = pk("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const WSOL_MINT: Pubkey = pk("So11111111111111111111111111111111111111112");

// ─────────────────────────── pump.fun ───────────────────────────
/// pump.fun bonding-curve program.
pub const PUMP_PROGRAM: Pubkey = pk("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
/// `global` PDA (snaйпнуто из buy-транзакции).
pub const PUMP_GLOBAL: Pubkey = pk("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
/// fee_recipient (writable).
pub const PUMP_FEE_RECIPIENT: Pubkey = pk("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");
/// event_authority.
pub const PUMP_EVENT_AUTHORITY: Pubkey = pk("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");
/// Отдельная fee-программа pump (считает сплиты квоты-минта).
pub const PUMP_FEE_PROGRAM: Pubkey = pk("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
/// global_volume_accumulator / user_volume_accumulator (из live-инструкции).
pub const PUMP_GLOBAL_VOLUME_ACC: Pubkey = pk("Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y");

/// PumpSwap (pAMM) — торговля уже «выпустившимися» (graduated) монетами.
pub const PUMPSWAP_PROGRAM: Pubkey = pk("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");

// ─────────────────────────── Lighthouse ───────────────────────────
/// Lighthouse — программа ассертов. Именно её бот ставит первой инструкцией
/// в buy/sell, чтобы транзакция исполнилась только при нужном состоянии
/// reward-кошелька. Снято из live-tx.
pub const LIGHTHOUSE_PROGRAM: Pubkey = pk("L2TExMFKdjpN9kozasaurPirfHy9P8sbXoAN1qA3S95");

// ─────────────────────────── OTC (otcdesks.cash) ───────────────────────────
/// Launcher rewards wallet — клеймит pump-фисы и покупает стоки держателям.
/// Именно его балансы бот читает в Lighthouse-ассертах для тайминга.
pub const OTC_LAUNCHER_REWARDS: Pubkey = pk("2k5hrzuykwyTbUe8L7UriYAQhr5hijNLgBvEB4B9pP5y");
/// Meteora rewards wallet (полный адрес из docs; сверено аналитиком 23.10.2026).
pub const OTC_METEORA_REWARDS: Pubkey = pk("4wYGg1KxUroz6Aitgmca64c5wqZW5q1cULCTRJ8dxhyL");

/// OTC launcher program (BPFLoaderUpgradeable). Per-coin accrual к раунду тут
/// НЕ читается он-чейн (это per-round леджеры) — близость раунда берём из API
/// или как прокси — баланс pump creator-vault.
pub const OTC_LAUNCHER_PROGRAM: Pubkey = pk("AjMx5My4YUDHMiCtLpTAtgkiUJgrpJnQqd5AcQnddHQW");

// ─────────────────────────── агрегаторы ───────────────────────────
/// Jupiter v6 aggregator (используем HTTP API, program id для справки).
pub const JUPITER_PROGRAM: Pubkey = pk("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
/// FLASHX — агрегатор/роутер, которым разобранный бот продавал награды.
pub const FLASHX_PROGRAM: Pubkey = pk("FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9");

// ─────────────── relay tip accounts (пополнять по мере надобности) ───────────────
// Это НЕ исчерпывающий список — tip-аккаунты у сервисов ротируются,
// бери актуальные из их доков. Jito tip accounts зашиты в relay.rs.
