//! Реальный ончейн-«квалифицированный флоат» (USD), который конкурирует за
//! одну reward-раздачу OTC.
//!
//! По докам otcdesks.cash раунд делится pro-rata между кошельками, у которых
//! на руках ≥ $20 монеты, ПРИЧЁМ перед делением из обращения исключаются:
//!   * bonding curve (её ATA под монетой),
//!   * AMM-пул (его vault — известен только ПОСЛЕ градации),
//!   * creator vault.
//! Этот модуль считает сумму USD по держателям за вычетом перечисленных
//! аккаунтов — чтобы заменить «угаданный» флоат в EV-фильтре на фактический.
//!
//! Две стратегии сбора держателей:
//!   (a) `qualified_float_usd` — ДЕШЁВАЯ, дефолтная. Берёт топ-20 токен-
//!       аккаунтов через `getTokenLargestAccounts`. Работает на публичном RPC,
//!       одна лёгкая ручка. Для флоата это хорошее приближение: верхние
//!       держатели доминируют в распределении, а хвост мелочи почти не влияет
//!       на сумму (и часто сам отсекается порогом min_holding_usd).
//!   (b) `qualified_float_usd_full` — ТЯЖЁЛАЯ, точная. Полный перебор всех
//!       токен-аккаунтов монеты через `getProgramAccounts`. Даёт точную сумму,
//!       но на публичных RPC обычно задросселено/запрещено (большой ответ).
//!       Включать только на приватной ноде.

use anyhow::{anyhow, Result};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

/// Разбивка квалифицированного флоата по итогам подсчёта.
#[derive(Debug, Clone)]
pub struct FloatBreakdown {
    /// Сумма USD по аккаунтам, прошедшим порог (за вычетом исключённых).
    pub float_usd: f64,
    /// Сколько аккаунтов зачлось во флоат (usd >= min_holding_usd).
    pub eligible_holders: usize,
    /// Сколько всего токен-аккаунтов просмотрено (ДО исключений и порога).
    pub total_accounts: usize,
}

/// Свести список `(адрес токен-аккаунта, ui-amount монеты)` в разбивку флоата.
///
/// Общая логика для обеих стратегий: `total_accounts` считает все просмотренные
/// аккаунты; аккаунт из `exclude` пропускается (bonding curve / creator vault /
/// AMM-пул); остальные пересчитываются в USD и зачитываются во флоат только при
/// usd >= `min_holding_usd`.
fn fold_float(
    accounts: impl IntoIterator<Item = (Pubkey, f64)>,
    usd_price_per_token: f64,
    min_holding_usd: f64,
    exclude: &[Pubkey],
) -> FloatBreakdown {
    let mut float_usd = 0.0f64;
    let mut eligible_holders = 0usize;
    let mut total_accounts = 0usize;

    for (addr, ui_amount) in accounts {
        total_accounts += 1;
        // Исключаем bonding curve / creator vault / AMM-пул (приходят в exclude).
        if exclude.contains(&addr) {
            continue;
        }
        let usd = ui_amount * usd_price_per_token;
        if usd >= min_holding_usd {
            float_usd += usd;
            eligible_holders += 1;
        }
    }

    FloatBreakdown {
        float_usd,
        eligible_holders,
        total_accounts,
    }
}

/// ДЕШЁВАЯ стратегия (дефолт): топ-20 держателей через `getTokenLargestAccounts`.
///
/// Приближение квалифицированного флоата: верхние держатели доминируют, хвост
/// обычно ниже порога и в сумму почти не идёт. Подходит для публичного RPC.
///
/// `exclude` — адреса токен-аккаунтов, которые НЕ учитываем (см.
/// [`excluded_accounts`]): ATA бондинг-кривой, creator vault, vault AMM-пула.
/// `usd_price_per_token` — цена за один (ui-)токен в USD.
/// `min_holding_usd` — порог участия в раунде (по докам OTC = $20).
pub async fn qualified_float_usd(
    rpc: &crate::rpc::Rpc,
    mint: &Pubkey,
    usd_price_per_token: f64,
    min_holding_usd: f64,
    exclude: &[Pubkey],
) -> Result<FloatBreakdown> {
    // getTokenLargestAccounts → Vec<RpcTokenAccountBalance { address: String,
    // amount: UiTokenAmount { ui_amount: Option<f64>, amount: String, .. } }>.
    let largest = rpc
        .client
        .get_token_largest_accounts(mint)
        .await
        .map_err(|e| anyhow!("get_token_largest_accounts {mint}: {e}"))?;

    let mut items = Vec::with_capacity(largest.len());
    for b in largest {
        let addr = Pubkey::from_str(&b.address)
            .map_err(|e| anyhow!("плохой адрес токен-аккаунта {}: {e}", b.address))?;
        // ui_amount уже учитывает decimals монеты; None (редко) трактуем как 0.
        let ui = b.amount.ui_amount.unwrap_or(0.0);
        items.push((addr, ui));
    }

    let bd = fold_float(items, usd_price_per_token, min_holding_usd, exclude);
    tracing::debug!(
        mint = %mint,
        float_usd = bd.float_usd,
        eligible = bd.eligible_holders,
        total = bd.total_accounts,
        "qualified_float_usd (top-20)"
    );
    Ok(bd)
}

/// ТЯЖЁЛАЯ стратегия: полный перебор ВСЕХ токен-аккаунтов монеты через
/// `getProgramAccounts` на классической Token-программе.
///
/// Фильтры:
///   * `DataSize(165)` — размер классического SPL token account;
///   * `Memcmp(0, mint)` — mint лежит в самом начале аккаунта (offset 0).
/// В отличие от `rpc.rs::token_accounts` (там фильтр по OWNER на offset 32)
/// здесь мы фильтруем именно по MINT — нужны все держатели одной монеты.
/// Amount парсим из байт 64..72 (как в `rpc.rs::parse_token_account`), raw u64
/// переводим в ui-amount по decimals монеты.
///
/// ⚠️ Тяжёлый запрос: большой ответ, на публичных RPC часто задросселен или
/// выключен. Держать за отдельной функцией и дёргать только на приватной ноде.
pub async fn qualified_float_usd_full(
    rpc: &crate::rpc::Rpc,
    mint: &Pubkey,
    usd_price_per_token: f64,
    min_holding_usd: f64,
    exclude: &[Pubkey],
) -> Result<FloatBreakdown> {
    // decimals монеты — чтобы перевести raw u64 из token account в ui-amount.
    let supply = rpc
        .client
        .get_token_supply(mint)
        .await
        .map_err(|e| anyhow!("get_token_supply {mint}: {e}"))?;
    let factor = 10f64.powi(supply.decimals as i32);

    let cfg = RpcProgramAccountsConfig {
        filters: Some(vec![
            RpcFilterType::DataSize(165),
            RpcFilterType::Memcmp(Memcmp::new_base58_encoded(0, mint.as_ref())),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(rpc.commitment),
            ..Default::default()
        },
        ..Default::default()
    };

    let accs = rpc
        .client
        .get_program_accounts_with_config(&crate::programs::TOKEN_PROGRAM, cfg)
        .await
        .map_err(|e| anyhow!("get_program_accounts_with_config {mint}: {e}"))?;

    let mut items = Vec::with_capacity(accs.len());
    for (addr, acc) in accs {
        if let Some(raw) = parse_amount(&acc.data) {
            items.push((addr, raw as f64 / factor));
        }
    }

    let bd = fold_float(items, usd_price_per_token, min_holding_usd, exclude);
    tracing::debug!(
        mint = %mint,
        float_usd = bd.float_usd,
        eligible = bd.eligible_holders,
        total = bd.total_accounts,
        "qualified_float_usd_full (getProgramAccounts)"
    );
    Ok(bd)
}

/// Amount (базовые единицы) из SPL token account: offset 64..72, LE u64.
/// Зеркалит парс из `rpc.rs::parse_token_account`, но mint/owner тут не нужны —
/// фильтрация по mint делается RPC-мемкмпом, см. [`qualified_float_usd_full`].
fn parse_amount(data: &[u8]) -> Option<u64> {
    if data.len() < 72 {
        return None;
    }
    Some(u64::from_le_bytes(data[64..72].try_into().ok()?))
}

/// Собрать список токен-аккаунтов, которые НАДО исключить из флоата монеты.
///
/// Возвращает:
///   * ATA бондинг-кривой под монетой — `ata(bonding_curve(mint), mint)`;
///   * ATA creator vault под монетой — `ata(creator_vault(creator), mint)`.
///
/// ВАЖНО: vault AMM-пула (PumpSwap) сюда НЕ попадает — до градации монеты его
/// адрес неизвестен. После градации его нужно добавить в `exclude` отдельно
/// (лид передаёт готовый список в `qualified_float_usd*`). Сам creator vault —
/// это преимущественно SOL-аккаунт комиссий; его ATA под монетой может не
/// существовать/быть пустым — исключаем на всякий случай, honoring док OTC.
pub fn excluded_accounts(mint: &Pubkey, creator: &Pubkey) -> Vec<Pubkey> {
    let curve = crate::pumpfun::bonding_curve(mint);
    let vault = crate::pumpfun::creator_vault(creator);
    // OTC-монеты — Token-2022, но подстрахуемся обоими вариантами ATA.
    vec![
        crate::pumpfun::ata(&curve, mint),
        crate::pumpfun::ata_2022(&curve, mint),
        crate::pumpfun::ata(&vault, mint),
        crate::pumpfun::ata_2022(&vault, mint),
    ]
}

/// Флоат, который РЕАЛЬНО конкурирует за ближайшую раздачу: только держатели,
/// у которых УЖЕ ОТКРЫТ ATA под reward-актив.
///
/// Проверено на чейне 03.11.2026: раздача НЕ создаёт ATA получателям. Три
/// подряд раздачи — 12 получателей, все 12 уже имели ATA до транзакции,
/// инструкций создания ноль, только переводы Tokenkeg. Кто не открыл заранее —
/// не получает ничего, и значит в знаменателе нашей доли ему не место.
///
/// Это меняет расчёт радикально: обычный флоат считает ВСЕХ держателей монеты,
/// а платят лишь подмножеству. Мы завышали знаменатель и занижали свою долю.
///
/// Стоит трёх RPC-вызовов: топ-аккаунты монеты, их владельцы, существование их
/// reward-ATA.
pub async fn eligible_float_usd(
    rpc: &crate::rpc::Rpc,
    mint: &Pubkey,
    reward_mint: &Pubkey,
    reward_token_program: &Pubkey,
    usd_price_per_token: f64,
    min_holding_usd: f64,
    exclude: &[Pubkey],
) -> Result<FloatBreakdown> {
    let largest = rpc
        .client
        .get_token_largest_accounts(mint)
        .await
        .map_err(|e| anyhow!("get_token_largest_accounts {mint}: {e}"))?;

    // Адрес токен-аккаунта → сколько на нём монеты.
    let mut accs: Vec<(Pubkey, f64)> = Vec::new();
    for b in largest {
        let addr = Pubkey::from_str(&b.address)
            .map_err(|e| anyhow!("плохой адрес токен-аккаунта {}: {e}", b.address))?;
        if exclude.contains(&addr) {
            continue;
        }
        let ui = b.amount.ui_amount.unwrap_or(0.0);
        if ui > 0.0 {
            accs.push((addr, ui));
        }
    }
    if accs.is_empty() {
        return Ok(FloatBreakdown { float_usd: 0.0, eligible_holders: 0, total_accounts: 0 });
    }

    // Владельцы этих токен-аккаунтов.
    let keys: Vec<String> = accs.iter().map(|(a, _)| a.to_string()).collect();
    let info = rpc
        .raw("getMultipleAccounts", serde_json::json!([keys, {"encoding":"jsonParsed"}]))
        .await?;
    let vals = info["value"].as_array().cloned().unwrap_or_default();

    let mut owners: Vec<(Pubkey, f64)> = Vec::new();
    for ((_, ui), v) in accs.iter().zip(vals.iter()) {
        let owner = v["data"]["parsed"]["info"]["owner"].as_str().unwrap_or("");
        if let Ok(o) = Pubkey::from_str(owner) {
            owners.push((o, *ui));
        }
    }
    if owners.is_empty() {
        return Ok(FloatBreakdown { float_usd: 0.0, eligible_holders: 0, total_accounts: 0 });
    }

    // Есть ли у владельца ATA под reward-актив.
    let atas: Vec<String> = owners
        .iter()
        .map(|(o, _)| {
            spl_associated_token_account::get_associated_token_address_with_program_id(
                o,
                reward_mint,
                reward_token_program,
            )
            .to_string()
        })
        .collect();
    let ex = rpc
        .raw("getMultipleAccounts", serde_json::json!([atas, {"encoding":"base64"}]))
        .await?;
    let exv = ex["value"].as_array().cloned().unwrap_or_default();

    let mut float_usd = 0.0f64;
    let mut eligible = 0usize;
    for ((_, ui), v) in owners.iter().zip(exv.iter()) {
        if v.is_null() {
            continue; // ATA нет → в раздаче не участвует
        }
        let usd = ui * usd_price_per_token;
        if usd >= min_holding_usd {
            float_usd += usd;
            eligible += 1;
        }
    }
    tracing::debug!(mint=%mint, float_usd, eligible, total=owners.len(), "eligible_float_usd");
    Ok(FloatBreakdown { float_usd, eligible_holders: eligible, total_accounts: owners.len() })
}
