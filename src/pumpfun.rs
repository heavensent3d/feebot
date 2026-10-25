//! pump.fun bonding-curve: деривация PDA, парс состояния кривой, buy/sell.
//!
//! ⚠️ СВЕРИТЬ ПЕРЕД ЗАЛИВКОЙ ДЕНЕГ.
//! Здесь реализован КЛАССИЧЕСКИЙ layout pump.fun (buy disc 66063d1201daebea,
//! sell disc 33e685a4017f83ad, 12 аккаунтов, с creator_vault). Разобранный бот
//! на части монет использовал более новую инструкцию `buy_exact_sol_in`
//! (disc 38fc74089edfcd5f) с доп. fee-программой pfeeUxB6… и аккаунтами
//! global/user volume accumulator + fee config PDA. Если целевые монеты
//! требуют нового пути — расширь account-лист по актуальному IDL
//! (см. NEW_BUY_EXTRA ниже, адреса сняты с live-tx).

use anyhow::{anyhow, Result};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

use tracing::warn;

use crate::programs::*;
use crate::rpc::Rpc;

// ─────────────────────────────────────────────────────────────────────────
// ЖИВАЯ СПЕКА (снято с реальных tx бота FpZAy… 23.10.2026) — ИСПОЛЬЗОВАТЬ ЭТО,
// а классический build_buy/build_sell ниже для OTC-монет НЕ подходит.
//
// ВАЖНО: монеты OTC — это Token-2022 минты (owner минта = TokenzQdB…), значит
// ATA монеты надо выводить под TOKEN_2022_PROGRAM, и token_program в инструкции
// = Token-2022. (Это ломает классический путь ниже — он под обычный SPL.)
//
// BUY = инструкция `buy_exact_sol_in`, disc 38fc74089edfcd5f, 18 аккаунтов.
//   data = disc(8) + sol_in:u64 + min_tokens_out:u64 + [0x01,0x00]
//   accounts (W=writable, R=ro, *=signer):
//    0 global(R) 1 fee_recipient(W) 2 mint(R) 3 bonding_curve(W)
//    4 assoc_bonding_curve(W) 5 OUR_ata(W) 6 OUR_user(*W) 7 system(R)
//    8 token_program=Token-2022(R) 9 creator_vault(W) 10 event_authority(R)
//    11 pump_program(R) 12 global_vol_acc(R) 13 OUR_user_vol_acc(W)
//    14 fee_config(R) 15 fee_program=pfee(R) 16 coin_fee_A(R) 17 coin_fee_B(W)
//   Свои только 5,6,13 — остальное coin/global-специфично.
//
// SELL = инструкция `sell`, disc 33e685a4017f83ad, 16 аккаунтов.
//   data = disc(8) + token_amount_in:u64 + min_sol_out:u64
//   accounts: 0 global(R) 1 fee_recipient(W) 2 mint(R) 3 bonding_curve(W)
//    4 assoc_bonding_curve(W) 5 OUR_ata(W) 6 OUR_user(*W) 7 system(R)
//    8 creator_vault(W) 9 token_program=Token-2022(R) 10 event_authority(R)
//    11 pump_program(R) 12 fee_config(R) 13 fee_program(R) 14 coin_fee_A(R)
//    15 coin_fee_B(W). Свои только 5,6.
//
// Надёжный способ построить без тестов: взять ШАБЛОН аккаунтов из свежей
// реальной buy/sell этой монеты (getSignaturesForAddress(mint)+getTransaction),
// скопировать coin-специфичные метки, подменить свои (5,6,13 для buy; 5,6 для
// sell), и собрать data со своими суммами. coin_fee_A/B и их PDA-деривацию не
// угадываем. Выпустившиеся (pAMM) монеты — отдельный путь (pAMMBay…) или Jupiter.
// TODO(тест-фаза): реализовать template-билдеры + Token-2022 ATA и проверить
// микродепозитом (0.1–0.3 SOL), прежде чем выключать dry_run.
// ─────────────────────────────────────────────────────────────────────────

// Дискриминаторы (Anchor sighash, 8 байт).
const DISC_BUY: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
pub const DISC_SELL: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];
/// `buy_exact_sol_in` — путь, которым реально идут OTC-монеты (см. спеку выше).
pub const DISC_BUY_EXACT_SOL_IN: [u8; 8] = [0x38, 0xfc, 0x74, 0x08, 0x9e, 0xdf, 0xcd, 0x5f];

/// Адреса, которые в НОВОМ пути (`buy_exact_sol_in`) идут доп. аккаунтами —
/// сняты с реальной buy-транзакции бота. Оставлены как справка для апгрейда.
pub mod new_path_refs {
    // global_volume_accumulator  = Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y (константа PUMP_GLOBAL_VOLUME_ACC)
    // user_volume_accumulator    = hAbrD5YQCwWahAJikujFTFTafN7M1qoevuSytSdMENf (PDA ["user_volume_accumulator", user])
    // fee_config                 = 8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt
    // fee_program                = pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ (PUMP_FEE_PROGRAM)
}

// ─────────────────────────── PDA ───────────────────────────

pub fn bonding_curve(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &PUMP_PROGRAM).0
}

pub fn creator_vault(creator: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"creator-vault", creator.as_ref()], &PUMP_PROGRAM).0
}

pub fn user_volume_accumulator(user: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"user_volume_accumulator", user.as_ref()], &PUMP_PROGRAM).0
}

/// ATA под классическим SPL Token.
pub fn ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    ata_with(owner, mint, &TOKEN_PROGRAM)
}

/// ATA под Token-2022 (OTC-монеты — Token-2022).
pub fn ata_2022(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    ata_with(owner, mint, &TOKEN_2022_PROGRAM)
}

/// ATA под произвольной token-программой.
pub fn ata_with(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    spl_associated_token_account::get_associated_token_address_with_program_id(
        owner, mint, token_program,
    )
}

/// Какая token-программа у минта (owner минт-аккаунта = Token или Token-2022).
pub async fn coin_token_program(rpc: &Rpc, mint: &Pubkey) -> Pubkey {
    match rpc.account(mint).await {
        Ok(Some(a)) => a.owner,
        _ => TOKEN_2022_PROGRAM, // OTC-монеты — Token-2022 по умолчанию
    }
}

// ─────────────────────────── template-путь (Token-2022, buy_exact / sell) ───────────────────────────

/// Достать ШАБЛОН аккаунтов pump-инструкции (`disc`) из свежей реальной tx монеты.
/// Копируем coin-специфичные аккаунты как есть (в т.ч. fee-PDA, которые не выводим),
/// а свои подменим в билдере. Fail-safe: не нашли → Err, позицию пропустят.
pub async fn fetch_template(rpc: &Rpc, mint: &Pubkey, disc: &[u8; 8]) -> Result<Vec<AccountMeta>> {
    let pump = PUMP_PROGRAM.to_string();
    let sigs = rpc
        .raw("getSignaturesForAddress", serde_json::json!([mint.to_string(), {"limit": 40}]))
        .await?;
    let arr = sigs.as_array().cloned().unwrap_or_default();
    for s in arr {
        // пропускаем упавшие (err != null)
        if !s.get("err").map(|e| e.is_null()).unwrap_or(false) {
            continue;
        }
        let Some(sig) = s.get("signature").and_then(|x| x.as_str()) else {
            continue;
        };
        let tx = match rpc
            .raw(
                "getTransaction",
                serde_json::json!([sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":1}]),
            )
            .await
        {
            Ok(t) if !t.is_null() => t,
            _ => continue,
        };
        let msg = &tx["transaction"]["message"];

        // флаги (signer, writable) по каждому аккаунту
        let mut flags: HashMap<String, (bool, bool)> = HashMap::new();
        if let Some(keys) = msg["accountKeys"].as_array() {
            for k in keys {
                if let Some(pk) = k["pubkey"].as_str() {
                    flags.insert(
                        pk.to_string(),
                        (k["signer"].as_bool().unwrap_or(false), k["writable"].as_bool().unwrap_or(false)),
                    );
                }
            }
        }
        let la = &tx["meta"]["loadedAddresses"];
        if let Some(w) = la["writable"].as_array() {
            for k in w {
                if let Some(p) = k.as_str() {
                    flags.insert(p.to_string(), (false, true));
                }
            }
        }
        if let Some(r) = la["readonly"].as_array() {
            for k in r {
                if let Some(p) = k.as_str() {
                    flags.insert(p.to_string(), (false, false));
                }
            }
        }

        let Some(ixs) = msg["instructions"].as_array() else {
            continue;
        };
        for ins in ixs {
            if ins["programId"].as_str() != Some(pump.as_str()) {
                continue;
            }
            let data = ins["data"].as_str().unwrap_or("");
            let bytes = bs58::decode(data).into_vec().unwrap_or_default();
            if bytes.len() < 8 || &bytes[..8] != disc {
                continue;
            }
            let Some(accs) = ins["accounts"].as_array() else {
                continue;
            };
            let mut metas = Vec::with_capacity(accs.len());
            for a in accs {
                let pk = a.as_str().unwrap_or("");
                let pubkey = Pubkey::from_str(pk).map_err(|e| anyhow!("плохой аккаунт {pk}: {e}"))?;
                let (is_signer, is_writable) = flags.get(pk).copied().unwrap_or((false, false));
                metas.push(AccountMeta { pubkey, is_signer, is_writable });
            }
            if metas.len() >= 16 {
                return Ok(metas);
            }
        }
    }
    Err(anyhow!("шаблон pump-инструкции для {mint} не найден в последних tx"))
}

/// BUY `buy_exact_sol_in` по шаблону: свои — аккаунты 5 (ATA), 6 (user), 13 (user_vol_acc).
/// data = disc + sol_in:u64 + min_tokens_out:u64 + [0x01,0x00].
pub fn build_buy_exact_from_template(
    template: &[AccountMeta],
    user: &Pubkey,
    mint: &Pubkey,
    coin_token_program: &Pubkey,
    sol_in: u64,
    min_tokens_out: u64,
) -> Result<Instruction> {
    if template.len() < 14 {
        return Err(anyhow!("buy-шаблон короткий ({} аккаунтов)", template.len()));
    }
    let mut accounts = template.to_vec();
    accounts[5] = AccountMeta::new(ata_with(user, mint, coin_token_program), false);
    accounts[6] = AccountMeta::new(*user, true);
    accounts[13] = AccountMeta::new(user_volume_accumulator(user), false);
    let mut data = Vec::with_capacity(26);
    data.extend_from_slice(&DISC_BUY_EXACT_SOL_IN);
    data.extend_from_slice(&sol_in.to_le_bytes());
    data.extend_from_slice(&min_tokens_out.to_le_bytes());
    data.extend_from_slice(&[0x01, 0x00]);
    Ok(Instruction { program_id: PUMP_PROGRAM, accounts, data })
}

/// SELL `sell` по шаблону: свои — аккаунты 5 (ATA) и 6 (user).
/// data = disc + token_amount_in:u64 + min_sol_out:u64.
pub fn build_sell_from_template(
    template: &[AccountMeta],
    user: &Pubkey,
    mint: &Pubkey,
    coin_token_program: &Pubkey,
    token_amount: u64,
    min_sol_out: u64,
) -> Result<Instruction> {
    if template.len() < 7 {
        return Err(anyhow!("sell-шаблон короткий ({} аккаунтов)", template.len()));
    }
    let mut accounts = template.to_vec();
    accounts[5] = AccountMeta::new(ata_with(user, mint, coin_token_program), false);
    accounts[6] = AccountMeta::new(*user, true);
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&DISC_SELL);
    data.extend_from_slice(&token_amount.to_le_bytes());
    data.extend_from_slice(&min_sol_out.to_le_bytes());
    Ok(Instruction { program_id: PUMP_PROGRAM, accounts, data })
}

// ═══════════════════ ДЕРИВАЦИОННЫЙ БИЛДЕР (без шаблона монеты) ═══════════════════
// Все 16 из 18 аккаунтов buy (и 14 из 16 sell) выводятся детерминированно —
// сверено байт-в-байт с реальными buy/sell бота FpZAy (24.10.2026, bump'ы и флаги
// подтверждены). Недеривируемы только coin_fee_A/B (внутренние PDA pfee-программы):
// их берём из ЛЮБОЙ свежей buy ИЛИ sell этой монеты (sell у торгующихся монет
// частые — это снимает редкость buy_exact, на которой ломался template-scraping).
// fee_recipient берём константой PUMP_FEE_RECIPIENT (в whitelist pump, юзался
// реальным sell; pump принимает его всегда).

/// ATA бондинг-кривой под монетой (её token-программа). Аккаунт 4 в buy/sell.
pub fn assoc_bonding_curve(mint: &Pubkey, coin_token_program: &Pubkey) -> Pubkey {
    ata_with(&bonding_curve(mint), mint, coin_token_program)
}

/// Два per-coin fee-аккаунта pump-fee-программы (coin_fee_A/B), которые нельзя
/// вывести без IDL pfee. Стабильны per-coin.
#[derive(Debug, Clone, Copy)]
pub struct CoinFees {
    pub coin_fee_a: Pubkey,
    pub coin_fee_b: Pubkey,
}

/// Сколько транзакций тянем ОДНИМ пакетным запросом. Холодный батч на 30 транз у
/// RPC занимает до ~16с, поэтому берём небольшими чанками и выходим на первом
/// совпадении: обычно хватает первого чанка.
const FEES_SCAN_CHUNK: usize = 6;

/// Достать coin_fee_A/B из свежей реальной buy ИЛИ sell монеты. В buy они на
/// индексах 16,17; в sell — 14,15. Сканируем последние сигнатуры (top+inner).
///
/// Вывести эти аккаунты деривацией нельзя — проверено брутфорсом сидов (префиксы
/// fee/coin_fee/fee_vault/… × аккаунты инструкции × pfee|pump|ATA, все бампы):
/// совпадений нет, нужен IDL pfee. Поэтому скрейп остаётся, но он пакетный.
///
/// ВАЖНО по `maxSupportedTransactionVersion`: должно быть 1, а не 0. С нулём RPC
/// отвечает ошибкой -32015 на каждую v1-транзу, и мы теряли ~29% выборки (8 из 28
/// на замере) — то есть не находили шаблон у вполне живых монет и зря уводили их
/// в no_template-кулдаун.
pub async fn fetch_coin_fees(rpc: &Rpc, mint: &Pubkey) -> Result<CoinFees> {
    let pump = PUMP_PROGRAM.to_string();
    let sigs = rpc
        .raw("getSignaturesForAddress", serde_json::json!([mint.to_string(), {"limit": 30}]))
        .await?;
    let ok_sigs: Vec<String> = sigs
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s.get("err").map(|e| e.is_null()).unwrap_or(false))
        .filter_map(|s| s.get("signature").and_then(|x| x.as_str()).map(String::from))
        .collect();

    for chunk in ok_sigs.chunks(FEES_SCAN_CHUNK) {
        let calls: Vec<(&str, serde_json::Value)> = chunk
            .iter()
            .map(|sig| {
                (
                    "getTransaction",
                    serde_json::json!([sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":1}]),
                )
            })
            .collect();
        let txs = match rpc.batch(&calls).await {
            Ok(t) => t,
            Err(e) => {
                warn!("coin_fee {mint}: батч не прошёл ({e}) — пробуем следующий чанк");
                continue;
            }
        };
        for tx in txs.into_iter().flatten() {
            if let Some(f) = fees_from_tx(&tx, &pump) {
                return Ok(f);
            }
        }
    }
    Err(anyhow!("coin_fee для {mint} не найден (нет свежих buy/sell монеты)"))
}

/// Выбрать coin_fee_A/B из одной разобранной транзакции (top-level + inner).
fn fees_from_tx(tx: &serde_json::Value, pump: &str) -> Option<CoinFees> {
    let mut all: Vec<serde_json::Value> = tx["transaction"]["message"]["instructions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if let Some(inner) = tx["meta"]["innerInstructions"].as_array() {
        for grp in inner {
            if let Some(g) = grp["instructions"].as_array() {
                all.extend(g.iter().cloned());
            }
        }
    }
    for ins in &all {
        if ins["programId"].as_str() != Some(pump) {
            continue;
        }
        let data = ins["data"].as_str().unwrap_or("");
        let bytes = bs58::decode(data).into_vec().unwrap_or_default();
        if bytes.len() < 8 {
            continue;
        }
        let accs = ins["accounts"].as_array()?;
        let (ia, ib) = if bytes[..8] == DISC_BUY_EXACT_SOL_IN && accs.len() >= 18 {
            (16usize, 17usize)
        } else if bytes[..8] == DISC_SELL && accs.len() >= 16 {
            (14usize, 15usize)
        } else {
            continue;
        };
        let a = accs[ia].as_str().and_then(|s| Pubkey::from_str(s).ok());
        let b = accs[ib].as_str().and_then(|s| Pubkey::from_str(s).ok());
        if let (Some(coin_fee_a), Some(coin_fee_b)) = (a, b) {
            return Some(CoinFees { coin_fee_a, coin_fee_b });
        }
    }
    None
}

/// Собрать `buy_exact_sol_in` ПОЛНОСТЬЮ из деривации (18 аккаунтов) + coin_fee.
/// data = disc + sol_in:u64 + min_tokens_out:u64 (24 байта, без хвоста).
pub fn build_buy_exact(
    payer: &Pubkey,
    mint: &Pubkey,
    creator: &Pubkey,
    coin_token_program: &Pubkey,
    fees: &CoinFees,
    sol_in: u64,
    min_tokens_out: u64,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new_readonly(PUMP_GLOBAL, false),                           // 0
        AccountMeta::new(PUMP_FEE_RECIPIENT, false),                             // 1
        AccountMeta::new_readonly(*mint, false),                                 // 2
        AccountMeta::new(bonding_curve(mint), false),                            // 3
        AccountMeta::new(assoc_bonding_curve(mint, coin_token_program), false),  // 4
        AccountMeta::new(ata_with(payer, mint, coin_token_program), false),      // 5
        AccountMeta::new(*payer, true),                                          // 6 signer
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false),                        // 7
        AccountMeta::new_readonly(*coin_token_program, false),                   // 8
        AccountMeta::new(creator_vault(creator), false),                         // 9
        AccountMeta::new_readonly(PUMP_EVENT_AUTHORITY, false),                  // 10
        AccountMeta::new_readonly(PUMP_PROGRAM, false),                          // 11
        AccountMeta::new(PUMP_GLOBAL_VOLUME_ACC, false),                         // 12
        AccountMeta::new(user_volume_accumulator(payer), false),                // 13
        AccountMeta::new_readonly(PUMP_FEE_CONFIG, false),                       // 14
        AccountMeta::new_readonly(PUMP_FEE_PROGRAM, false),                      // 15
        AccountMeta::new_readonly(fees.coin_fee_a, false),                       // 16
        AccountMeta::new(fees.coin_fee_b, false),                                // 17
    ];
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&DISC_BUY_EXACT_SOL_IN);
    data.extend_from_slice(&sol_in.to_le_bytes());
    data.extend_from_slice(&min_tokens_out.to_le_bytes());
    Instruction { program_id: PUMP_PROGRAM, accounts, data }
}

/// Собрать `sell` ПОЛНОСТЬЮ из деривации (16 аккаунтов) + coin_fee.
/// data = disc + token_amount:u64 + min_sol_out:u64.
pub fn build_sell_derived(
    payer: &Pubkey,
    mint: &Pubkey,
    creator: &Pubkey,
    coin_token_program: &Pubkey,
    fees: &CoinFees,
    token_amount: u64,
    min_sol_out: u64,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new_readonly(PUMP_GLOBAL, false),                           // 0
        AccountMeta::new(PUMP_FEE_RECIPIENT, false),                             // 1
        AccountMeta::new_readonly(*mint, false),                                 // 2
        AccountMeta::new(bonding_curve(mint), false),                            // 3
        AccountMeta::new(assoc_bonding_curve(mint, coin_token_program), false),  // 4
        AccountMeta::new(ata_with(payer, mint, coin_token_program), false),      // 5
        AccountMeta::new(*payer, true),                                          // 6 signer
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false),                        // 7
        AccountMeta::new(creator_vault(creator), false),                         // 8
        AccountMeta::new_readonly(*coin_token_program, false),                   // 9
        AccountMeta::new_readonly(PUMP_EVENT_AUTHORITY, false),                  // 10
        AccountMeta::new_readonly(PUMP_PROGRAM, false),                          // 11
        AccountMeta::new_readonly(PUMP_FEE_CONFIG, false),                       // 12
        AccountMeta::new_readonly(PUMP_FEE_PROGRAM, false),                      // 13
        AccountMeta::new_readonly(fees.coin_fee_a, false),                       // 14
        AccountMeta::new(fees.coin_fee_b, false),                                // 15
    ];
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&DISC_SELL);
    data.extend_from_slice(&token_amount.to_le_bytes());
    data.extend_from_slice(&min_sol_out.to_le_bytes());
    Instruction { program_id: PUMP_PROGRAM, accounts, data }
}

// ─────────────────────────── состояние кривой ───────────────────────────

#[derive(Debug, Clone)]
pub struct Curve {
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub complete: bool,
    pub creator: Pubkey,
}

/// Парс аккаунта bonding curve (классический layout, с creator на хвосте).
pub fn parse_curve(data: &[u8]) -> Result<Curve> {
    // 8 disc + 5×u64 + bool + Pubkey
    if data.len() < 8 + 8 * 5 + 1 + 32 {
        return Err(anyhow!("короткий bonding curve ({} байт)", data.len()));
    }
    let g = |o: usize| u64::from_le_bytes(data[o..o + 8].try_into().unwrap());
    let vtr = g(8);
    let vsr = g(16);
    let rtr = g(24);
    let rsr = g(32);
    let complete = data[48] != 0;
    let creator = Pubkey::try_from(&data[49..81]).map_err(|_| anyhow!("creator parse"))?;
    Ok(Curve {
        virtual_token_reserves: vtr,
        virtual_sol_reserves: vsr,
        real_token_reserves: rtr,
        real_sol_reserves: rsr,
        complete,
        creator,
    })
}

impl Curve {
    /// Оценка токенов на выходе за `sol_in` lamports (constant product,
    /// без учёта 1% комиссии pump — поэтому ставим как ОЖИДАЕМОЕ, а кап
    /// по SOL задаём отдельно со слиппеджем).
    pub fn tokens_out_for_sol(&self, sol_in: u64) -> u64 {
        let vsr = self.virtual_sol_reserves as u128;
        let vtr = self.virtual_token_reserves as u128;
        let sin = sol_in as u128;
        // tokens_out = vtr - k/(vsr+sin), k = vsr*vtr
        let k = vsr.saturating_mul(vtr);
        let new_vtr = k / (vsr + sin);
        (vtr.saturating_sub(new_vtr)) as u64
    }

    /// Оценка SOL за продажу `tokens_in`.
    pub fn sol_out_for_tokens(&self, tokens_in: u64) -> u64 {
        let vsr = self.virtual_sol_reserves as u128;
        let vtr = self.virtual_token_reserves as u128;
        let tin = tokens_in as u128;
        let k = vsr.saturating_mul(vtr);
        let new_vsr = k / (vtr + tin);
        (vsr.saturating_sub(new_vsr)) as u64
    }
}

// ─────────────────────────── инструкции ───────────────────────────

/// Общий account-лист для buy/sell классического pump.
fn accounts(mint: &Pubkey, curve: &Pubkey, user: &Pubkey, creator: &Pubkey) -> Vec<AccountMeta> {
    let assoc_curve = ata(curve, mint);
    let assoc_user = ata(user, mint);
    vec![
        AccountMeta::new_readonly(PUMP_GLOBAL, false),
        AccountMeta::new(PUMP_FEE_RECIPIENT, false),
        AccountMeta::new_readonly(*mint, false),
        AccountMeta::new(*curve, false),
        AccountMeta::new(assoc_curve, false),
        AccountMeta::new(assoc_user, false),
        AccountMeta::new(*user, true),
        AccountMeta::new_readonly(SYSTEM_PROGRAM, false),
        AccountMeta::new_readonly(TOKEN_PROGRAM, false),
        AccountMeta::new(creator_vault(creator), false),
        AccountMeta::new_readonly(PUMP_EVENT_AUTHORITY, false),
        AccountMeta::new_readonly(PUMP_PROGRAM, false),
    ]
}

/// BUY: `token_amount` токенов на выходе, не дороже `max_sol_cost` lamports.
pub fn build_buy(
    mint: &Pubkey,
    user: &Pubkey,
    creator: &Pubkey,
    token_amount: u64,
    max_sol_cost: u64,
) -> Instruction {
    let curve = bonding_curve(mint);
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&DISC_BUY);
    data.extend_from_slice(&token_amount.to_le_bytes());
    data.extend_from_slice(&max_sol_cost.to_le_bytes());
    Instruction {
        program_id: PUMP_PROGRAM,
        accounts: accounts(mint, &curve, user, creator),
        data,
    }
}

/// SELL: `token_amount` токенов, не меньше `min_sol_output` lamports.
pub fn build_sell(
    mint: &Pubkey,
    user: &Pubkey,
    creator: &Pubkey,
    token_amount: u64,
    min_sol_output: u64,
) -> Instruction {
    let curve = bonding_curve(mint);
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&DISC_SELL);
    data.extend_from_slice(&token_amount.to_le_bytes());
    data.extend_from_slice(&min_sol_output.to_le_bytes());
    Instruction {
        program_id: PUMP_PROGRAM,
        accounts: accounts(mint, &curve, user, creator),
        data,
    }
}
