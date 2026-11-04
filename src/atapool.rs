//! ПОСТОЯННЫЙ ПУЛ ATA ПОД REWARD-АКТИВЫ.
//!
//! Раздача OTC никому не создаёт ATA — проверено на трёх подряд раздачах:
//! 12 получателей, все 12 уже имели аккаунт до транзакции, инструкций создания
//! ноль, только переводы Tokenkeg. Кто не открыл аккаунт заранее, не получает
//! ничего, сколько бы монеты он ни держал.
//!
//! Открывать аккаунт в горячем пути, перед покупкой, плохо по трём причинам:
//!   1. лишняя транзакция ровно там, где идёт гонка за вход;
//!   2. если она не приземлится, мы входим в позицию, по которой НЕ ЗАПЛАТЯТ,
//!      и узнаём об этом только по нулевой сверке;
//!   3. одни и те же активы открываются заново под каждую монету.
//!
//! Поэтому держим аккаунты открытыми ПОСТОЯННО. Это дёшево: замер борды
//! 04.11.2026 (1000 свежих монет) дал всего 126 разных текущих reward-активов
//! и 241 с учётом корзин, а рента ВОЗВРАТНАЯ. Кривая покрытия:
//!
//!   топ  10 активов -> 47.2% монет борды   рента 0.020 SOL
//!   топ  30 активов -> 77.7% монет борды   рента 0.061 SOL
//!   топ  50 активов -> 87.4% монет борды   рента 0.102 SOL
//!   топ 100 активов -> 97.4% монет борды   рента 0.204 SOL
//!   топ 124 актива  -> 99.8% монет борды   рента 0.253 SOL
//!
//! Второе следствие важнее первого: раз пул известен заранее, вход можно
//! ГЕЙТОВАТЬ по нему. Монета, под чей актив аккаунта нет, отсеивается одним
//! обращением к памяти — до всякого RPC, и мы больше не способны купить то,
//! по чему нам не могут заплатить.

use crate::rpc::Rpc;
use crate::txbuild::{build_signed, compute_budget};
use anyhow::Result;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Сколько созданий кладём в одну транзакцию. Создание ATA это ~25k CU и
/// четыре аккаунта, так что восемь укладываются с запасом по обоим лимитам.
const PER_TX: usize = 8;
/// Потолок созданий за один проход прогрева: первый запуск открывает сотню
/// аккаунтов, и лить их одним залпом незачем — следующий проход добьёт остаток.
const MAX_PER_PASS: usize = 48;
/// Размер аккаунта Token-2022 с расширением ImmutableOwner (его ставит
/// create_idempotent). Классический SPL-аккаунт меньше, 165 байт, поэтому
/// считаем бюджет по большему и не ошибаемся в минус.
const ATA_LEN_2022: usize = 170;

pub struct AtaPool {
    /// Минты, под которые аккаунт ПОДТВЕРЖДЁН на чейне. Пополняется только
    /// проверкой существования, никогда оптимистично по факту отправки:
    /// гейт входа опирается на этот набор, и ошибка здесь стоит позиции.
    ready: Mutex<HashSet<Pubkey>>,
    /// Минты, которые пул ВЕДЁТ (подтверждённые плюс те, что ещё открываем).
    /// Отдаётся инсинератору как запрет на сжигание.
    tracked: Mutex<HashSet<Pubkey>>,
    /// mint -> token-программа минта. Угадывать её нельзя: ATA под неверной
    /// программой это другой адрес, то есть выплата мимо нас.
    programs: Mutex<HashMap<Pubkey, Pubkey>>,
    /// Прошёл ли ХОТЬ ОДИН полный проход прогрева. До этого входы на паузе.
    warmed: AtomicBool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WarmStats {
    /// Сколько активов попало в целевой список (ограничен бюджетом ренты).
    pub target: usize,
    /// Из них аккаунт уже открыт.
    pub open: usize,
    /// Отправлено созданий за этот проход.
    pub created: usize,
    /// Рента, лежащая в открытых аккаунтах (возвратная).
    pub rent_locked_sol: f64,
    /// Доля монет борды, под чей текущий актив у нас есть аккаунт.
    pub coverage_pct: f64,
}

impl AtaPool {
    pub fn new() -> Self {
        Self {
            ready: Mutex::new(HashSet::new()),
            tracked: Mutex::new(HashSet::new()),
            programs: Mutex::new(HashMap::new()),
            warmed: AtomicBool::new(false),
        }
    }

    /// Готов ли пул к работе (прошёл первый проход прогрева).
    pub fn warmed(&self) -> bool {
        self.warmed.load(Ordering::Relaxed)
    }

    /// Есть ли у нас ПОДТВЕРЖДЁННЫЙ аккаунт под этот актив. Это и есть гейт
    /// входа: без аккаунта раздача пройдёт мимо, какой бы ни была доля.
    pub async fn is_ready(&self, mint: &Pubkey) -> bool {
        self.ready.lock().await.contains(mint)
    }

    pub async fn ready_count(&self) -> usize {
        self.ready.lock().await.len()
    }

    /// Минты, которые нельзя сжигать инсинератором: он закрывает ПУСТЫЕ
    /// классические токен-аккаунты, а пустой reward-ATA это ровно наше право
    /// на выплату. Без этого запрета прогрев и инсинератор отменяли бы друг
    /// друга — reward-активы встречаются и в классической программе (раздача,
    /// которую мы разобрали, шла целиком через Tokenkeg).
    pub async fn protected_mints(&self) -> HashSet<Pubkey> {
        self.tracked.lock().await.clone()
    }

    /// Прогрев: привести пул в соответствие с ранжированным списком активов.
    ///
    /// `ranked` — активы в порядке приоритета с весом (сколько монет борды
    /// платят этим активом СЕЙЧАС). Вес нужен только для отчёта о покрытии.
    /// `budget_sol` — сколько ренты разрешено держать залоченной,
    /// `reserve_sol` — сколько SOL обязано остаться свободным на покупки.
    pub async fn refresh(
        &self,
        rpc: &Rpc,
        payer: &Keypair,
        ranked: &[(Pubkey, u64)],
        budget_sol: f64,
        reserve_sol: f64,
        cu_price: u64,
        dry: bool,
    ) -> Result<WarmStats> {
        let me = payer.pubkey();
        let total_weight: u64 = ranked.iter().map(|(_, w)| *w).sum();

        // Рента за аккаунт — у сети, а не из константы в коде.
        let rent: u64 = rpc
            .raw(
                "getMinimumBalanceForRentExemption",
                serde_json::json!([ATA_LEN_2022]),
            )
            .await
            .ok()
            .and_then(|v| v.as_u64())
            .unwrap_or(2_074_080);

        // Бюджет ренты задаёт, сколько активов мы вообще можем вести.
        let max_open = ((budget_sol * 1e9) / rent as f64).floor().max(0.0) as usize;
        let target: Vec<(Pubkey, u64)> = ranked.iter().take(max_open).copied().collect();
        if target.is_empty() {
            self.warmed.store(true, Ordering::Relaxed);
            return Ok(WarmStats::default());
        }

        // 1. Token-программа минта. Спрашиваем только про незнакомые.
        {
            let unknown: Vec<Pubkey> = {
                let known = self.programs.lock().await;
                target
                    .iter()
                    .map(|(m, _)| *m)
                    .filter(|m| !known.contains_key(m))
                    .collect()
            };
            for chunk in unknown.chunks(100) {
                let keys: Vec<String> = chunk.iter().map(|m| m.to_string()).collect();
                let v = match rpc
                    .raw(
                        "getMultipleAccounts",
                        serde_json::json!([keys, {"encoding":"base64"}]),
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("прогрев ATA: не прочитать минты ({e})");
                        continue;
                    }
                };
                let vals = v["value"].as_array().cloned().unwrap_or_default();
                let mut progs = self.programs.lock().await;
                for (m, item) in chunk.iter().zip(vals.iter()) {
                    // Минт не прочитался — НЕ угадываем программу: неверный
                    // адрес ATA это молча пропущенная выплата.
                    let Some(owner) = item["owner"].as_str() else {
                        continue;
                    };
                    if let Ok(p) = Pubkey::from_str(owner) {
                        progs.insert(*m, p);
                    }
                }
            }
        }

        // 2. Что из целевого списка уже открыто.
        let mut open: HashSet<Pubkey> = HashSet::new();
        let progs = self.programs.lock().await.clone();
        let resolved: Vec<(Pubkey, Pubkey)> = target
            .iter()
            .filter_map(|(m, _)| progs.get(m).map(|p| (*m, *p)))
            .collect();
        for chunk in resolved.chunks(100) {
            let atas: Vec<String> = chunk
                .iter()
                .map(|(m, p)| {
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &me, m, p,
                    )
                    .to_string()
                })
                .collect();
            let v = match rpc
                .raw(
                    "getMultipleAccounts",
                    serde_json::json!([atas, {"encoding":"base64"}]),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("прогрев ATA: не проверить аккаунты ({e})");
                    continue;
                }
            };
            let vals = v["value"].as_array().cloned().unwrap_or_default();
            for ((m, _), item) in chunk.iter().zip(vals.iter()) {
                if !item.is_null() {
                    open.insert(*m);
                }
            }
        }

        // 3. Открываем недостающие — в порядке приоритета, пока хватает и
        //    бюджета ренты, и свободного остатка на покупку.
        let missing: Vec<(Pubkey, Pubkey)> = resolved
            .iter()
            .filter(|(m, _)| !open.contains(m))
            .copied()
            .collect();
        let balance = rpc.lamports(&me).await.unwrap_or(0);
        let reserve = (reserve_sol * 1e9) as u64;
        let spendable = balance.saturating_sub(reserve);
        let affordable = (spendable / rent.max(1)) as usize;
        let take = missing.len().min(MAX_PER_PASS).min(affordable);
        if take < missing.len() {
            info!(
                "прогрев ATA: откроем {take} из {} недостающих (бюджет ренты {budget_sol:.3} SOL, резерв на покупку {reserve_sol:.2} SOL, баланс {:.3} SOL)",
                missing.len(),
                balance as f64 / 1e9
            );
        }
        let mut created = 0usize;
        let mut attempted: Vec<(Pubkey, Pubkey)> = Vec::new();
        for chunk in missing[..take].chunks(PER_TX) {
            let ixs: Vec<Instruction> = chunk
                .iter()
                .map(|(m, p)| {
                    spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                        &me, &me, m, p,
                    )
                })
                .collect();
            if dry {
                info!("[DRY] открыл бы {} ATA под reward-активы", ixs.len());
                created += ixs.len();
                continue;
            }
            let mut all = compute_budget(40_000 * chunk.len() as u32, cu_price).to_vec();
            all.extend(ixs);
            let bh = match rpc.latest_blockhash().await {
                Ok(b) => b,
                Err(e) => {
                    warn!("прогрев ATA: нет blockhash ({e})");
                    break;
                }
            };
            // Обычным RPC, а не через релеи-сендеры: это не гонка, тут не нужны
            // ни tip, ни веер. Preflight оставлен включённым — ошибку создания
            // хочется видеть сразу, а не искать потом по нулевым сверкам, — но
            // `preflightCommitment` ОБЯЗАН быть не строже того, на котором взят
            // blockhash. Без него узел симулировал на finalized, свежего
            // блокхэша ещё не знал и отбивал ВСЁ подряд `BlockhashNotFound`.
            // Торговый путь этого не видел: там skipPreflight.
            match build_signed(payer, &all, bh, &[], &[])
                .and_then(|tx| {
                    bincode::serialize(&tx).map_err(|e| anyhow::anyhow!("serialize: {e}"))
                })
                .map(|raw| B64.encode(raw))
            {
                Ok(wire) => match rpc
                    .raw(
                        "sendTransaction",
                        serde_json::json!([wire, {
                            "encoding":"base64",
                            "preflightCommitment":"processed",
                            "maxRetries":5
                        }]),
                    )
                    .await
                {
                    Ok(_) => {
                        created += chunk.len();
                        attempted.extend_from_slice(chunk);
                    }
                    Err(e) => warn!("прогрев ATA: отправка не прошла ({e})"),
                },
                Err(e) => warn!("прогрев ATA: сборка не прошла ({e})"),
            }
        }

        // 4. Подтверждаем отправленное. В набор ready попадает только то, что
        //    реально видно на чейне, — иначе гейт входа начнёт врать.
        if !attempted.is_empty() {
            tokio::time::sleep(std::time::Duration::from_secs(12)).await;
            for chunk in attempted.chunks(100) {
                let atas: Vec<String> = chunk
                    .iter()
                    .map(|(m, p)| {
                        spl_associated_token_account::get_associated_token_address_with_program_id(
                            &me, m, p,
                        )
                        .to_string()
                    })
                    .collect();
                if let Ok(v) = rpc
                    .raw(
                        "getMultipleAccounts",
                        serde_json::json!([atas, {"encoding":"base64"}]),
                    )
                    .await
                {
                    let vals = v["value"].as_array().cloned().unwrap_or_default();
                    for ((m, _), item) in chunk.iter().zip(vals.iter()) {
                        if !item.is_null() {
                            open.insert(*m);
                        }
                    }
                }
            }
        }

        // 5. Публикуем состояние.
        let covered: u64 = target
            .iter()
            .filter(|(m, _)| open.contains(m))
            .map(|(_, w)| *w)
            .sum();
        let coverage_pct = if total_weight > 0 {
            covered as f64 * 100.0 / total_weight as f64
        } else {
            0.0
        };
        let stats = WarmStats {
            target: target.len(),
            open: open.len(),
            created,
            rent_locked_sol: open.len() as f64 * rent as f64 / 1e9,
            coverage_pct,
        };
        {
            // В DRY аккаунты не создаются, поэтому «реально открытых» почти нет,
            // и гейт входа отсеял бы всех кандидатов подряд — симуляция
            // проверяла бы только сама себя. Поэтому в dry готовым считается то,
            // что пул ВЁЛ БЫ: гейт тогда проверяет настоящее условие («актив в
            // нашем пуле»), а не наличие аккаунта, которого в dry быть и не может.
            let mut r = self.ready.lock().await;
            *r = if dry {
                let mut eff = open.clone();
                eff.extend(target.iter().map(|(m, _)| *m));
                eff
            } else {
                open.clone()
            };
        }
        {
            let mut t = self.tracked.lock().await;
            // Ведём и то, что ещё не открылось: инсинератор не должен сжечь
            // аккаунт, который мы только что создали, пока он пустой.
            t.extend(target.iter().map(|(m, _)| *m));
        }
        // В dry аккаунты не создаются, поэтому гейт входа держал бы бота молча
        // закрытым. Прогрев в dry считается состоявшимся: там проверяется
        // логика отбора, а не право на выплату.
        if dry || stats.open > 0 {
            self.warmed.store(true, Ordering::Relaxed);
        }
        Ok(stats)
    }
}
