//! Торговая логика: вход в лоукап перед раундом → спам-выход с Lighthouse-гейтом
//! → сброс накопленных наград. Плюс фоновые задачи (награды, инсинератор).
//!
//! Модель тайминга (честно про допущения):
//!  • ВХОД: покупаем, когда накопленная доля держателей (pending_reward_sol)
//!    из OTC API попадает в окно фильтра [min,max] — это практический эквивалент
//!    «раунд близко». Разобранный бот ещё и гейтил сам buy Lighthouse-ассертом;
//!    точный порог — приватное состояние протокола, поэтому воспроизводим его
//!    через фильтр pending, а не через ассерт на вход.
//!  • ВЫХОД: спамим заранее подписанный sell с Lighthouse-ассертом на ATA
//!    reward-кошелька: «баланс стока вырос ≥ epsilon» ⇒ раунд выдал сток,
//!    снапшот уже снят, нас зачли — можно сбрасывать монету. Пока условие
//!    не выполнено, tx падает на ассерте и ничего не стоит (с durable nonce
//!    nonce не расходуется). Это ровно та механика, что видно в ончейне.

use crate::atapool::AtaPool;
use crate::config::Config;
use crate::filters::{Filters, Verdict};
use crate::jupiter::Jupiter;
use crate::lighthouse::{assert_account_lamports, assert_token_amount, Op};
use crate::nonce;
use crate::oracle::{Candidate, Oracle};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use crate::programs::{TOKEN_2022_PROGRAM, TOKEN_PROGRAM, WSOL_MINT};
use crate::pumpfun;
use crate::pumpswap;
use crate::relay::RelaySet;
use crate::reporter::{Fees, Reporter, TradeReport};
use crate::rpc::Rpc;
use crate::shreds::ShredEvent;
use crate::txbuild::{compute_budget, no_alts};
use anyhow::{anyhow, Result};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify, Semaphore};
use tracing::{info, warn};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

pub struct Bot {
    pub cfg: Config,
    pub rpc: Rpc,
    pub relays: RelaySet,
    pub jup: Jupiter,
    pub oracle: Oracle,
    pub filters: Filters,
    pub payer: Arc<Keypair>,
    pub reporter: Arc<Reporter>,
    active: Arc<Mutex<HashSet<Pubkey>>>,
    /// Жёсткий потолок одновременных позиций (race-free). Пермит берётся ПЕРЕД
    /// спавном задачи и живёт до её конца. При max_concurrent_positions=1 это
    /// гарантирует: в любой момент открыта максимум ОДНА позиция, даже если
    /// два тика входа (интервал + будилка от шредов) наложились.
    slots: Arc<Semaphore>,
    /// Какие reward-стоки уже фармим: не держим два разных мемкоина на один сток,
    /// иначе раунд одного ложно триггерит выход другого (общий ATA стока).
    active_reward_mints: Arc<Mutex<HashSet<Pubkey>>>,
    /// Кэш курса SOL (цена, когда обновляли) для EV-фильтра.
    sol_price: Arc<Mutex<(f64, std::time::Instant)>>,
    /// reward_token_account → будилки открытых позиций, платящих этим стоком.
    /// Шред-поток дёргает их в момент раунда → мгновенный выход.
    exit_registry: Arc<Mutex<HashMap<Pubkey, Vec<Arc<Notify>>>>>,
    /// Монеты, у которых не нашли buy-шаблон → не долбим их каждый тик, даём
    /// кулдаун и пробуем ДРУГИХ кандидатов (иначе одна дохлая монета в топе
    /// борды застревает и блокирует входы). mint → когда отметили.
    no_template: Arc<Mutex<HashMap<Pubkey, std::time::Instant>>>,
    /// Кэш coin_fee_A/B по монете: они стабильны per-coin, поэтому скрейпим ОДИН
    /// раз (40 getTransaction) и дальше берём из кэша — иначе на каждый тик упираемся
    /// в rate-limit RPC (429 → битые ответы, флап входа).
    coin_fees: Arc<Mutex<HashMap<Pubkey, pumpfun::CoinFees>>>,
    /// bonding_curve → mint кандидатов (обновляется в tick_entries). Нужно, чтобы по
    /// шред-событию раунда (где фигурирует кривая) понять, КАКАЯ монета раундилась.
    curve_to_mint: Arc<Mutex<HashMap<Pubkey, Pubkey>>>,
    /// mint → времена недавних раундов (из шредов). Частота раундов = сигнал, что
    /// монета «живая» и раунд придёт, пока держим.
    round_times: Arc<Mutex<HashMap<Pubkey, VecDeque<std::time::Instant>>>>,
    /// mint → когда последний раз торговали (кулдаун на повторный вход).
    recent_trade: Arc<Mutex<HashMap<Pubkey, std::time::Instant>>>,
    /// Будилка цикла входа: шреды будят немедленный опрос борды OTC.
    poll_now: Arc<Notify>,
    /// ВОТЧЛИСТ: кандидаты со ВСЕЙ борды, отранжированные по РЕАЛЬНОМУ котлу
    /// из чейна. Это и есть источник отбора — см. `sweep_board`. Прежний
    /// источник (десять страниц «свежих») цели не содержал: котла в списочном
    /// ответе борды нет, сортировать по нему API не умеет, а монеты с набранным
    /// котлом рассыпаны по всем 67 тысячам записей.
    board: Arc<Mutex<Vec<Candidate>>>,
    /// mint → creator для монет вотчлиста. Котёл живёт на PDA создателя, и эта
    /// карта избавляет быстрый перечёт от повторного чтения кривых.
    watch_creators: Arc<Mutex<HashMap<Pubkey, Pubkey>>>,
    /// mint → (котёл SOL за вычетом ренты, прирост SOL/мин, когда измерено).
    pot_cache: Arc<Mutex<HashMap<Pubkey, (f64, f64, std::time::Instant)>>>,
    /// Рента аккаунта-котла в лампортах (0 = ещё не спрашивали). Клейм её не
    /// забирает, поэтому пот = lamports − рента.
    vault_rent_lamports: std::sync::atomic::AtomicU64,
    /// mint -> hook ("koth" и т.п.), None = обычная раздача. Кэш на весь
    /// процесс: хук у монеты не меняется, а запрос per-mint, в пагинации его нет.
    hook_cache: Arc<Mutex<HashMap<Pubkey, Option<String>>>>,
    /// Когда последний раз печатали каждое сообщение «входы на паузе».
    /// Тик входа идёт каждые `poll_ms` (400 мс), поэтому без троттла пауза
    /// заливает журнал: на первом запуске это 300 строк за две минуты, в
    /// которых тонет всё остальное.
    pause_log: Arc<Mutex<HashMap<&'static str, std::time::Instant>>>,
    /// ПОСТОЯННО ОТКРЫТЫЕ ATA под reward-активы. Раздача аккаунтов не создаёт,
    /// поэтому право на выплату нужно иметь ЗАРАНЕЕ, а не добывать транзакцией
    /// перед покупкой. Пул же позволяет гейтовать вход: монета, под чей актив
    /// аккаунта нет, отсеивается до всякого RPC.
    ata_pool: Arc<AtaPool>,
}

impl Bot {
    pub fn new(
        cfg: Config,
        rpc: Rpc,
        relays: RelaySet,
        jup: Jupiter,
        oracle: Oracle,
        filters: Filters,
        payer: Arc<Keypair>,
        reporter: Arc<Reporter>,
    ) -> Self {
        let slot_count = cfg.strategy.max_concurrent_positions.max(1);
        Self {
            cfg,
            rpc,
            relays,
            jup,
            oracle,
            filters,
            payer,
            reporter,
            active: Arc::new(Mutex::new(HashSet::new())),
            slots: Arc::new(Semaphore::new(slot_count)),
            active_reward_mints: Arc::new(Mutex::new(HashSet::new())),
            sol_price: Arc::new(Mutex::new((0.0, std::time::Instant::now()))),
            exit_registry: Arc::new(Mutex::new(HashMap::new())),
            board: Arc::new(Mutex::new(Vec::new())),
            watch_creators: Arc::new(Mutex::new(HashMap::new())),
            pot_cache: Arc::new(Mutex::new(HashMap::new())),
            vault_rent_lamports: std::sync::atomic::AtomicU64::new(0),
            hook_cache: Arc::new(Mutex::new(HashMap::new())),
            pause_log: Arc::new(Mutex::new(HashMap::new())),
            ata_pool: Arc::new(AtaPool::new()),
            no_template: Arc::new(Mutex::new(HashMap::new())),
            coin_fees: Arc::new(Mutex::new(HashMap::new())),
            curve_to_mint: Arc::new(Mutex::new(HashMap::new())),
            round_times: Arc::new(Mutex::new(HashMap::new())),
            recent_trade: Arc::new(Mutex::new(HashMap::new())),
            poll_now: Arc::new(Notify::new()),
        }
    }

    /// Кулдаун для монеты без найденного buy-шаблона (сек).
    const NO_TEMPLATE_COOLDOWN_SECS: u64 = 180;
/// Доля пота, уходящая ХОЛДЕРАМ. Остальное расписано в интерфейсе монеты:
/// протокол 12.5%, desk pot 10%, выкупы OTC 10%. Проверено на STOCKS, где
/// distributed/earned сошлось с этой долей до копейки.
const HOLDERS_CUT: f64 = 0.675;
/// Фактическая цена round-trip по монете, доля от позиции. Замер по 17 живым
/// сделкам: средняя −4.7%, минимальная −2.6% (две комиссии pump по 1% плюс
/// спред). Берём минимальную как базу для подбора размера.
const ROUND_TRIP_COST: f64 = 0.026;

    /// РЕНТА АККАУНТА-КОТЛА. Клейм её не забирает, поэтому забрать можно только
    /// то, что выше: пот = lamports − рента.
    ///
    /// Замер 04.11.2026 по семи клеймам монеты EVYRrux8: после каждого в котле
    /// остаётся РОВНО 0.000650 SOL = 650 240 лампортов =
    /// `getMinimumBalanceForRentExemption(0)` при текущей ставке ренты (2540
    /// лампортов за байт-год). Отсюда же поправка к прежнему выводу:
    /// расхождение +0.00089 между чейном и витриной OTC — не «чейн опережает
    /// их API», а ровно эта рента.
    ///
    /// Ставку НЕ хардкодим: сеть её уже снижала (было 3480, и 0-байтный
    /// аккаунт стоил 890 880). Спрашиваем один раз на процесс.
    async fn vault_rent(&self) -> u64 {
        use std::sync::atomic::Ordering;
        let cached = self.vault_rent_lamports.load(Ordering::Relaxed);
        if cached > 0 {
            return cached;
        }
        let got = self
            .rpc
            .raw("getMinimumBalanceForRentExemption", serde_json::json!([0]))
            .await
            .ok()
            .and_then(|v| v.as_u64())
            .unwrap_or(650_240);
        self.vault_rent_lamports.store(got, Ordering::Relaxed);
        got
    }

    /// Пот — то, что воркер реально может забрать из котла.
    fn pot_of(lamports: u64, rent: u64) -> f64 {
        lamports.saturating_sub(rent) as f64 / LAMPORTS_PER_SOL
    }

    /// Сообщение о паузе — не чаще раза в 30 секунд на причину. Причину знать
    /// надо (иначе «работает, но молчит» не отличить от «сломан»), но повторять
    /// её 2.5 раза в секунду незачем.
    async fn pause_note(&self, key: &'static str, msg: &str) {
        let mut m = self.pause_log.lock().await;
        let fire = m
            .get(key)
            .map(|t| t.elapsed().as_secs() >= 30)
            .unwrap_or(true);
        if fire {
            m.insert(key, std::time::Instant::now());
            info!("{msg}");
        }
    }

    /// Будилка цикла входа (её дёргает шред-консьюмер).
    pub fn poll_now(&self) -> Arc<Notify> {
        self.poll_now.clone()
    }

    /// Консьюмер шред-потока: раунд по нашему стоку → немедленный выход;
    /// любая активность reward-кошелька → немедленный опрос борды на вход.
    pub async fn consume_shreds(self: Arc<Self>, mut rx: mpsc::Receiver<ShredEvent>) {
        use crate::programs::{OTC_LAUNCHER_REWARDS, PUMPSWAP_PROGRAM};
        while let Some(ev) = rx.recv().await {
            // Будим открытые позиции, чей сток/минт фигурирует в событии (раунд → выход).
            let mut fired = false;
            {
                let reg = self.exit_registry.lock().await;
                if !reg.is_empty() {
                    for acc in &ev.accounts {
                        if let Some(notifies) = reg.get(acc) {
                            for n in notifies {
                                n.notify_one();
                                fired = true;
                            }
                        }
                    }
                }
            }
            if fired {
                info!("shred: раунд по нашей монете/стоку slot={} → триггер выхода", ev.slot);
            }
            // РАУНД OTC = событие кошелька раздачи + pAMM-своп (ревард→сток). По кривой
            // в аккаунтах понимаем, КАКАЯ монета раундилась, и копим частоту раундов.
            if ev.touches(&OTC_LAUNCHER_REWARDS) {
                if ev.uses_program(&PUMPSWAP_PROGRAM) {
                    let c2m = self.curve_to_mint.lock().await;
                    if !c2m.is_empty() {
                        for acc in &ev.accounts {
                            if let Some(mint) = c2m.get(acc) {
                                let mint = *mint;
                                drop(c2m);
                                self.record_round(&mint).await;
                                break;
                            }
                        }
                    }
                }
                self.poll_now.notify_one();
            }
        }
    }

    /// Записать раунд монеты (для частоты). Чистим старые (> round_window_secs).
    async fn record_round(&self, mint: &Pubkey) {
        let window = std::time::Duration::from_secs(self.cfg.strategy.round_window_secs);
        let now = std::time::Instant::now();
        let mut rt = self.round_times.lock().await;
        let dq = rt.entry(*mint).or_default();
        dq.push_back(now);
        while dq.front().map(|t| now.duration_since(*t) > window).unwrap_or(false) {
            dq.pop_front();
        }
        info!("shred: раунд монеты {mint} (всего в окне: {})", dq.len());
    }

    /// Сколько раундов у монеты в окне round_window_secs.
    async fn rounds_in_window(&self, mint: &Pubkey) -> usize {
        let window = std::time::Duration::from_secs(self.cfg.strategy.round_window_secs);
        let now = std::time::Instant::now();
        let mut rt = self.round_times.lock().await;
        match rt.get_mut(mint) {
            Some(dq) => {
                while dq.front().map(|t| now.duration_since(*t) > window).unwrap_or(false) {
                    dq.pop_front();
                }
                dq.len()
            }
            None => 0,
        }
    }

    async fn register_exit(&self, key: &Pubkey, notify: &Arc<Notify>) {
        self.exit_registry
            .lock()
            .await
            .entry(*key)
            .or_default()
            .push(Arc::clone(notify));
    }

    async fn deregister_exit(&self, key: &Pubkey, notify: &Arc<Notify>) {
        let mut reg = self.exit_registry.lock().await;
        if let Some(v) = reg.get_mut(key) {
            v.retain(|n| !Arc::ptr_eq(n, notify));
            if v.is_empty() {
                reg.remove(key);
            }
        }
    }

    /// Курс SOL с кэшем (обновляем не чаще раза в 60с). 0.0 → EV-гейт не применится.
    pub async fn sol_price_usd(&self) -> f64 {
        {
            let g = self.sol_price.lock().await;
            if g.0 > 0.0 && g.1.elapsed().as_secs() < 60 {
                return g.0;
            }
        }
        match self.jup.sol_price_usd().await {
            Ok(p) if p > 0.0 => {
                *self.sol_price.lock().await = (p, std::time::Instant::now());
                p
            }
            _ => self.sol_price.lock().await.0, // последний известный (или 0.0)
        }
    }

    /// Реальный квалифицированный флоат (USD) монеты из он-чейн держателей.
    /// None → фильтр откатится на допущение `qualified_float_pct`.
    async fn float_override(&self, c: &Candidate, sol_price: f64) -> Option<f64> {
        if sol_price <= 0.0 {
            return None;
        }
        let acc = self.rpc.account(&pumpfun::bonding_curve(&c.mint)).await.ok()??;
        let curve = pumpfun::parse_curve(&acc.data).ok()?;
        if curve.complete {
            return None; // выпустилась — пул-vault неизвестен, не считаем
        }
        // Спот-цена на кривой: SOL за 1 целый токен (pump = 6 decimals) → USD.
        let price_sol = (curve.virtual_sol_reserves as f64 / 1e9)
            / (curve.virtual_token_reserves as f64 / 1e6).max(1e-9);
        let price_usd = price_sol * sol_price;
        let exclude = crate::holders::excluded_accounts(&c.mint, &curve.creator);
        // КОНКУРИРУЮТ ЗА РАЗДАЧУ НЕ ВСЕ ДЕРЖАТЕЛИ, а только те, у кого уже
        // открыт ATA под reward-актив: раздача ATA не создаёт (проверено на
        // трёх раздачах — 12 получателей, все 12 с готовым ATA, ноль созданий).
        // Обычный флоат считал всех подряд и тем завышал знаменатель нашей доли.
        // Не узнали token-программу стока → НЕ считаем флоат по догадке: ATA
        // под чужой программой не найдутся, флоат выйдет нулевым, а наша доля —
        // ложными 100%. None здесь = «замера нет», и require_float_measure
        // отправит монету в скип.
        let reward_tp = pumpfun::coin_token_program(&self.rpc, &c.reward_mint).await?;
        if let Ok(bd) = crate::holders::eligible_float_usd(
            &self.rpc,
            &c.mint,
            &c.reward_mint,
            &reward_tp,
            price_usd,
            20.0,
            &exclude,
        )
        .await
        {
            info!(
                "{}: конкурентов с открытым ATA {} из {} держателей, их флоат ${:.0}",
                c.mint, bd.eligible_holders, bd.total_accounts, bd.float_usd
            );
            return Some(bd.float_usd);
        }
        // Запасной путь — прежний подсчёт по всем держателям.
        crate::holders::qualified_float_usd(&self.rpc, &c.mint, price_usd, 20.0, &exclude)
            .await
            .ok()
            .map(|bd| bd.float_usd)
    }

    /// Доступ к пулу ATA (инсинератору — чтобы он не сжёг наше право на выплату).
    pub fn ata_pool(&self) -> Arc<AtaPool> {
        Arc::clone(&self.ata_pool)
    }

    /// ЗАМЕР КОТЛОВ ПО БОРДЕ. Отвечает на один вопрос: где вообще лежат монеты
    /// с заполненным котлом, если борда сортировать по котлу НЕ УМЕЕТ.
    ///
    /// Проверено 04.11.2026: из всех значений `sort` борда принимает ровно два —
    /// `volume24h` и `marketCap`. Любое другое (в т.ч. наш `fresh`) она молча
    /// отдаёт дефолтным порядком. Котла в списочном ответе нет вообще: там
    /// только mint/name/symbol/image/createdAt/rewardMint/rewardBasket/
    /// rewardCycle/category. Значит котёл надо мерить самому.
    ///
    /// Путь целиком батчевый и целиком из деривации, без per-coin запросов:
    ///   mint -> bonding_curve(mint) -> creator (байты 49..81 аккаунта кривой)
    ///        -> creator_vault(creator) -> lamports
    /// 100 аккаунтов на запрос, то есть две батч-пачки на каждые 100 монет.
    ///
    /// ВАЖНО про `creator_vault`: он привязан к СОЗДАТЕЛЮ, а не к монете. Если
    /// один создатель выпустил несколько монет, фисы копятся в ОДИН котёл, и
    /// читать его «как котёл монеты» было бы завышением. Поэтому замер отдельно
    /// считает, сколько монет приходится на одного создателя.
    pub async fn scan_pots(&self, pages: usize, size: usize, sort: &str) -> Result<()> {
        let rent = self.vault_rent().await;
        let cands = self.oracle.candidates_with(pages, size, sort).await?;
        info!(
            "ЗАМЕР КОТЛОВ: сорт={sort}, {pages} страниц по {size} -> монет {} (рента котла {:.6} SOL вычитается)",
            cands.len(),
            rent as f64 / LAMPORTS_PER_SOL
        );

        // 1. Кривые батчами: оттуда creator и признак выпуска.
        let mut rows_in: Vec<(Pubkey, String, Pubkey)> = Vec::new();
        let mut complete = 0usize;
        let mut unread = 0usize;
        for chunk in cands.chunks(100) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|c| pumpfun::bonding_curve(&c.mint).to_string())
                .collect();
            let v = match self
                .rpc
                .raw(
                    "getMultipleAccounts",
                    serde_json::json!([keys, {"encoding":"base64"}]),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("замер котлов: кривые не прочитались ({e})");
                    unread += chunk.len();
                    continue;
                }
            };
            let vals = v["value"].as_array().cloned().unwrap_or_default();
            for (c, item) in chunk.iter().zip(vals.iter()) {
                let Some(b64) = item["data"][0].as_str() else {
                    unread += 1;
                    continue;
                };
                let Ok(raw) = B64.decode(b64) else {
                    unread += 1;
                    continue;
                };
                match pumpfun::parse_curve(&raw) {
                    Ok(cv) => {
                        if cv.complete {
                            complete += 1;
                        }
                        rows_in.push((c.mint, c.symbol.clone(), cv.creator));
                    }
                    Err(_) => unread += 1,
                }
            }
        }

        // 2. Котёл на создателя (дедуплицируем: котёл один на создателя).
        let creators: Vec<Pubkey> = rows_in
            .iter()
            .map(|(_, _, cr)| *cr)
            .collect::<HashSet<Pubkey>>()
            .into_iter()
            .collect();
        let mut pots: HashMap<Pubkey, f64> = HashMap::new();
        for chunk in creators.chunks(100) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|cr| pumpfun::creator_vault(cr).to_string())
                .collect();
            let Ok(v) = self
                .rpc
                .raw(
                    "getMultipleAccounts",
                    serde_json::json!([keys, {"encoding":"base64"}]),
                )
                .await
            else {
                continue;
            };
            let vals = v["value"].as_array().cloned().unwrap_or_default();
            for (cr, item) in chunk.iter().zip(vals.iter()) {
                let lam = item["lamports"].as_u64().unwrap_or(0);
                pots.insert(*cr, Self::pot_of(lam, rent));
            }
        }

        // 3. Сколько монет на создателя — насколько котёл «общий».
        let mut per_creator: HashMap<Pubkey, usize> = HashMap::new();
        for (_, _, cr) in &rows_in {
            *per_creator.entry(*cr).or_insert(0) += 1;
        }
        let shared: usize = per_creator.values().filter(|n| **n > 1).sum();
        let max_coins = per_creator.values().copied().max().unwrap_or(0);

        // 4. Распределение относительно НАШЕГО гейта входа.
        let floor = self.cfg.strategy.payout_floor_sol.max(1e-9);
        let gate = floor * self.cfg.strategy.min_vault_fill_pct / 100.0;
        let mut rows: Vec<(f64, String, Pubkey, usize)> = rows_in
            .iter()
            .map(|(m, sym, cr)| {
                (
                    pots.get(cr).copied().unwrap_or(0.0),
                    sym.clone(),
                    *m,
                    per_creator.get(cr).copied().unwrap_or(1),
                )
            })
            .collect();
        rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let n = rows.len().max(1);
        let cnt = |t: f64| rows.iter().filter(|r| r.0 >= t).count();
        info!(
            "кривых прочитано {} (не прочиталось {unread}), выпустившихся {complete}; создателей {} (монет у одного максимум {max_coins}, монет с общим котлом {shared})",
            rows_in.len(),
            creators.len()
        );
        info!(
            "котёл >= гейта {gate:.3} SOL: {} ({:.1}%) | >={floor:.2}: {} | >=0.05: {} | >=0.01: {} | ноль: {}",
            cnt(gate),
            cnt(gate) as f64 * 100.0 / n as f64,
            cnt(floor),
            cnt(0.05),
            cnt(0.01),
            rows.iter().filter(|r| r.0 <= 0.0).count()
        );
        let med = rows.get(rows.len() / 2).map(|r| r.0).unwrap_or(0.0);
        info!(
            "медиана котла {med:.4} SOL, максимум {:.4} SOL",
            rows.first().map(|r| r.0).unwrap_or(0.0)
        );
        for (pot, sym, mint, coins) in rows.iter().take(20) {
            info!("  {pot:.4} SOL  {sym:<10} {mint}  (монет у создателя: {coins})");
        }
        Ok(())
    }

    /// НАСТОЯЩИЙ ПОРОГ КЛЕЙМА, снятый с чейна, а не угаданный из `floorSol`.
    ///
    /// Клейм опустошает `creator-vault`, поэтому в транзакции клейма видно,
    /// сколько в котле лежало ДО — то есть при каком накоплении воркер OTC
    /// решает забирать фисы. Это ровно то число, на котором должен стоять гейт
    /// входа: входить надо под клейм, а не под произвольный процент от
    /// придуманного порога.
    ///
    /// Путь: mint -> bonding_curve -> creator -> creator_vault ->
    /// getSignaturesForAddress -> транзакции, где баланс котла УПАЛ.
    pub async fn scan_claims(&self, mints: &[String], limit: usize) -> Result<()> {
        let mut all_drops: Vec<f64> = Vec::new();
        for ms in mints {
            let Ok(mint) = ms.parse::<Pubkey>() else {
                warn!("не минт: {ms}");
                continue;
            };
            let curve = pumpfun::bonding_curve(&mint);
            let Ok(Some(acc)) = self.rpc.account(&curve).await else {
                warn!("{ms}: кривая не читается");
                continue;
            };
            let Ok(cv) = pumpfun::parse_curve(&acc.data) else {
                warn!("{ms}: кривая не парсится");
                continue;
            };
            let vault = pumpfun::creator_vault(&cv.creator);
            let now_lam = self.rpc.lamports(&vault).await.unwrap_or(0);
            let sigs = self
                .rpc
                .raw(
                    "getSignaturesForAddress",
                    serde_json::json!([vault.to_string(), {"limit": limit}]),
                )
                .await
                .unwrap_or(serde_json::Value::Null);
            let arr = sigs.as_array().cloned().unwrap_or_default();
            info!(
                "=== {ms} | котёл {vault} сейчас {:.6} SOL | транзакций котла {} ===",
                now_lam as f64 / LAMPORTS_PER_SOL,
                arr.len()
            );
            let mut drops = 0usize;
            for s in arr.iter() {
                let Some(sig) = s["signature"].as_str() else { continue };
                let Ok(t) = self
                    .rpc
                    .raw(
                        "getTransaction",
                        serde_json::json!([sig, {"encoding":"json","maxSupportedTransactionVersion":0}]),
                    )
                    .await
                else {
                    continue;
                };
                let keys = t["transaction"]["message"]["accountKeys"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let vs = vault.to_string();
                let Some(idx) = keys.iter().position(|k| k.as_str() == Some(vs.as_str())) else {
                    continue;
                };
                let pre = t["meta"]["preBalances"][idx].as_u64().unwrap_or(0) as f64
                    / LAMPORTS_PER_SOL;
                let post = t["meta"]["postBalances"][idx].as_u64().unwrap_or(0) as f64
                    / LAMPORTS_PER_SOL;
                if post < pre - 1e-9 {
                    drops += 1;
                    all_drops.push(pre);
                    let when = t["blockTime"].as_i64().unwrap_or(0);
                    info!(
                        "  КЛЕЙМ в {when}: котёл был {pre:.6} -> стал {post:.6} (забрали {:.6} SOL)",
                        pre - post
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            }
            if drops == 0 {
                info!("  клеймов в истории котла НЕТ");
            }
        }
        if !all_drops.is_empty() {
            all_drops.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let n = all_drops.len();
            info!(
                "ИТОГ по {n} клеймам: минимум {:.6} | медиана {:.6} | максимум {:.6} SOL",
                all_drops[0],
                all_drops[n / 2],
                all_drops[n - 1]
            );
        }
        Ok(())
    }

    /// REWARD-АКТИВЫ БОРДЫ, ранжированные по приоритету, с весом «сколько монет
    /// платят этим активом СЕЙЧАС». Только HTTP к борде, RPC не трогается.
    ///
    /// Вес в сортировке: текущий актив монеты считается вчетверо против членства
    /// в корзине. Платят ТЕКУЩИМ (в интерфейсе он помечен PAYING NOW), а корзина
    /// это страховка от ротации, которая случается уже после выплаты.
    pub async fn reward_ranking(&self) -> Result<Vec<(Pubkey, u64)>> {
        // ВСЯ борда, а не десять страниц «свежих». Пул ATA — это гейт входа:
        // монета, под чей reward-актив аккаунта нет, отсеивается до RPC. Греть
        // пул по одной вселенной монет, а кандидатов брать из другой — значит
        // резать именно то, за чем пришли: в первом драе так ушло 81 из 600
        // кандидатов вотчлиста. Замер покрытия (top 124 актива = 99.8% монет)
        // тоже считался по ВСЕЙ борде.
        let cands = self
            .oracle
            .candidates_all(60, self.cfg.strategy.board_sweep_concurrency as usize)
            .await?;
        let mut now: HashMap<Pubkey, u64> = HashMap::new();
        let mut inbasket: HashMap<Pubkey, u64> = HashMap::new();
        for c in &cands {
            *now.entry(c.reward_mint).or_insert(0) += 1;
            for b in &c.reward_basket {
                *inbasket.entry(*b).or_insert(0) += 1;
            }
        }
        let mut ranked: Vec<(Pubkey, u64)> = now
            .keys()
            .chain(inbasket.keys())
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|m| (*m, *now.get(m).unwrap_or(&0)))
            .collect();
        ranked.sort_by_key(|(m, w)| {
            let rank = w * 4 + inbasket.get(m).copied().unwrap_or(0);
            (std::cmp::Reverse(rank), m.to_bytes())
        });
        Ok(ranked)
    }

    /// ПРОГРЕВ ПУЛА ATA. Раз в `ata_warm_interval_secs` берёт борду, считает,
    /// какими активами платят монеты, и держит под верхушку этого списка
    /// постоянно открытые аккаунты.
    ///
    /// Почему не открывать перед покупкой (как было): создание ATA в горячем
    /// пути это лишняя транзакция ровно в момент гонки, а если она не
    /// приземлится, мы входим в позицию, по которой нам не заплатят. Активов на
    /// всю борду ~240, рента возвратная, так что дешевле держать их открытыми
    /// всегда: 0.25 SOL залога покрывают 99.8% монет.
    ///
    /// Вес актива: текущий reward-актив монеты весит вчетверо против членства в
    /// корзине. Платят ТЕКУЩИМ (в интерфейсе это PAYING NOW), а корзина это лишь
    /// страховка от ротации, которая случается после выплаты.
    pub async fn ata_warm_loop(self: Arc<Self>) {
        let every =
            std::time::Duration::from_secs(self.cfg.strategy.ata_warm_interval_secs.max(60));
        loop {
            match self.reward_ranking().await {
                Ok(ranked) => {
                    match self
                        .ata_pool
                        .refresh(
                            &self.rpc,
                            &self.payer,
                            &ranked,
                            self.cfg.strategy.ata_rent_budget_sol,
                            self.cfg.strategy.max_buy_sol + 0.05,
                            self.cfg.strategy.sell_cu_price.max(1_000),
                            self.cfg.strategy.dry_run,
                        )
                        .await
                    {
                        Ok(st) => info!(
                            "ПУЛ ATA{}: ведём {} активов, реально открыто {} (за проход {}), рента {:.3} SOL, покрытие борды {:.1}%",
                            if self.cfg.strategy.dry_run { " [DRY]" } else { "" },
                            st.target,
                            st.open,
                            st.created,
                            st.rent_locked_sol,
                            st.coverage_pct
                        ),
                        Err(e) => warn!("прогрев ATA: {e:#}"),
                    }
                }
                Err(e) => warn!("прогрев ATA: борда недоступна ({e})"),
            }
            tokio::time::sleep(every).await;
        }
    }

    /// ПОЛНЫЙ ОБХОД БОРДЫ ПО КОТЛУ — он же источник кандидатов.
    ///
    /// Прежний источник (десять страниц «свежих», 600 монет) не мог найти цель
    /// в принципе, и это измерено 04.11.2026: котла в списочном ответе борды
    /// нет вовсе, сортировать по котлу API не умеет (принимает ровно два
    /// сорта — `volume24h` и `marketCap`), а монеты с набранным котлом
    /// рассыпаны по всем 67 тысячам записей: котёл ≥0.05 SOL был у ПЯТИ монет
    /// из 67 714. Поэтому борду обходим целиком и считаем котлы сами:
    ///     mint → bonding_curve → creator (байты 49..81) → creator_vault
    ///          → lamports − рента
    /// Всё батчами по 100 аккаунтов, то есть несколько сотен RPC-вызовов на
    /// обход вместо 67 тысяч.
    ///
    /// Результат обхода — ВОТЧЛИСТ: top-N монет по поту. Тайминг входа от
    /// длительности обхода не зависит: котлы вотчлиста перечитывает
    /// `pot_refresh_loop` за секунды.
    pub async fn board_sweep_loop(self: Arc<Self>) {
        let every =
            std::time::Duration::from_secs(self.cfg.strategy.board_sweep_interval_secs.max(60));
        loop {
            let started = std::time::Instant::now();
            match self.sweep_board().await {
                Ok((seen, kept, best)) => info!(
                    "обход борды: {seen} монет на кривой за {:.0}с, вотчлист {kept}, лучший котёл {best:.4} SOL",
                    started.elapsed().as_secs_f64()
                ),
                Err(e) => warn!("обход борды: {e:#}"),
            }
            tokio::time::sleep(every.saturating_sub(started.elapsed())).await;
        }
    }

    /// Один полный обход борды: собрать вотчлист по РЕАЛЬНОМУ коту из чейна.
    /// Возвращает (монет на кривой, монет в вотчлисте, лучший пот в SOL).
    /// ХУКИ монет пачкой, с кэшем на весь процесс.
    ///
    /// `hook` приходит ТОЛЬКО в `/api/coins?mint=X` — в пагинации его нет
    /// (проверено на 26 760 записях списка). Поэтому один запрос на монету, но
    /// результат кэшируется навсегда: игра у монеты не меняется.
    ///
    /// Ошибку запроса НЕ кэшируем и трактуем как «хука нет»: иначе одна
    /// недоступность API вычистила бы весь вотчлист. Страховкой служит жёсткая
    /// проверка в `run_position` — до покупки хук перепроверяется.
    async fn hooks_of(&self, mints: &[Pubkey]) -> HashMap<Pubkey, Option<String>> {
        let mut out: HashMap<Pubkey, Option<String>> = HashMap::new();
        let mut need: Vec<Pubkey> = Vec::new();
        {
            let cache = self.hook_cache.lock().await;
            for m in mints {
                match cache.get(m) {
                    Some(h) => {
                        out.insert(*m, h.clone());
                    }
                    None => need.push(*m),
                }
            }
        }
        if need.is_empty() {
            return out;
        }
        let mut set: tokio::task::JoinSet<(Pubkey, Option<Option<String>>)> =
            tokio::task::JoinSet::new();
        for m in need {
            let otc = self.oracle.otc.clone();
            set.spawn(async move {
                let got = otc.coin_detail(&m.to_string()).await.ok().map(|c| {
                    c.hook
                        .map(|h| h.trim().to_string())
                        .filter(|h| !h.is_empty())
                });
                (m, got)
            });
        }
        let mut learned: Vec<(Pubkey, Option<String>)> = Vec::new();
        while let Some(r) = set.join_next().await {
            let Ok((m, got)) = r else { continue };
            match got {
                Some(h) => {
                    out.insert(m, h.clone());
                    learned.push((m, h));
                }
                None => {
                    out.insert(m, None); // запрос не прошёл — не кэшируем
                }
            }
        }
        if !learned.is_empty() {
            let mut cache = self.hook_cache.lock().await;
            for (m, h) in learned {
                cache.insert(m, h);
            }
        }
        out
    }

    /// Выбросить из ранжированного списка монеты с хуком и оставить `want`
    /// чистых. Идём сверху вниз пачками: хук стоит у единиц монет, но лежат
    /// они ровно в верхушке по поту, поэтому проверять весь список не нужно.
    async fn drop_hooked(&self, ranked: &mut Vec<(f64, Candidate)>, want: usize) -> usize {
        const CHUNK: usize = 48;
        let mut kept: Vec<(f64, Candidate)> = Vec::with_capacity(want);
        let mut dropped = 0usize;
        let mut shown = 0usize;
        let mut idx = 0usize;
        while kept.len() < want && idx < ranked.len() {
            let end = (idx + CHUNK).min(ranked.len());
            let slice: Vec<Pubkey> = ranked[idx..end].iter().map(|(_, c)| c.mint).collect();
            let hooks = self.hooks_of(&slice).await;
            for (pot, c) in ranked[idx..end].iter() {
                if kept.len() >= want {
                    break;
                }
                match hooks.get(&c.mint) {
                    Some(Some(h)) => {
                        dropped += 1;
                        if shown < 5 {
                            shown += 1;
                            info!(
                                "вотчлист: {}/{} с котлом {pot:.4} SOL выброшена — hook {h}, платит не по правилам раздачи",
                                c.symbol,
                                &c.mint.to_string()[..6]
                            );
                        }
                    }
                    _ => kept.push((*pot, c.clone())),
                }
            }
            idx = end;
        }
        *ranked = kept;
        dropped
    }

    /// ОСНОВНОЙ ЦИКЛ ОТБОРА: свежие страницы борды, часто.
    ///
    /// Заменяет полный обход борды как источник кандидатов. Почему — замер
    /// 04.11.2026 по 333 входам прибыльного бота danek:
    ///   • медианный возраст монеты на входе 5 минут (25% — 3, 75% — 8);
    ///   • капа $2.6-2.9k, то есть дно кривой; холдеров 1-9;
    ///   • котёл на входе медианой 0.0071 SOL, у 53% входов он ВООБЩЕ ПУСТ.
    /// То есть он не ищет жирный котёл — он приходит к наполнению первым и
    /// ждёт выплаты, пока монету торгуют. Ранжирование по текущему котлу (наш
    /// прежний обход) даёт ровно обратное: старые мёртвые монеты, чей котёл
    /// накопился и не выплачивается. Лучший котёл всей борды был 0.0400 SOL,
    /// а на свежих страницах прямо в тот же момент — 0.1696 SOL.
    pub async fn fresh_loop(self: Arc<Self>) {
        let every = std::time::Duration::from_secs(self.cfg.strategy.fresh_poll_secs.max(3));
        loop {
            let t0 = std::time::Instant::now();
            match self.sweep_fresh().await {
                Ok((seen, kept, best)) => {
                    self.pause_note(
                        "fresh",
                        &format!(
                            "свежие: {seen} монет за {:.1}с, в работе {kept}, лучший котёл {best:.4} SOL",
                            t0.elapsed().as_secs_f64()
                        ),
                    )
                    .await;
                }
                Err(e) => warn!("свежие страницы: {e:#}"),
            }
            tokio::time::sleep(every).await;
        }
    }

    /// Один проход по свежим страницам: кривые -> котлы -> вотчлист с приростом.
    pub async fn sweep_fresh(&self) -> Result<(usize, usize, f64)> {
        let rent = self.vault_rent().await;
        let cands = self
            .oracle
            .candidates_fresh(self.cfg.strategy.fresh_pages, 60)
            .await?;
        let seen = cands.len();

        // Возраст — главный фильтр, и он бесплатный: отсекаем до всякого RPC.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let max_age = self.cfg.strategy.max_coin_age_secs as i64;
        let cands: Vec<Candidate> = cands
            .into_iter()
            .filter(|c| match (max_age, c.created_at) {
                (0, _) => true,
                (_, Some(t)) => now - t <= max_age,
                (_, None) => false,
            })
            .collect();

        // Кривые батчами: creator + отсев выпустившихся.
        let mut creator_of: HashMap<Pubkey, Pubkey> = HashMap::new();
        let mut alive: Vec<Candidate> = Vec::with_capacity(cands.len());
        for chunk in cands.chunks(100) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|c| pumpfun::bonding_curve(&c.mint).to_string())
                .collect();
            let Ok(v) = self
                .rpc
                .raw("getMultipleAccounts", serde_json::json!([keys, {"encoding":"base64"}]))
                .await
            else {
                continue;
            };
            let vals = v["value"].as_array().cloned().unwrap_or_default();
            for (c, item) in chunk.iter().zip(vals.iter()) {
                let Some(b64) = item["data"][0].as_str() else { continue };
                let Ok(raw) = B64.decode(b64) else { continue };
                let Ok(cv) = pumpfun::parse_curve(&raw) else { continue };
                if cv.complete {
                    continue;
                }
                creator_of.insert(c.mint, cv.creator);
                alive.push(c.clone());
            }
        }
        let pots = self.read_pots(&creator_of, rent).await;

        // Прирост котла считаем относительно прошлого прохода: именно он, а не
        // сам котёл, говорит, что монету торгуют и выплата придёт за наш холд.
        let now_i = std::time::Instant::now();
        let prev: HashMap<Pubkey, (f64, f64, std::time::Instant)> =
            { self.pot_cache.lock().await.clone() };
        let mut watch: HashMap<Pubkey, Pubkey> = HashMap::new();
        let mut fresh_pots: HashMap<Pubkey, (f64, f64, std::time::Instant)> = HashMap::new();
        let mut ranked: Vec<(f64, Candidate)> = Vec::with_capacity(alive.len());
        for c in alive {
            let Some(cr) = creator_of.get(&c.mint) else { continue };
            let pot = pots.get(cr).copied().unwrap_or(0.0);
            let growth = match prev.get(&c.mint) {
                Some((p0, _, t0)) => {
                    let mins = now_i.duration_since(*t0).as_secs_f64() / 60.0;
                    if mins > 0.01 {
                        ((pot - p0) / mins).max(0.0)
                    } else {
                        0.0
                    }
                }
                None => 0.0,
            };
            watch.insert(c.mint, *cr);
            fresh_pots.insert(c.mint, (pot, growth, now_i));
            ranked.push((pot + growth, c));
        }
        ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        if self.cfg.strategy.exclude_hooked_coins {
            let want = ranked.len();
            self.drop_hooked(&mut ranked, want).await;
        }
        let best = ranked.first().map(|(p, _)| *p).unwrap_or(0.0);
        let kept = ranked.len();
        let board: Vec<Candidate> = ranked.into_iter().map(|(_, c)| c).collect();
        *self.board.lock().await = board;
        *self.watch_creators.lock().await = watch;
        *self.pot_cache.lock().await = fresh_pots;
        Ok((seen, kept, best))
    }

    pub async fn sweep_board(&self) -> Result<(usize, usize, f64)> {
        let rent = self.vault_rent().await;
        let cands = self
            .oracle
            .candidates_all(60, self.cfg.strategy.board_sweep_concurrency as usize)
            .await?;

        // 1. Кривые батчами: оттуда creator и признак выпуска. Выпустившиеся
        //    торгуются на pAMM и вне MVP — выкидываем сразу, чтобы они не
        //    занимали места в вотчлисте.
        let mut creator_of: HashMap<Pubkey, Pubkey> = HashMap::new();
        let mut alive: Vec<Candidate> = Vec::with_capacity(cands.len());
        for chunk in cands.chunks(100) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|c| pumpfun::bonding_curve(&c.mint).to_string())
                .collect();
            let Ok(v) = self
                .rpc
                .raw(
                    "getMultipleAccounts",
                    serde_json::json!([keys, {"encoding":"base64"}]),
                )
                .await
            else {
                continue;
            };
            let vals = v["value"].as_array().cloned().unwrap_or_default();
            for (c, item) in chunk.iter().zip(vals.iter()) {
                let Some(b64) = item["data"][0].as_str() else {
                    continue;
                };
                let Ok(raw) = B64.decode(b64) else { continue };
                let Ok(cv) = pumpfun::parse_curve(&raw) else {
                    continue;
                };
                if cv.complete {
                    continue;
                }
                creator_of.insert(c.mint, cv.creator);
                alive.push(c.clone());
            }
        }
        let on_curve = alive.len();

        // 2. Котлы батчами по создателям.
        let pots = self.read_pots(&creator_of, rent).await;

        // 3. Ранжируем по поту и оставляем вотчлист.
        let mut ranked: Vec<(f64, Candidate)> = alive
            .into_iter()
            .map(|c| {
                let pot = creator_of
                    .get(&c.mint)
                    .and_then(|cr| pots.get(cr))
                    .copied()
                    .unwrap_or(0.0);
                (pot, c)
            })
            .collect();
        ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        // 3б. ВЫБРАСЫВАЕМ МОНЕТЫ С ХУКОМ. Без этого верхушка вотчлиста состоит
        //     из них: котёл у хуковой монеты жирный ИМЕННО потому, что её игра
        //     не выплачивает (koth не короновал никого с 20.10.2026).
        let want = self.cfg.strategy.watchlist_size.max(1);
        if self.cfg.strategy.exclude_hooked_coins {
            let dropped = self.drop_hooked(&mut ranked, want).await;
            if dropped > 0 {
                info!("вотчлист: выброшено монет с хуком {dropped}");
            }
        }
        ranked.truncate(want);
        let best = ranked.first().map(|(p, _)| *p).unwrap_or(0.0);
        let kept = ranked.len();

        let now = std::time::Instant::now();
        let mut watch: HashMap<Pubkey, Pubkey> = HashMap::new();
        let mut fresh: HashMap<Pubkey, (f64, f64, std::time::Instant)> = HashMap::new();
        let mut board: Vec<Candidate> = Vec::with_capacity(kept);
        for (pot, c) in ranked {
            if let Some(cr) = creator_of.get(&c.mint) {
                watch.insert(c.mint, *cr);
            }
            fresh.insert(c.mint, (pot, 0.0, now));
            board.push(c);
        }
        *self.board.lock().await = board;
        *self.watch_creators.lock().await = watch;
        *self.pot_cache.lock().await = fresh;
        Ok((on_curve, kept, best))
    }

    /// Котлы по карте mint→creator, батчами по 100 аккаунтов.
    /// Котёл один НА СОЗДАТЕЛЯ, не на монету, поэтому дедуплицируем (на
    /// практике у создателя одна монета — проверено на всей борде).
    async fn read_pots(
        &self,
        creator_of: &HashMap<Pubkey, Pubkey>,
        rent: u64,
    ) -> HashMap<Pubkey, f64> {
        let creators: Vec<Pubkey> = creator_of
            .values()
            .copied()
            .collect::<HashSet<Pubkey>>()
            .into_iter()
            .collect();
        let mut pots: HashMap<Pubkey, f64> = HashMap::new();
        for chunk in creators.chunks(100) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|cr| pumpfun::creator_vault(cr).to_string())
                .collect();
            let Ok(v) = self
                .rpc
                .raw(
                    "getMultipleAccounts",
                    serde_json::json!([keys, {"encoding":"base64"}]),
                )
                .await
            else {
                continue;
            };
            let vals = v["value"].as_array().cloned().unwrap_or_default();
            for (cr, item) in chunk.iter().zip(vals.iter()) {
                let lam = item["lamports"].as_u64().unwrap_or(0);
                pots.insert(*cr, Self::pot_of(lam, rent));
            }
        }
        pots
    }

    /// БЫСТРЫЙ ПЕРЕЧЁТ КОТЛОВ ВОТЧЛИСТА. 600 монет = шесть батчей, то есть
    /// секунды, поэтому ходить можно часто — а именно это и решает тайминг:
    /// воркер клеймит раз в 4-5 минут, и войти надо в котёл, который уже дорос.
    ///
    /// Прежний цикл вместо этого опрашивал `/api/vault` по одной монете (851 мс
    /// каждая, 426 секунд на 500 монет) и мерил не пот, а процент от порога,
    /// которого не существует.
    pub async fn pot_refresh_loop(self: Arc<Self>) {
        let every = std::time::Duration::from_secs(self.cfg.strategy.pot_scan_interval_secs.max(5));
        loop {
            tokio::time::sleep(every).await;
            let map = self.watch_creators.lock().await.clone();
            if map.is_empty() {
                continue;
            }
            let started = std::time::Instant::now();
            let rent = self.vault_rent().await;
            let pots = self.read_pots(&map, rent).await;
            let prev = self.pot_cache.lock().await.clone();
            let now = std::time::Instant::now();
            let mut fresh: HashMap<Pubkey, (f64, f64, std::time::Instant)> = HashMap::new();
            for (mint, cr) in map.iter() {
                let Some(pot) = pots.get(cr).copied() else {
                    continue;
                };
                // Прирост котла в SOL/мин — признак живой торговли по монете.
                let rate = prev
                    .get(mint)
                    .map(|(old, _, t)| {
                        let mins = t.elapsed().as_secs_f64() / 60.0;
                        if mins > 0.05 {
                            (pot - old) / mins
                        } else {
                            0.0
                        }
                    })
                    .unwrap_or(0.0);
                fresh.insert(*mint, (pot, rate, now));
            }
            let best = fresh.values().map(|(p, _, _)| *p).fold(0.0f64, f64::max);
            let growing = fresh.values().filter(|(_, r, _)| *r > 1e-9).count();
            info!(
                "котлы вотчлиста: {} монет за {:.1}с, лучший {best:.4} SOL, растущих {growing}",
                fresh.len(),
                started.elapsed().as_secs_f64()
            );
            *self.pot_cache.lock().await = fresh;
        }
    }

    /// Один проход отбора: берём кандидатов, фильтруем, открываем позиции.
    pub async fn tick_entries(self: &Arc<Self>) -> Result<()> {
        // Потолок занят целиком → не дёргаем RPC впустую (при max=1 это значит
        // «позиция уже открыта»).
        if self.slots.available_permits() == 0 {
            return Ok(());
        }
        // Пул ATA ещё не прогрет → права на выплату у нас нет ни по одной
        // монете. Покупать в таком состоянии значит гарантированно заплатить
        // round-trip и не получить фис.
        if !self.ata_pool.warmed() {
            self.pause_note("ata", "пул ATA ещё не прогрет — входы на паузе")
                .await;
            return Ok(());
        }
        // КАНДИДАТЫ — ИЗ ВОТЧЛИСТА ПОЛНОГО ОБХОДА БОРДЫ. Пустой вотчлист
        // значит, что первый обход ещё не закончился. Откатываться на прежний
        // источник (страницы «свежих») НЕЛЬЗЯ: это ровно тот список, который
        // гарантированно не содержит цели.
        let mut cands = { self.board.lock().await.clone() };
        if cands.is_empty() {
            self.pause_note("board", "вотчлист борды ещё не собран — входы на паузе")
                .await;
            return Ok(());
        }
        // РАНЖИРОВАНИЕ ПО КОТЛУ. Цикл ниже берёт ПЕРВОГО прошедшего кандидата,
        // поэтому порядок списка и есть выбор позиции. Котёл здесь —
        // РЕАЛЬНЫЙ пот в SOL из чейна, а не процент от выдуманного порога:
        // заработок ограничен именно им (фис = пот × 0.675 × наша доля).
        // Прирост котла — вторичный ключ: он отличает живую торговлю от
        // стоячего остатка при равном поте.
        {
            let pots = self.pot_cache.lock().await;
            if !pots.is_empty() {
                cands.sort_by(|a, b| {
                    let ka = pots.get(&a.mint).map(|(p, r, _)| (*p, *r)).unwrap_or((-1.0, 0.0));
                    let kb = pots.get(&b.mint).map(|(p, r, _)| (*p, *r)).unwrap_or((-1.0, 0.0));
                    kb.0
                        .partial_cmp(&ka.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then(kb.1.partial_cmp(&ka.1).unwrap_or(std::cmp::Ordering::Equal))
                });
            }
        }
        // Обновляем карту кривая→mint по ВСЕМ кандидатам, чтобы шред-консьюмер мог
        // по событию раунда опознать монету и копить частоту раундов.
        {
            let mut c2m = self.curve_to_mint.lock().await;
            *c2m = cands
                .iter()
                .map(|c| (pumpfun::bonding_curve(&c.mint), c.mint))
                .collect();
        }
        let sol_price = self.sol_price_usd().await;
        let buy_sol = self.cfg.strategy.buy_sol;
        let min_rounds = self.cfg.strategy.min_rounds_in_window;
        let max_float = self.cfg.strategy.max_qualified_float_usd;
        let cooldown = self.cfg.strategy.post_trade_cooldown_secs;
        let mut considered = 0usize;
        // Разбор отсева по этапам. Без него бот при нулевом результате просто молчит,
        // и «работает, но ничего не подходит» не отличить от «сломан» — именно на
        // этом мы потеряли время на первом боевом запуске.
        let total = cands.len();
        // Верхушка по котлу — для разбора, почему именно она не прошла.
        let top5: Vec<Candidate> = cands.iter().take(5).cloned().collect();
        let (mut sk_active, mut sk_cooldown, mut sk_rounds, mut sk_notpl) = (0, 0, 0, 0);
        let mut sk_noata = 0usize;
        let (mut sk_pre1, mut sk_pre2, mut sk_float, mut sk_eval, mut sk_rmint) = (0, 0, 0, 0, 0);
        // Копим ПРИЧИНЫ отказа: номер этапа мало что говорит, тюнить можно только
        // по конкретному условию, которое режет.
        let mut reasons: HashMap<String, usize> = HashMap::new();
        let mut note = |r: &mut HashMap<String, usize>, s: String| {
            // Отрезаем числа из текста, чтобы причины группировались.
            let key = s.split(|ch: char| ch.is_ascii_digit()).collect::<Vec<_>>().join("");
            *r.entry(key.trim().to_string()).or_insert(0) += 1;
        };
        for mut c in cands {
            // Нет свободного слота — дальше перебирать незачем.
            if self.slots.available_permits() == 0 {
                break;
            }
            // Уже держим этот мемкоин.
            if self.active.lock().await.contains(&c.mint) {
                sk_active += 1;
                continue;
            }
            // Кулдаун на повторный вход в только что проторгованную монету.
            {
                let rtd = self.recent_trade.lock().await;
                if let Some(t) = rtd.get(&c.mint) {
                    if t.elapsed().as_secs() < cooldown {
                        sk_cooldown += 1;
                        continue;
                    }
                }
            }
            // ПРАВО НА ВЫПЛАТУ. Раздача не создаёт получателям ATA, поэтому
            // монета, под чей текущий reward-актив у нас аккаунта нет, бесполезна
            // при любой доле: фис уйдёт мимо. Проверка из памяти, поэтому стоит
            // первой — она снимает кандидата до единого обращения к RPC.
            if !self.ata_pool.is_ready(&c.reward_mint).await {
                sk_noata += 1;
                continue;
            }
            // ГЛАВНЫЙ гейт модели: монета должна АКТИВНО раундиться (иначе раунд не
            // придёт, пока держим — именно на этом бот сливал). Дёшево, из памяти.
            if self.rounds_in_window(&c.mint).await < min_rounds {
                sk_rounds += 1;
                continue;
            }
            // Недавно у этой монеты не нашли buy-шаблон — пропускаем на кулдаун,
            // чтобы перейти к другим кандидатам, а не застревать на ней.
            {
                let mut nt = self.no_template.lock().await;
                if let Some(t) = nt.get(&c.mint) {
                    if t.elapsed().as_secs() < Self::NO_TEMPLATE_COOLDOWN_SECS {
                        sk_notpl += 1;
                        continue;
                    }
                    nt.remove(&c.mint); // кулдаун истёк — дадим ещё шанс
                }
            }
            // Дешёвый прескрин без RPC (капа/объём/ликвидность) — до per-coin запроса.
            if let Verdict::Skip(r) = self.filters.prescreen(&c) {
                sk_pre1 += 1;
                note(&mut reasons, r);
                continue;
            }
            // Догружаем rewards (owed / distributed / lastPaidHolders) — объект приходит
            // ТОЛЬКО из /api/coins?mint=X. Это и есть сигналы отбора: платит ли монета
            // реально и на скольких холдеров делится выплата.
            if let Some((owed, distributed, paid_holders, payout_age)) =
                self.oracle.enrich_rewards(&c.mint).await
            {
                c.owed_sol = Some(owed);
                c.distributed_sol = Some(distributed);
                c.last_paid_holders = Some(paid_holders);
                c.last_payout_age_secs = payout_age;
            }
            // Прескрин ещё раз — теперь с rewards-гейтами (роздано / холдеры / обвал).
            if let Verdict::Skip(r) = self.filters.prescreen(&c) {
                sk_pre2 += 1;
                note(&mut reasons, r);
                continue;
            }
            // Реальный он-чейн флоат только для прошедших прескрин (экономим RPC).
            let float_override = self.float_override(&c, sol_price).await;
            // ДОЛЯ В ВЫПЛАТЕ — это и есть весь заработок. Реворд делится
            // пропорционально токенам, поэтому важен не абсолютный флоат, а
            // отношение нашего входа к чужому весу:
            //     доля = buy_usd / (free_float_usd + buy_usd)
            //
            // Прежний гейт `max_qualified_float_usd` был абсолютом и намерения
            // не выражал: один и тот же порог $400 означал 13% доли на входе
            // 0.3 SOL и 43% на 1.5 SOL. Теперь порог задан прямо в процентах и
            // сам масштабируется с размером позиции.
            //
            // Замер 27.10.2026 показывает, насколько это решает: на живых
            // кандидатах проектная доля гуляла от 9.4% до 100%. Наша убыточная
            // Fortress дала по факту 1.63% при трёх получателях.
            let buy_usd = buy_sol * sol_price;
            if let Some(f) = float_override {
                let share = if buy_usd + f > 0.0 {
                    100.0 * buy_usd / (f + buy_usd)
                } else {
                    0.0
                };
                if share < self.cfg.strategy.min_reward_share_pct {
                    sk_float += 1;
                    note(
                        &mut reasons,
                        format!("доля в выплате {share:.0}% < min (чужой флоат ${f:.0})"),
                    );
                    continue;
                }
                c.share_frac = Some(share / 100.0);
                c.float_usd = Some(f);
                info!(
                    "{}: флоат ${f:.0}, вход ${buy_usd:.0} → наша доля выплаты ~{share:.0}%",
                    c.mint
                );
            } else if self.cfg.strategy.require_float_measure {
                // Флоат не измерился (RPC) — входить вслепую нельзя: именно так
                // и берутся монеты, где чужой вес в десятки раз больше нашего.
                sk_float += 1;
                note(&mut reasons, "флоат не измерился".to_string());
                continue;
            }
            match self.filters.evaluate(&c, buy_sol, sol_price, float_override) {
                Verdict::Skip(r) => {
                    sk_eval += 1;
                    note(&mut reasons, r);
                    continue;
                }
                Verdict::Take => {}
            }
            // Не фармить два мемкоина на один и тот же reward-сток.
            {
                let mut rm = self.active_reward_mints.lock().await;
                if rm.contains(&c.reward_mint) {
                    sk_rmint += 1;
                    continue;
                }
                rm.insert(c.reward_mint);
            }
            // Жёсткий слот (атомарно). Не удалось — потолок занят: откатываем
            // резерв стока и прекращаем перебор.
            let permit = match Arc::clone(&self.slots).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    self.active_reward_mints.lock().await.remove(&c.reward_mint);
                    break;
                }
            };
            considered += 1;
            // Пометить активной и запустить позицию в отдельной задаче.
            self.active.lock().await.insert(c.mint);
            let me = Arc::clone(self);
            tokio::spawn(async move {
                // Пермит живёт до конца позиции → слот освобождается только после
                // полного закрытия (вход+выход+отчёт).
                let _permit = permit;
                let reward_mint = c.reward_mint;
                let mint = c.mint;
                if let Err(e) = me.run_position(c).await {
                    warn!("позиция {mint}: {e:#}");
                }
                // Кулдаун на повторный вход (не перезаходить сразу в ту же монету).
                me.recent_trade.lock().await.insert(mint, std::time::Instant::now());
                me.active_reward_mints.lock().await.remove(&reward_mint);
                me.active.lock().await.remove(&mint);
            });
        }
        if considered == 0 {
            // ЧТО В ВЕРХУШКЕ. При молчании бота первый вопрос — «почему не
            // взяли лучший котёл», и ответ должен быть в логе, а не в голове:
            // агрегат по 600 кандидатам его не даёт.
            let rows: Vec<(Pubkey, String, f64, f64, f64, i64, Pubkey)> = {
                let pots = self.pot_cache.lock().await;
                top5.iter()
                    .map(|c| {
                        (
                            c.mint,
                            c.symbol.clone(),
                            pots.get(&c.mint).map(|(p, _, _)| *p).unwrap_or(0.0),
                            c.volume_24h_usd.unwrap_or(-1.0),
                            c.market_cap_usd.unwrap_or(-1.0),
                            c.holders.map(|h| h as i64).unwrap_or(-1),
                            c.reward_mint,
                        )
                    })
                    .collect()
            };
            let mut notes = Vec::new();
            for (mint, sym, pot, vol, mcap, hold, rm) in rows {
                let ata = if self.ata_pool.is_ready(&rm).await {
                    "есть"
                } else {
                    "НЕТ"
                };
                notes.push(format!(
                    "{sym}/{} пот {pot:.4} vol ${vol:.0} капа ${mcap:.0} холдеров {hold} ATA {ata}",
                    &mint.to_string()[..6]
                ));
            }
            if !notes.is_empty() {
                self.pause_note("top5", &format!("верхушка по котлу: {}", notes.join(" | ")))
                    .await;
            }
            // Говорим ВСЛУХ, почему ни один кандидат не подошёл.
            self.pause_note(
                "sieve",
                &format!(
                    "tick: из {total} кандидатов не подошёл никто — отсев: \
                     держим {sk_active}, кулдаун {sk_cooldown}, \
                     нет ATA под актив {sk_noata}, нет раундов {sk_rounds}, \
                     нет шаблона {sk_notpl}, прескрин {sk_pre1}, \
                     реворды {sk_pre2}, флоат {sk_float}, EV {sk_eval}, \
                     сток занят {sk_rmint}"
                ),
            )
            .await;
            let mut why: Vec<_> = reasons.into_iter().collect();
            why.sort_by(|a, b| b.1.cmp(&a.1));
            let why: Vec<String> = why.iter().take(5).map(|(r, n)| format!("{r} ×{n}")).collect();
            if !why.is_empty() {
                self.pause_note("why", &format!("tick: что именно режет — {}", why.join(" | ")))
                    .await;
            }
        }
        if considered > 0 {
            info!("tick: открыто попыток входа {considered}");
        }
        Ok(())
    }

    /// Полный цикл одной позиции: вход → спам-выход.
    async fn run_position(&self, c: Candidate) -> Result<()> {
        let mint = c.mint;
        // ЖЁСТКИЙ ГЕЙТ ПО ХУКУ — последняя проверка перед деньгами.
        //
        // Вотчлист хуковые монеты уже не содержит, но между появлением монеты
        // на борде и ближайшим обходом есть окно, а цена ошибки здесь — целая
        // позиция, купленная по чужим правилам. Проверка идёт по кэшу, то есть
        // почти всегда без запроса.
        //
        // 04.11.2026 мы так и купили `jV7eSr…`: отбор увидел самый жирный
        // котёл борды (0.0936 SOL) и ноль конкурентов, а монета оказалась
        // `hook: "koth"` — там выигрывает не доля, а наибольшая ОДНА покупка в
        // окне 900с, и `hold_max_secs` сбросил бы позицию раньше конца раунда,
        // сам аннулировав приз.
        if self.cfg.strategy.exclude_hooked_coins {
            if let Some(Some(h)) = self.hooks_of(&[mint]).await.get(&mint) {
                info!(
                    "{mint}: hook {h} — другая механика выплаты, не наша игра (кулдаун {}s)",
                    Self::NO_TEMPLATE_COOLDOWN_SECS
                );
                self.no_template
                    .lock()
                    .await
                    .insert(mint, std::time::Instant::now());
                return Ok(());
            }
        }
        // Размер подбирается ниже, когда станут известны пот и чужой флоат.
        // `strategy.buy_sol` на него НЕ влияет: он остался только грубой
        // оценкой для прескрина доли в tick_entries, где пот ещё не прочитан.
        let buy_lamports: u64;
        // Баланс ДО сделки — основа честного PnL: разница с балансом после
        // закрытия включает вход, выход, все комиссии, tip и ренту ATA.
        let lamports_before = self.rpc.lamports(&self.payer.pubkey()).await.unwrap_or(0);

        // Читаем bonding curve (лоукап = ещё на кривой).
        let curve_acc = self.rpc.account(&pumpfun::bonding_curve(&mint)).await?;
        let curve = match &curve_acc {
            Some(a) => Some(pumpfun::parse_curve(&a.data)?),
            None => None,
        };
        if pumpswap::is_graduated(&curve) {
            // Выпустившиеся монеты торгуются на pAMM — вне MVP (нужен нативный
            // билдер pAMM или маршрут через Jupiter). Для лоукап-фарма пропускаем.
            info!("{mint}: уже выпустилась (pAMM) — пропуск в MVP (кулдаун {}s)",
                  Self::NO_TEMPLATE_COOLDOWN_SECS);
            self.no_template
                .lock()
                .await
                .insert(mint, std::time::Instant::now());
            return Ok(());
        }
        let curve = curve.unwrap();
        let creator = curve.creator;

        // Token-программа REWARD-СТОКА. Её НЕЛЬЗЯ хардкодить в Token-2022: стоки
        // бывают и обычные SPL Token (например ZEC). На этом бот терял каждую
        // такую монету — создание reward-ATA падало с IncorrectProgramId внутри
        // Token-2022 (GetAccountDataSize на чужом минте), и покупка не проходила.
        // Хуже того, от программы зависит АДРЕС ATA, то есть Lighthouse-гейт
        // сторожил бы несуществующий аккаунт и выход не сработал бы никогда.
        let reward_tp = match pumpfun::coin_token_program(&self.rpc, &c.reward_mint).await {
            Some(tp) => tp,
            None => {
                anyhow::bail!(
                    "не узнали token-программу reward-стока {} — вход отменён (догадка ломает адрес ATA и гейт выхода)",
                    c.reward_mint
                )
            }
        };

        // НАШ ATA под reward-сток. Гейт выхода смотрит именно на него:
        // «нам начислили сток» — безопасный сигнал, что раунд нас зачёл.
        let our_reward_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &self.payer.pubkey(),
            &c.reward_mint,
            &reward_tp,
        );
        let reward_baseline = self.rpc.token_amount(&our_reward_ata).await.unwrap_or(0);

        // ───── ВХОД ─────
        let me = self.payer.pubkey();
        // Token-программа САМОЙ МОНЕТЫ. Берём с минта и не угадываем: от неё
        // зависит адрес нашего ATA, ATA кривой и аккаунт token_program в
        // инструкции buy — ошибка здесь роняет покупку с IncorrectProgramId.
        let coin_tp = match pumpfun::coin_token_program(&self.rpc, &mint).await {
            Some(tp) => tp,
            None => anyhow::bail!("не узнали token-программу монеты {mint} — вход отменён"),
        };
        // ───── КОТЁЛ, РАЗМЕР И ГЕЙТ ВХОДА — ЦЕЛИКОМ ИЗ ЧЕЙНА ─────
        //
        // Витрина OTC здесь больше не решает ничего, и это измерено:
        // `floorSol` порогом выплаты не является (у большинства монет 0.002),
        // порога 0.10 SOL не существует вовсе — воркер клеймит котёл при
        // 0.003-0.009 SOL (медиана 0.0038, снято с истории creator-vault), а
        // `waitingSol` отстаёт от чейна. Прежний гейт «котёл заполнен ≥150% от
        // 0.10 SOL» требовал 0.15 SOL, тогда как ни одна монета из 67 714 на
        // борде не имела даже 0.10 (максимум по всей борде 0.0942): он
        // запрещал вход ВСЕГДА, при любой сортировке.
        //
        // Пот считаем сами: фисы создателя pump копятся на PDA
        // `creator-vault`, их и клеймит воркер OTC, чтобы раздать холдерам.
        // Рента аккаунта из пота ВЫЧИТАЕТСЯ — клейм её не забирает.
        let rent = self.vault_rent().await;
        let vault_pda = pumpfun::creator_vault(&creator);
        let vault_lamports = self.rpc.lamports(&vault_pda).await.unwrap_or(0);
        let pot_sol = Self::pot_of(vault_lamports, rent);
        info!(
            "{mint}: котёл {pot_sol:.6} SOL (на аккаунте {:.6}, рента {:.6} не забирается)",
            vault_lamports as f64 / LAMPORTS_PER_SOL,
            rent as f64 / LAMPORTS_PER_SOL
        );

        // ПОДБОР РАЗМЕРА ПОЗИЦИИ. Прибыль от позиции есть
        //     net(buy) = pot × 0.675 × buy/(float+buy) − cost × buy
        // где первое слагаемое насыщается, а второе растёт линейно. Максимум
        // там, где производная ноль:
        //     buy_opt = sqrt(pot × 0.675 × float / cost) − float
        let sol_usd = self.sol_price_usd().await;
        let float_sol = c.float_usd.unwrap_or(0.0) / sol_usd.max(1e-9);
        // Вырожденный случай: чужой флоат измерен НУЛЁМ (конкурентов с открытым
        // ATA нет). Тогда доля равна 100% при любом размере, издержки растут
        // линейно, и формальный оптимум — минимум. Но снимок флоата сделан НА
        // МОМЕНТ ВХОДА, а конкуренты открывают аккаунты своей же покупкой: к
        // раздаче их станет больше. Поэтому берём не минимум, а заданный
        // размер — запас на размытие дешевле потерянной доли.
        let buy_sol_dyn = if float_sol <= 1e-9 {
            self.cfg.strategy.default_buy_sol
        } else {
            let raw_opt = (pot_sol * Self::HOLDERS_CUT * float_sol / Self::ROUND_TRIP_COST)
                .max(0.0)
                .sqrt()
                - float_sol;
            raw_opt
                .max(self.cfg.strategy.min_buy_sol)
                .min(self.cfg.strategy.max_buy_sol)
        };
        let share_frac = if float_sol + buy_sol_dyn > 0.0 {
            buy_sol_dyn / (float_sol + buy_sol_dyn)
        } else {
            1.0
        };
        buy_lamports = (buy_sol_dyn * LAMPORTS_PER_SOL) as u64;
        // ОЖИДАЕМЫЙ ФИС СЧИТАЕМ НА МОМЕНТ ВЫПЛАТЫ, А НЕ НА МОМЕНТ ВХОДА.
        //
        // Гейт «котёл уже жирный» отбирал мёртвые монеты: у живой свежей котёл
        // на входе почти пуст и наполняется, пока мы держим. Разбор 333 входов
        // danek: медианный котёл на входе 0.0071 SOL, у 53% входов он ВООБЩЕ
        // ПУСТ, и деньги он всё равно делает — потому что приходит к
        // наполнению первым. Поэтому к текущему котлу добавляем то, что
        // набежит за холд по измеренному приросту (SOL/мин из sweep_fresh).
        let pot_growth = {
            self.pot_cache
                .lock()
                .await
                .get(&mint)
                .map(|(_, g, _)| *g)
                .unwrap_or(0.0)
        };
        // Горизонт — до ближайшей раздачи, а не весь холд: выход гейтуется
        // ПЕРВОЙ выплатой, и собираем мы котёл на её момент.
        let hold_min = self.cfg.strategy.pot_growth_horizon_secs as f64 / 60.0;
        let pot_at_payout = pot_sol + pot_growth * hold_min;
        let expected_reward_sol = pot_at_payout * Self::HOLDERS_CUT * share_frac;

        // ЦЕНА КРУГА. 2.6% от позиции — замер по 17 живым round-trip (две
        // комиссии pump по 1% плюс спред; средняя там −4.7%, берём лучшую как
        // базу).
        //
        // Пролив считаем по ОЖИДАЕМОЙ трате, а не по `exit_spam_max_sol`: это
        // жёсткий лимит (0.08 SOL), и подставлять его в цену круга значит
        // считать, что пролив всегда выжрет лимит. Так и вышло в первом драе:
        // круг «стоил» 0.119 SOL вместо 0.039, и лучший котёл борды давал
        // EV ×0.13 вместо ×1.7 — гейт снова запрещал всё, теперь по другой
        // причине. Падающая транзакция стоит base_fee + приоритет, а приоритет
        // на продаже у нас 1 µL/CU:
        //     5000 + 1 × 800 000 / 1e6 ≈ 5001 лампорт ≈ 0.000005 SOL
        // то есть минута пролива на 1 tx/s — это 0.0003 SOL, а не 0.08.
        let spam_budget = if self.cfg.strategy.exit_spam_tps > 0 {
            let per_attempt = (5_000.0
                + self.cfg.strategy.sell_cu_price as f64
                    * self.cfg.strategy.sell_cu_limit as f64
                    / 1e6)
                / LAMPORTS_PER_SOL;
            let attempts = self.cfg.strategy.exit_spam_tps as f64
                * self.cfg.strategy.exit_spam_window_secs as f64;
            (per_attempt * attempts).min(self.cfg.strategy.exit_spam_max_sol)
        } else {
            0.0
        };
        let round_trip = buy_sol_dyn * Self::ROUND_TRIP_COST + spam_budget;
        let ev_ratio = if round_trip > 1e-12 {
            expected_reward_sol / round_trip
        } else {
            f64::INFINITY
        };
        info!(
            "{mint}: РАЗМЕР {buy_sol_dyn:.3} SOL, чужой флоат {float_sol:.4} SOL → доля {:.0}%; \
             ФИС ожидаем {expected_reward_sol:.4} SOL (котёл {pot_sol:.4} + прирост {pot_growth:.4}/мин × {hold_min:.1}мин = {pot_at_payout:.4}, × {:.1}% холдерам × доля); \
             круг стоит {round_trip:.4} SOL → EV ×{ev_ratio:.2}",
            share_frac * 100.0,
            Self::HOLDERS_CUT * 100.0
        );

        // Порог ПЫЛИ: ниже этого реворд не продаётся вовсе и остаётся висеть
        // в ATA мёртвым грузом.
        if expected_reward_sol < self.cfg.strategy.min_expected_reward_sol {
            info!(
                "{mint}: фис {expected_reward_sol:.4} < порога пыли {:.4} SOL — скип",
                self.cfg.strategy.min_expected_reward_sol
            );
            return Ok(());
        }
        // ГЛАВНЫЙ ГЕЙТ: фис обязан перекрывать круг с запасом. В логе сразу
        // печатаем, какой котёл для этого нужен, чтобы не считать в голове.
        if ev_ratio < self.cfg.strategy.min_ev_ratio {
            let pot_needed = self.cfg.strategy.min_ev_ratio * round_trip
                / (Self::HOLDERS_CUT * share_frac.max(1e-9));
            info!(
                "{mint}: EV ×{ev_ratio:.2} < min ×{:.2} — круг не окупится, скип \
                 (при доле {:.0}% нужен котёл ≥{pot_needed:.4} SOL)",
                self.cfg.strategy.min_ev_ratio,
                share_frac * 100.0
            );
            return Ok(());
        }
        info!("{mint}: ВХОДИМ — котёл {pot_sol:.4} SOL, EV ×{ev_ratio:.2}");

        // Защита от проскальзывания считается от ИТОГОВОГО размера. Раньше она
        // считалась ДО подбора размера: при расхождении больше slippage_bps
        // покупка падала бы на min_tokens, а при меньшем размере защиты не было
        // вовсе.
        let tokens_out = curve.tokens_out_for_sol(buy_lamports);
        let min_tokens = tokens_out - tokens_out * self.cfg.strategy.slippage_bps / 10_000;

        // coin_fee_A/B из свежей реальной buy/sell монеты (остальное деривируем).
        // Кэшируем per-coin: нет свежих buy/sell → монета не торгуется, фисов не
        // будет, скип с кулдауном.
        let cached = self.coin_fees.lock().await.get(&mint).copied();
        let fees = match cached {
            Some(f) => f,
            None => match pumpfun::fetch_coin_fees(&self.rpc, &mint).await {
                Ok(f) => {
                    self.coin_fees.lock().await.insert(mint, f);
                    f
                }
                Err(e) => {
                    // Rate-limit RPC — это «спроси позже», а НЕ «шаблона нет».
                    // Раньше 429 уводил вполне живую монету в кулдаун на 180
                    // секунд, и мы теряли ровно тех кандидатов, до которых
                    // дошла очередь в момент нагрузки.
                    let msg = format!("{e:#}");
                    if msg.contains("429") || msg.contains("Too Many") {
                        warn!("{mint}: RPC занят ({e}) — попробуем позже, БЕЗ кулдауна");
                        return Ok(());
                    }
                    info!("{mint}: нет coin_fee ({e}) — пропуск позиции (кулдаун {}s)", Self::NO_TEMPLATE_COOLDOWN_SECS);
                    self.no_template
                        .lock()
                        .await
                        .insert(mint, std::time::Instant::now());
                    return Ok(());
                }
            },
        };
        // ATA мемкоина — под своей token-программой. ATA reward-актива тут
        // БОЛЬШЕ НЕ СОЗДАЁТСЯ: его держит постоянно открытым пул (ata_warm_loop),
        // и гейт входа не пропускает монету, под чей актив аккаунта нет. Это
        // убирает из горячего пути и транзакцию, и четыре аккаунта в самой
        // покупке — тем легче ей приземлиться в гонке.
        let coin_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &me, &me, &mint, &coin_tp,
        );
        // BUY полностью из деривации (18 аккаунтов) + coin_fee. Сверено байт-в-байт.
        let buy_ix = pumpfun::build_buy_exact(&me, &mint, &creator, &coin_tp, &fees, buy_lamports, min_tokens);

        // Гейт входа: «мы ещё не держим эту монету» → из N веерных копий купит одна.
        let coin_ata = pumpfun::ata_with(&me, &mint, &coin_tp);
        let mut ixs: Vec<Instruction> = Vec::new();
        if self.cfg.strategy.entry_gate {
            let exists = matches!(self.rpc.account(&coin_ata).await, Ok(Some(_)));
            let gate = if exists {
                assert_token_amount(&coin_ata, 0, Op::Eq)
            } else {
                assert_account_lamports(&coin_ata, 0, Op::Eq)
            };
            ixs.push(gate);
        }
        // Tip добавляет relay под конкретный эндпоинт.
        ixs.extend(compute_budget(self.cfg.strategy.buy_cu_limit, self.cfg.strategy.buy_cu_price));
        ixs.extend([coin_ata_ix, buy_ix]);

        if self.cfg.strategy.dry_run {
            info!(
                "[DRY] BUY {mint} sym={} ~{} токенов за {:.4} SOL (min_out {}); reward-ATA {} baseline={}",
                c.symbol,
                tokens_out,
                buy_lamports as f64 / LAMPORTS_PER_SOL,
                min_tokens,
                our_reward_ata,
                reward_baseline,
            );
        } else {
            let bh = self.rpc.latest_blockhash().await?;
            // С гейтом входа можно безопасно веерить (исполнится одна копия).
            // Без гейта — строго один эндпоинт (иначе двойная покупка).
            let sig = if self.cfg.strategy.fanout_entry && self.cfg.strategy.entry_gate {
                self.relays
                    .broadcast_build(&self.payer, &ixs, bh, &no_alts(), &[])
                    .await?
            } else {
                self.relays
                    .submit_primary(&self.payer, &ixs, bh, &no_alts(), &[])
                    .await?
            };
            info!("BUY {mint} отправлен: {sig}");
            // Реально вошли → маркер «в позиции»: деплой-скрипт не рестартит
            // бота, пока мы держим монету (иначе осиротит спам-выход). В dry
            // сюда не попадаем, поэтому dry-рестарты всегда свободны.
            self.set_busy(true);
            // Филл дожидается сам run_exit (ретраями), чтобы медленный вход
            // не осиротил позицию без управляющего цикла.
        }

        // ───── ВЫХОД (спам с гейтом, триггерится шредами) ─────
        // Будим выход и по нашему ATA стока (нам начислили), и по минту монеты
        // (её раунд в шред-потоке). Гейт на НАШ ATA — корректность.
        let notify = Arc::new(Notify::new());
        self.register_exit(&our_reward_ata, &notify).await;
        self.register_exit(&mint, &notify).await;
        let r = self
            .run_exit(&c, coin_tp, creator, &fees, reward_baseline, &our_reward_ata, &notify)
            .await;
        self.deregister_exit(&our_reward_ata, &notify).await;
        self.deregister_exit(&mint, &notify).await;
        // Позиция закрыта/сдались → снимаем маркер (no-op в dry, где не ставили).
        self.set_busy(false);
        let (closed, attempts, sell_amount) = r?;
        if closed && !self.cfg.strategy.dry_run {
            self.emit_report(&c, &our_reward_ata, reward_baseline, lamports_before, attempts, expected_reward_sol)
                .await;
        }
        Ok(())
    }

    /// Собрать подробный PnL по закрытой сделке и отдать репортеру (TG+журнал).
    async fn emit_report(
        &self,
        c: &Candidate,
        reward_ata: &Pubkey,
        reward_baseline: u64,
        lamports_before: u64,
        attempts: u64,
        expected_reward_sol: f64,
    ) {
        let sol_price = self.sol_price_usd().await;
        // ФАКТИЧЕСКАЯ дельта кошелька за сделку, а не расчёт по кривой.
        //
        // Раньше выручка бралась как curve.sol_out_for_tokens(sell_amount) — то
        // есть из ФОРМУЛЫ по состоянию кривой, прочитанному ДО сделки. Это не
        // то, что реально пришло: ни проскальзывания, ни фактической цены
        // исполнения, ни комиссий там нет. Теперь считаем по балансу: он
        // включает всё разом — вход, выход, комиссии, ренту ATA, tip.
        let lamports_after = self.rpc.lamports(&self.payer.pubkey()).await.unwrap_or(lamports_before);
        let wallet_delta_sol =
            (lamports_after as f64 - lamports_before as f64) / LAMPORTS_PER_SOL;

        // Сколько стока начислил раунд (raw-единицы) и его оценка в SOL (котировка).
        let reward_now = self.rpc.token_amount(reward_ata).await.unwrap_or(reward_baseline);
        let reward_raw = reward_now.saturating_sub(reward_baseline);
        let reward_value_sol = self.reward_value_sol(&c.reward_mint, reward_raw).await;

        // Разбивка расходов. tip релеев пока не трекаем per-tx (ground truth — сводка
        // по балансу); спам-кост — верхняя оценка (приземлившиеся-и-упавшие × base).
        let s = &self.cfg.strategy;
        let priority = s.buy_cu_price * s.buy_cu_limit as u64 / 1_000_000
            + s.sell_cu_price * s.sell_cu_limit as u64 / 1_000_000;
        let fees = Fees {
            base_lamports: 5000 * 2,
            priority_lamports: priority,
            // tip платится только за ПРИЗЕМЛИВШУЮСЯ tx: вход и выход.
            tips_lamports: 5_000 * 2,
            spam_attempts: attempts,
            spam_cost_lamports: attempts.saturating_mul(5000),
            ata_rent_lamports: 2 * 2_039_280,
        };
        let buy_sol = s.buy_sol;
        // Дельта кошелька уже содержит вход, выход и ВСЕ комиссии. Реворд ещё не
        // продан (его сбрасывает sell_rewards позже), поэтому добавляем его
        // котировку — иначе каждая прибыльная сделка выглядела бы убыточной.
        let net_sol = wallet_delta_sol + reward_value_sol;
        // Нога по монете = дельта минус комиссии: показываем отдельно, чтобы
        // было видно, сколько съела сама монета, а сколько комиссии.
        let coin_sell_sol = buy_sol + wallet_delta_sol
            + fees.total_nonrefundable() as f64 / LAMPORTS_PER_SOL;

        let rep = TradeReport {
            mint: c.mint.to_string(),
            symbol: c.symbol.clone(),
            reward_symbol: c.reward_mint.to_string().chars().take(6).collect(),
            buy_sol,
            coin_sell_sol,
            reward_amount: reward_raw as f64 / 1e6,
            reward_value_sol,
            fees,
            net_sol,
            net_usd: net_sol * sol_price,
            closed_at: now_unix(),
        };
        // СВЕРКА ПРОГНОЗА С ФАКТОМ. Формула фиса (пот creator-vault × 0.675 ×
        // наша доля) считается до входа, и только это сравнение показывает,
        // работает ли она на самом деле.
        if expected_reward_sol > 0.0 {
            let ratio = 100.0 * reward_value_sol / expected_reward_sol;
            info!(
                "СВЕРКА {}: ожидали фис {expected_reward_sol:.4} SOL, получили {reward_value_sol:.4} SOL ({ratio:.0}% от прогноза)",
                &c.symbol
            );
        }
        self.reporter.report_trade(&rep).await;
    }

    /// Строит и спамит выход, пока не исполнится или не выйдет лимит слотов.
    /// `notify` дёргается шред-потоком в момент раунда → шлём немедленно.
    async fn run_exit(
        &self,
        c: &Candidate,
        coin_tp: Pubkey,
        creator: Pubkey,
        fees: &pumpfun::CoinFees,
        reward_baseline: u64,
        reward_ata: &Pubkey,
        notify: &Notify,
    ) -> Result<(bool, u64, u64)> {
        let mint = c.mint;
        let me = self.payer.pubkey();
        let user_ata = pumpfun::ata_with(&me, &mint, &coin_tp);

        // Дожидаемся филла входа ретраями (вход мог приземлиться не мгновенно).
        let bal = if self.cfg.strategy.dry_run {
            0
        } else {
            let mut b = 0u64;
            for _ in 0..25 {
                b = self.rpc.token_amount(&user_ata).await.unwrap_or(0);
                if b > 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            b
        };
        if !self.cfg.strategy.dry_run && bal == 0 {
            // Нечего продавать — вход не подтвердился. НЕ спамим пустой выход
            // (иначе сожгли бы durable nonce, продав ноль).
            return Err(anyhow!("вход {mint} не подтвердился за ~7.5s — позиция не открыта"));
        }
        let sell_amount = ((bal as f64) * self.cfg.strategy.sell_fraction) as u64;
        // ЦЕНОВОЙ ПОЛ (как у работающего бота): НЕ продаём дешевле стоимости входа
        // + таргет. Транзакция просто упадёт (pump error 6023) и будет стоить
        // базовую комиссию — зато мы не фиксируем убыток. Дамп «по любой цене»
        // (min_sol_out=0) — ровно то, на чём мы сливали.
        let cost_lamports = (self.cfg.strategy.buy_sol * LAMPORTS_PER_SOL) as u64;
        let min_sol_output = cost_lamports
            .saturating_mul(self.cfg.strategy.sell_floor_bps)
            / 10_000;
        let threshold = reward_baseline.saturating_add(self.cfg.strategy.exit_gate_epsilon);
        let gate = self.cfg.strategy.lighthouse_exit_gate;

        let poll = std::time::Duration::from_millis(self.cfg.strategy.poll_ms);

        // sell из деривации (16 аккаунтов) + coin_fee. В dry sell_amount=0 → сеть не трогаем.
        let sell_ix = if sell_amount > 0 {
            Some(pumpfun::build_sell_derived(&me, &mint, &creator, &coin_tp, fees, sell_amount, min_sol_output))
        } else {
            None
        };

        if self.cfg.strategy.dry_run {
            info!(
                "[DRY] HOLD {mint}: держим до раунда (≤{}s), гейт reward(ourReward {reward_ata})≥{threshold}, продать {sell_amount} токенов",
                self.cfg.strategy.hold_max_secs
            );
            return Ok((false, 0, sell_amount));
        }

        // ── ГОНКА ВЫХОДА (пре-подписанный гейтованный sell + непрерывный пролив) ──
        // Реверс работающего бота: конкуренты льют 300-500 падающих продаж/сек,
        // чтобы исполниться В ПЕРВОМ СЛОТЕ после раздачи. Опоздал на 2 секунды —
        // продал в −21% вместо −1.4%. Поэтому НЕ «ждём и шлём одну», а держим
        // заранее подписанную гейтованную продажу и непрерывно её льём: пока
        // реворда нет, гейт роняет её за base fee (tip при этом НЕ списывается).
        if self.cfg.strategy.exit_spam_tps > 0 {
            if let Some(ix) = sell_ix.clone() {
                let (sold, tries, exhausted) = self
                    .spam_gated_exit(&mint, &user_ata, reward_ata, threshold, bal, ix, notify)
                    .await;
                if sold {
                    info!("EXIT {mint}: ПРОДАНО В ГОНКЕ (попыток {tries})");
                    return Ok((true, tries, sell_amount));
                }
                // Бюджет/окно пролива кончились, но холд ещё идёт — НЕ бейлим в
                // минус: проваливаемся в дешёвый опрос ниже и ждём реворд даром.
                if !exhausted {
                    warn!(
                        "EXIT {mint}: гонка не дала результата за {}с (попыток {tries}) → бейл",
                        self.cfg.strategy.hold_max_secs
                    );
                    // Раздачи не случилось за весь холд — выходим по слиппедж-полу.
                    let floor =
                        cost_lamports.saturating_mul(self.cfg.strategy.bail_floor_bps) / 10_000;
                    let bail = pumpfun::build_sell_derived(
                        &me, &mint, &creator, &coin_tp, fees, sell_amount, floor,
                    );
                    for _ in 0..10 {
                        let mut ixs: Vec<Instruction> = compute_budget(
                            self.cfg.strategy.sell_cu_limit,
                            self.cfg.strategy.sell_cu_price,
                        )
                        .to_vec();
                        ixs.push(bail.clone());
                        if let Ok(bh) = self.rpc.latest_blockhash().await {
                            let _ = self
                                .relays
                                .broadcast_build(&self.payer, &ixs, bh, &no_alts(), &[])
                                .await;
                        }
                        if let Ok(now) = self.rpc.token_amount(&user_ata).await {
                            if now == 0 || now < bal / 2 {
                                info!("EXIT {mint}: закрыто бейлом (остаток {now})");
                                return Ok((true, tries, sell_amount));
                            }
                        }
                        tokio::time::sleep(poll).await;
                    }
                    warn!("EXIT {mint}: бейл не прошёл — позиция висит, разбирайся вручную");
                    return Ok((false, tries, sell_amount));
                }
            }
        }

        // ── ХОЛД ДО РАУНДА (запасной путь, если пролив выключен) ──
        // Раунды OTC атомарны (своп+раздача в одной tx) — влезть в гэп нельзя, надо
        // быть холдером ДО раунда. Поэтому ДЕРЖИМ монету и ждём, пока раунд начислит
        // сток на наш reward-ATA (shred-будилка + дешёвый опрос). Как начислил —
        // продаём (реворд уже у нас). Истёк бюджет холда → бейл по рынку.
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(self.cfg.strategy.hold_max_secs);
        let mut attempts = 0u64;
        let mut got_reward = false;
        let mut rewards_collected = 0u32;
        // Порог «следующего» реворда: после каждого полученного сдвигаем выше,
        // чтобы ждать именно НОВЫЙ раунд, а не видеть старый баланс.
        let mut next_threshold = threshold;
        // Выход сработал по защите прибыли (профит утёк обратно) → продаём в безубыток.
        let mut breakeven_stop = false;
        let mut polls = 0u32;
        'hold: loop {
            let mut hit = false;
            while std::time::Instant::now() < deadline {
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
                // ЗАЩИТА ПРИБЫЛИ: реворд уже взят и мы держим ради следующего —
                // следим, чтобы дамп конкурентов (после раздачи) не съел плюс.
                if rewards_collected > 0 {
                    polls += 1;
                    if polls % self.cfg.strategy.profit_check_every_polls.max(1) == 0 {
                        if let Some(p) = self
                            .position_profit_pct(&mint, &c.reward_mint, sell_amount, reward_ata, reward_baseline)
                            .await
                        {
                            if p <= self.cfg.strategy.breakeven_exit_pct {
                                warn!(
                                    "EXIT {mint}: профит просел до {p:.1}% (≤{:.1}%) → выходим в безубыток, не отдаём прибыль",
                                    self.cfg.strategy.breakeven_exit_pct
                                );
                                breakeven_stop = true;
                                break;
                            }
                        }
                    }
                }
                let reward_now = self.rpc.token_amount(reward_ata).await.unwrap_or(reward_baseline);
                if reward_now >= next_threshold {
                    got_reward = true;
                    rewards_collected += 1;
                    hit = true;
                    info!(
                        "EXIT {mint}: реворд #{rewards_collected} начислен ({reward_now}≥{next_threshold})"
                    );
                    next_threshold = reward_now.saturating_add(self.cfg.strategy.exit_gate_epsilon);
                    break;
                }
            }
            if breakeven_stop {
                break 'hold; // прибыль утекает — выходим немедленно
            }
            if !hit {
                break 'hold; // дедлайн без реворда
            }
            // Риск отбит ревордом: если позиция уже в хорошем плюсе — НЕ продаём,
            // ждём следующий раунд (ещё реворд + возможный рост монеты).
            if rewards_collected < self.cfg.strategy.max_rewards_per_hold
                && std::time::Instant::now() < deadline
            {
                if let Some(p) = self
                    .position_profit_pct(&mint, &c.reward_mint, sell_amount, reward_ata, reward_baseline)
                    .await
                {
                    if p >= self.cfg.strategy.hold_more_profit_pct {
                        info!(
                            "EXIT {mint}: профит {p:.1}% ≥ {:.1}% → ДЕРЖИМ дальше до следующего раунда (реворд {rewards_collected}/{})",
                            self.cfg.strategy.hold_more_profit_pct,
                            self.cfg.strategy.max_rewards_per_hold
                        );
                        continue 'hold;
                    }
                    info!("EXIT {mint}: профит {p:.1}% < порога → продаём сейчас");
                }
            }
            break 'hold;
        }

        // Реворд пришёл → gated sell (гейт пройдёт, он уже у нас). Бюджет истёк без
        // реворда → бейл по рынку (без гейта), если включено.
        let use_gate = got_reward && gate;
        if !got_reward {
            if !self.cfg.strategy.exit_bail_on_timeout {
                warn!("EXIT {mint}: за {}s раунд не пришёл, bail выключен — позиция висит, разбирайся вручную", self.cfg.strategy.hold_max_secs);
                return Ok((false, attempts, sell_amount));
            }
            warn!("EXIT {mint}: за {}s раунд не пришёл → продаём по рынку (бейл, в убыток)", self.cfg.strategy.hold_max_secs);
        }
        // Пол продажи зависит от причины выхода:
        //  • защита прибыли  → ровно безубыток (иначе продажа не пройдёт и плюс утечёт);
        //  • реворд получен  → обычный пол с таргетом;
        //  • таймаут без реворда → аварийный пол (ограниченный минус), но не ноль.
        let floor_bps = if breakeven_stop {
            self.cfg.strategy.breakeven_floor_bps
        } else if got_reward {
            self.cfg.strategy.sell_floor_bps
        } else {
            self.cfg.strategy.bail_floor_bps
        };
        let final_floor = cost_lamports.saturating_mul(floor_bps) / 10_000;
        let bail_ix = if sell_amount > 0 && floor_bps != self.cfg.strategy.sell_floor_bps {
            Some(pumpfun::build_sell_derived(
                &me, &mint, &creator, &coin_tp, fees, sell_amount, final_floor,
            ))
        } else {
            None
        };
        info!(
            "EXIT {mint}: продаём с полом {:.4} SOL ({} bps от затрат), реворд={got_reward}",
            final_floor as f64 / LAMPORTS_PER_SOL,
            floor_bps
        );
        for _ in 0..15 {
            let mut ixs: Vec<Instruction> =
                compute_budget(self.cfg.strategy.sell_cu_limit, self.cfg.strategy.sell_cu_price).to_vec();
            if use_gate {
                ixs.push(assert_token_amount(reward_ata, threshold, Op::Gte));
            }
            if let Some(ix) = bail_ix.as_ref().or(sell_ix.as_ref()) {
                ixs.push(ix.clone());
            }
            if let Ok(bh) = self.rpc.latest_blockhash().await {
                if let Err(e) = self
                    .relays
                    .broadcast_build(&self.payer, &ixs, bh, &no_alts(), &[])
                    .await
                {
                    warn!("EXIT {mint} продажа: {e}");
                }
            }
            attempts += 1;
            if let Ok(now) = self.rpc.token_amount(&user_ata).await {
                if now == 0 || now < bal / 2 {
                    info!("EXIT {mint}: монета продана (остаток {now}, реворд={got_reward})");
                    return Ok((true, attempts, sell_amount));
                }
            }
            tokio::time::sleep(poll).await;
        }
        warn!("EXIT {mint}: продажа не прошла за 15 попыток — позиция висит, разбирайся вручную");
        Ok((false, attempts, sell_amount))
    }

    /// РАЗОВАЯ ПОКУПКА на заданную сумму: без отбора, без гейтов, без выхода.
    ///
    /// Нужна для хука `koth` («King of the Hill»), который живёт по другим
    /// правилам, чем раздача холдерам: раунд там выигрывает НАИБОЛЬШАЯ ОДНА
    /// покупка в окне 900 секунд, победитель обязан додержать ≥90% купленного
    /// (`holdRatio`), и забирает он 67.5% котла целиком, а не долю. Замер
    /// 04.11.2026: закрывает раунд не таймер, а СЛЕДУЮЩАЯ СДЕЛКА — на
    /// `87aLG1z2…` клейм котла прошёл в ту же секунду, что и `kingAt`, а наше
    /// собственное окно (18:20:33 → 18:35:33, единственный лидер, 100%
    /// удержано) истекло без коронации, потому что после нас никто не купил.
    ///
    /// Торговый цикл такую покупку сделать НЕ МОЖЕТ и не должен: `entry_gate`
    /// намеренно требует пустой ATA, чтобы веерные копии входа не купили
    /// дважды. Здесь гейта нет именно потому, что монету мы уже держим, —
    /// отсюда отдельная команда, а не флаг в стратегии.
    pub async fn buy_once(&self, mint: &Pubkey, sol: f64, cu_price: u64) -> Result<()> {
        let me = self.payer.pubkey();
        let buy_lamports = (sol * LAMPORTS_PER_SOL) as u64;
        anyhow::ensure!(buy_lamports > 0, "сумма покупки должна быть больше нуля");

        let acc = self
            .rpc
            .account(&pumpfun::bonding_curve(mint))
            .await?
            .ok_or_else(|| anyhow!("нет аккаунта кривой у {mint}"))?;
        let curve = pumpfun::parse_curve(&acc.data)?;
        anyhow::ensure!(
            !curve.complete,
            "{mint} выпустилась с кривой — разовая покупка собрана под кривую, не под пул"
        );
        let creator = curve.creator;

        let coin_tp = match pumpfun::coin_token_program(&self.rpc, mint).await {
            Some(tp) => tp,
            None => anyhow::bail!("не узнали token-программу монеты {mint} — покупка отменена"),
        };
        let fees = pumpfun::fetch_coin_fees(&self.rpc, mint).await?;

        let tokens_out = curve.tokens_out_for_sol(buy_lamports);
        let min_tokens = tokens_out - tokens_out * self.cfg.strategy.slippage_bps / 10_000;

        let mut ixs: Vec<Instruction> = Vec::new();
        // Приоритет задаётся аргументом, а не `buy_cu_price`: торговый вход
        // платит 0.0006 SOL за место в гонке, а здесь гонки нет — на мёртвой
        // монете мы единственный покупатель, и переплата была бы процентом
        // от самой покупки.
        ixs.extend(compute_budget(self.cfg.strategy.buy_cu_limit, cu_price));
        ixs.push(
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &me, &me, mint, &coin_tp,
            ),
        );
        ixs.push(pumpfun::build_buy_exact(
            &me,
            mint,
            &creator,
            &coin_tp,
            &fees,
            buy_lamports,
            min_tokens,
        ));

        if self.cfg.strategy.dry_run {
            info!("[DRY] BUY-ONCE {mint}: {sol:.4} SOL → ~{tokens_out} токенов (min_out {min_tokens})");
            return Ok(());
        }
        let bh = self.rpc.latest_blockhash().await?;
        let sig = self
            .relays
            .submit_primary(&self.payer, &ixs, bh, &no_alts(), &[])
            .await?;
        info!("BUY-ONCE {mint}: {sol:.4} SOL отправлено (min_out {min_tokens}), tx {sig}");
        Ok(())
    }

    /// РАЗОВАЯ ПРОДАЖА всей позиции по монете. Пара к `buy_once`.
    ///
    /// Торговый цикл продаёт только то, во что сам вошёл в этом запуске: после
    /// остановки бота позиция живёт на кошельке, а управляющего цикла у неё
    /// больше нет. Восстановление позиции с кошелька не сделано (и не нужно
    /// для разового случая), поэтому выход даётся отдельной командой.
    ///
    /// Пол продажи берём из `bail_floor_bps` относительно переданной цены
    /// входа: без пола `min_sol_out = 0` и есть тот самый дамп в ноль, на
    /// котором бот терял деньги раньше.
    pub async fn sell_once(&self, mint: &Pubkey, cost_sol: f64, cu_price: u64) -> Result<()> {
        let me = self.payer.pubkey();
        let acc = self
            .rpc
            .account(&pumpfun::bonding_curve(mint))
            .await?
            .ok_or_else(|| anyhow!("нет аккаунта кривой у {mint}"))?;
        let curve = pumpfun::parse_curve(&acc.data)?;
        anyhow::ensure!(!curve.complete, "{mint} выпустилась с кривой — эта продажа только под кривую");
        let creator = curve.creator;

        let coin_tp = match pumpfun::coin_token_program(&self.rpc, mint).await {
            Some(tp) => tp,
            None => anyhow::bail!("не узнали token-программу монеты {mint} — продажа отменена"),
        };
        let ata = pumpfun::ata_with(&me, mint, &coin_tp);
        let amount = self.rpc.token_amount(&ata).await.unwrap_or(0);
        anyhow::ensure!(amount > 0, "на {ata} нет токенов — продавать нечего");

        let spot = curve.sol_out_for_tokens(amount);
        let floor = if cost_sol > 0.0 {
            ((cost_sol * LAMPORTS_PER_SOL) as u128 * self.cfg.strategy.bail_floor_bps as u128 / 10_000) as u64
        } else {
            0
        };
        info!(
            "SELL-ONCE {mint}: {amount} токенов, по кривой ~{:.4} SOL, пол {:.4} SOL",
            spot as f64 / LAMPORTS_PER_SOL,
            floor as f64 / LAMPORTS_PER_SOL
        );
        anyhow::ensure!(
            spot >= floor,
            "по кривой дают {:.4} SOL, это ниже пола {:.4} — продажа отменена",
            spot as f64 / LAMPORTS_PER_SOL,
            floor as f64 / LAMPORTS_PER_SOL
        );

        let fees = pumpfun::fetch_coin_fees(&self.rpc, mint).await?;
        let mut ixs: Vec<Instruction> = Vec::new();
        ixs.extend(compute_budget(self.cfg.strategy.sell_cu_limit, cu_price));
        ixs.push(pumpfun::build_sell_derived(
            &me, mint, &creator, &coin_tp, &fees, amount, floor,
        ));

        if self.cfg.strategy.dry_run {
            info!("[DRY] SELL-ONCE {mint}: продал бы {amount} токенов, min_out {floor}");
            return Ok(());
        }
        let bh = self.rpc.latest_blockhash().await?;
        let sig = self
            .relays
            .submit_primary(&self.payer, &ixs, bh, &no_alts(), &[])
            .await?;
        info!("SELL-ONCE {mint}: отправлено, tx {sig}");
        Ok(())
    }

    /// Сколько SOL стоит начисленный реворд.
    ///
    /// Для WSOL котировка не нужна и ВРЕДНА: Jupiter на паре WSOL→WSOL отвечает
    /// ошибкой, `unwrap_or(0.0)` обнулял бы реворд, и прибыльная сделка
    /// выглядела бы убыточной — а `breakeven_exit_pct` считал бы профит
    /// заниженным и выбрасывал нас из позиции сразу после выплаты. WSOL это уже
    /// SOL: его raw-единицы И ЕСТЬ лампорты.
    async fn reward_value_sol(&self, reward_mint: &Pubkey, reward_raw: u64) -> f64 {
        if reward_raw == 0 {
            return 0.0;
        }
        if *reward_mint == WSOL_MINT {
            return reward_raw as f64 / LAMPORTS_PER_SOL;
        }
        self.jup
            .quote(reward_mint, &WSOL_MINT, reward_raw, self.jup.cfg().slippage_bps)
            .await
            .map(|q| Jupiter::out_amount(&q) as f64 / LAMPORTS_PER_SOL)
            .unwrap_or(0.0)
    }

    /// Сброс накопленных reward-стоков в SOL через Jupiter.
    ///
    /// Сканируем ОБЕ token-программы: стоки бывают и Token-2022, и обычные SPL
    /// Token (ZEC и подобные). Раньше смотрели только Token-2022 — заработанные
    /// legacy-реворды просто лежали на кошельке непроданными.
    pub async fn sell_rewards(&self) -> Result<()> {
        let owner = self.payer.pubkey();
        let mut accs = self.rpc.token_accounts(&owner, &TOKEN_2022_PROGRAM).await?;
        match self.rpc.token_accounts(&owner, &TOKEN_PROGRAM).await {
            Ok(mut legacy) => accs.append(&mut legacy),
            Err(e) => warn!("sell_rewards: не прочитать legacy-токены: {e}"),
        }
        let min_lamports = (self.jup.cfg().min_reward_sol * LAMPORTS_PER_SOL) as u64;

        // ⚠️ НИКОГДА не трогаем монету, которую держим позицией.
        //
        // Этот цикл сбрасывает в SOL всё, где есть баланс, и ровно поэтому
        // 27.10.2026 он УБИЛ позицию: в 09:39:20 бот купил GbPNoYXY на 1.5 SOL, а
        // в 09:39:52 этот же цикл продал её как «реворд» за 1.4630 SOL — через 32
        // секунды после входа, до всякой раздачи. Раньше это не проявлялось лишь
        // потому, что котировки Jupiter не работали вовсе.
        //
        // Смысл позиции в том, чтобы ДОЖДАТЬСЯ выплаты, поэтому её монета для
        // этого цикла неприкосновенна.
        let held: HashSet<Pubkey> = self.active.lock().await.clone();
        // ...И НЕ ТРОГАЕМ REWARD-АКТИВ ОТКРЫТОЙ ПОЗИЦИИ. Гейт выхода сторожит
        // ИМЕННО баланс нашего reward-ATA («нам начислили сток» = раунд нас
        // зачёл), сравнивая его с baseline, снятым на входе. Если этот цикл
        // успеет сбросить начисленное раньше, чем гейт его увидит, баланс
        // вернётся к baseline и выход не сработает НИКОГДА — позиция провисит
        // до аварийного бейла. Для WSOL хуже: close_account пересоздаёт
        // аккаунт, и Lighthouse-ассерт выхода сторожил бы новый пустой ATA.
        let held_rewards: HashSet<Pubkey> = self.active_reward_mints.lock().await.clone();

        for ta in accs {
            if ta.amount == 0 {
                continue;
            }
            if held.contains(&ta.mint) {
                continue; // это открытая позиция, а не реворд
            }
            if held_rewards.contains(&ta.mint) {
                continue; // сток открытой позиции — его сторожит гейт выхода
            }
            // WSOL-РЕВОРД НЕ СВОПАЕТСЯ — он уже SOL, просто завёрнутый, и
            // Jupiter на паре WSOL→WSOL отвечает ошибкой. Без этой ветви
            // заработанный фис навсегда оставался бы лежать в reward-ATA:
            // своп падает, а инсинератор WSOL-аккаунт намеренно не трогает.
            // Проверено 04.11.2026: у монеты верхушки борды (jV7eSrcE…,
            // котёл 0.0936 SOL) rewardMint = So1111…112, то есть фис придёт
            // именно в этом виде.
            //
            // Разворачиваем сами: `close_account` отдаёт владельцу И начисленный
            // баланс, И ренту аккаунта, а следующей инструкцией того же tx
            // открываем ATA заново (idempotent) — пустой reward-ATA это наше
            // право на СЛЕДУЮЩУЮ выплату, раздача аккаунтов не создаёт.
            if ta.mint == WSOL_MINT {
                if ta.amount < min_lamports {
                    continue; // пыль — рента на переоткрытие дороже
                }
                let sol = ta.amount as f64 / LAMPORTS_PER_SOL;
                if self.cfg.strategy.dry_run {
                    info!("[DRY] UNWRAP реворда WSOL: развернул бы {sol:.4} SOL");
                    continue;
                }
                let ixs = vec![
                    spl_token::instruction::close_account(
                        &TOKEN_PROGRAM,
                        &ta.address,
                        &owner,
                        &owner,
                        &[],
                    )?,
                    spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                        &owner,
                        &owner,
                        &WSOL_MINT,
                        &TOKEN_PROGRAM,
                    ),
                ];
                let mut tx_ixs = compute_budget(60_000, self.cfg.strategy.sell_cu_price).to_vec();
                tx_ixs.extend(ixs);
                let bh = self.rpc.latest_blockhash().await?;
                let tx = crate::txbuild::build_signed(&self.payer, &tx_ixs, bh, &no_alts(), &[])?;
                let wire = B64.encode(bincode::serialize(&tx).map_err(|e| anyhow!("serialize unwrap: {e}"))?);
                match self
                    .rpc
                    .raw(
                        "sendTransaction",
                        serde_json::json!([wire, {"encoding":"base64","skipPreflight":true,"maxRetries":3}]),
                    )
                    .await
                {
                    Ok(v) => info!(
                        "UNWRAP реворда WSOL: {sol:.4} SOL → кошелёк: {}",
                        v.as_str().unwrap_or("отправлено")
                    ),
                    Err(e) => warn!("UNWRAP реворда WSOL ({sol:.4} SOL): {e}"),
                }
                continue;
            }
            let quote = match self
                .jup
                .quote(&ta.mint, &WSOL_MINT, ta.amount, self.jup.cfg().slippage_bps)
                .await
            {
                Ok(q) => q,
                Err(e) => {
                    warn!("reward {}: нет котировки ({e})", ta.mint);
                    continue;
                }
            };
            let out = Jupiter::out_amount(&quote);
            if out < min_lamports {
                continue; // dust — не тратим комиссию
            }
            if self.cfg.strategy.dry_run {
                info!(
                    "[DRY] SELL reward {} amt={} → ~{:.4} SOL",
                    ta.mint,
                    ta.amount,
                    out as f64 / LAMPORTS_PER_SOL
                );
                continue;
            }
            match self
                .jup
                .swap_signed(&quote, &self.payer, self.cfg.strategy.sell_cu_price)
                .await
            {
                // Своп шлём ОБЫЧНЫМ RPC, а не через релеи-сендеры. Транзакцию
                // строит Jupiter, и вставить в неё tip-инструкцию мы не можем, а
                // Helius Sender без tip отвечает отказом («нет result») — из-за
                // этого продажа реворда падала даже с рабочей котировкой.
                // Скорость тут не нужна: это не гонка, а обычная конвертация.
                Ok(tx) => match bincode::serialize(&tx)
                    .map_err(|e| anyhow!("serialize swap: {e}"))
                    .and_then(|raw| Ok(B64.encode(raw)))
                {
                    Ok(wire) => match self
                        .rpc
                        .raw(
                            "sendTransaction",
                            serde_json::json!([wire, {"encoding":"base64","skipPreflight":true,"maxRetries":3}]),
                        )
                        .await
                    {
                        Ok(v) => info!(
                            "SELL reward {} → ~{:.4} SOL: {}",
                            ta.mint,
                            out as f64 / LAMPORTS_PER_SOL,
                            v.as_str().unwrap_or("отправлено")
                        ),
                        Err(e) => warn!("reward swap send {}: {e}", ta.mint),
                    },
                    Err(e) => warn!("reward swap serialize {}: {e}", ta.mint),
                },
                Err(e) => warn!("reward swap build {}: {e}", ta.mint),
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        Ok(())
    }

    // ───────── helpers ─────────

    /// Ждём ПРЕКУРСОР раздачи, чтобы не лить пролив вслепую весь холд.
    ///
    /// Воркер OTC сначала клеймит фисы pump В КОТЁЛ монеты, и только потом (у
    /// разобранной раздачи — через ~37с) свопает котёл в сток и раздаёт. Клейм
    /// виден как РЕЗКИЙ РОСТ `waitingSol` в /api/vault, а это обычный HTTP: можно
    /// опрашивать раз в секунду, не тратя ни RPC-лимитов, ни комиссий.
    ///
    /// Если шреды включены, тот же клейм видно мгновенно через `notify` (шред-поток
    /// дёргает будилку по кошельку раздач OTC) — тогда ждём, что сработает раньше,
    /// и экономим на окне пролива. Без шредов котёл несёт всю нагрузку сам.
    ///
    /// Возвращает true, если дождались триггера (пора лить), false — если вышло
    /// время холда. Если котла не видно (API молчит), не блокируем выход: лучше
    /// лить по бюджету, чем не продать совсем.
    async fn wait_for_round_trigger(
        &self,
        mint: &Pubkey,
        deadline: std::time::Instant,
        notify: &Notify,
    ) -> bool {
        let frac = self.cfg.strategy.exit_trigger_jump_frac;
        if frac <= 0.0 {
            return true; // триггер отключён — льём сразу
        }
        let mint_s = mint.to_string();
        let poll = poll_ms(self.cfg.strategy.exit_trigger_poll_ms);
        let mut base: Option<f64> = None;
        let mut blind = 0u32;
        // ГЛАВНЫЙ прекурсор — клейм воркера (`rewards.lastClaimAt`): от него до
        // раздачи ~37с. Запоминаем, что было на входе, и ждём РОСТА.
        let mut claim_base = self.oracle.last_claim_at(&mint_s).await;
        info!(
            "EXIT {mint}: ждём прекурсор раздачи (клейм воркера либо скачок котла ≥{:.0}% порога), пролив пока не льём",
            frac * 100.0
        );
        while std::time::Instant::now() < deadline {
            // 1) Клейм воркера — точный сигнал, что раздача через десятки секунд.
            if let Some(now_claim) = self.oracle.last_claim_at(&mint_s).await {
                match claim_base {
                    None => claim_base = Some(now_claim),
                    Some(prev) if now_claim > prev => {
                        info!("EXIT {mint}: ТРИГГЕР — воркер склеймил фисы (lastClaimAt {prev}→{now_claim}), раздача на подходе");
                        return true;
                    }
                    _ => {}
                }
            }
            // 2) Запасной сигнал — резкий рост котла.
            match self.oracle.vault(&mint_s).await {
                Ok(v) => {
                    blind = 0;
                    // Порог — НАСТОЯЩИЙ (0.10 SOL), а не floorSol из API: см.
                    // OtcVault::payout_floor. Иначе триггер считал бы от числа в
                    // 50 раз меньше и срабатывал на пустом котле.
                    let floor = v.payout_floor(self.cfg.strategy.payout_floor_sol);
                    let jump = floor * frac;
                    // ⚠️ Полнота котла — условие ВХОДА, а НЕ старта пролива. Мы и
                    // входим-то только когда котёл близок к порогу, так что проверка
                    // «котёл ≥ порога» срабатывала бы мгновенно на каждой позиции.
                    // Именно это сожгло весь бюджет (0.08 SOL за 41 секунду) при
                    // котле 148% — воркер приходит по своему расписанию и может
                    // тянуть минутами. Старт пролива даёт только КЛЕЙМ воркера.
                    match base {
                        None => base = Some(v.waiting_sol),
                        Some(prev) => {
                            // Резкий рост = воркер закинул фисы в котёл (прекурсор).
                            if v.waiting_sol - prev >= jump && jump > 0.0 {
                                info!(
                                    "EXIT {mint}: ТРИГГЕР — котёл {prev:.4}→{:.4} SOL (скачок ≥{jump:.4}, порог {floor:.4}), раздача на подходе",
                                    v.waiting_sol
                                );
                                return true;
                            }
                            // Котёл упал = раздача уже прошла без нас: новый отсчёт.
                            if v.waiting_sol < prev {
                                base = Some(v.waiting_sol);
                            }
                        }
                    }
                }
                Err(e) => {
                    blind += 1;
                    if blind == 5 {
                        warn!("EXIT {mint}: котёл не читается ({e}) → льём по бюджету вслепую");
                        return true;
                    }
                }
            }
            // Шред-будилка опережает опрос: что сработает первым, то и триггер.
            tokio::select! {
                _ = notify.notified() => {
                    info!("EXIT {mint}: ТРИГГЕР от шредов (событие по кошельку раздач OTC)");
                    return true;
                }
                _ = tokio::time::sleep(poll) => {}
            }
        }
        false
    }

    /// ГОНКА ВЫХОДА: держим заранее подписанную гейтованную продажу и льём её с
    /// частотой `exit_spam_tps`, пока раздача не начислит реворд — тогда гейт
    /// пропускает и мы исполняемся в первом же слоте, раньше конкурентов.
    ///
    /// Пока реворда нет, попытка падает на гейте. Это НЕ бесплатно: tip
    /// откатывается вместе со стейтом, но base fee (5000) и priority fee
    /// (sell_cu_price × sell_cu_limit) списываются — при текущем конфиге 21320
    /// лампортов за попытку. Поэтому пролив идёт только ОКНОМ после триггера и
    /// жёстко ограничен бюджетом `exit_spam_max_sol`.
    ///
    /// Возвращает (продано, попыток, бюджет_исчерпан). Исчерпан → звонящий должен
    /// не бейлить, а перейти в дешёвый опрос.
    async fn spam_gated_exit(
        &self,
        mint: &Pubkey,
        user_ata: &Pubkey,
        reward_ata: &Pubkey,
        threshold: u64,
        bal: u64,
        sell_ix: Instruction,
        notify: &Notify,
    ) -> (bool, u64, bool) {
        let hold_deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(self.cfg.strategy.hold_max_secs);
        if !self.wait_for_round_trigger(mint, hold_deadline, notify).await {
            return (false, 0, false);
        }

        let tps = self.cfg.strategy.exit_spam_tps.max(1);
        let gap = std::time::Duration::from_micros(1_000_000 / tps as u64);
        // Окно пролива не может пережить бюджет холда.
        let deadline = (std::time::Instant::now()
            + std::time::Duration::from_secs(self.cfg.strategy.exit_spam_window_secs.max(1)))
        .min(hold_deadline);
        // Цена ОДНОГО ВЫСТРЕЛА. Важно: send_cached веерит на ВСЕ эндпоинты сразу,
        // то есть один выстрел = по одной упавшей tx на каждый релей, и платим
        // комиссию за каждую. Поэтому цена попытки умножается на число релеев,
        // иначе потолок бюджета завышен ровно в это число раз.
        // Комиссия одной tx: base fee + priority по ЗАПРОШЕННОМУ лимиту CU.
        let relays = self.relays.endpoint_count().max(1) as u64;
        let per_tx = 5_000
            + (self.cfg.strategy.sell_cu_price as u128 * self.cfg.strategy.sell_cu_limit as u128
                / 1_000_000) as u64;
        let per_try = per_tx.saturating_mul(relays);
        let budget = (self.cfg.strategy.exit_spam_max_sol * LAMPORTS_PER_SOL) as u64;
        let max_tries = (budget / per_try.max(1)).max(1);
        let resign = std::time::Duration::from_secs(self.cfg.strategy.exit_resign_secs.max(5));
        let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let send_errs = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut fired = 0u64;
        let mut cached: Option<Vec<crate::relay::CachedTx>> = None;
        let mut signed_at = std::time::Instant::now();
        let mut last_check = std::time::Instant::now();
        info!(
            "EXIT {mint}: гонка — {tps} выстрелов/с × {relays} релеев = {} tx/с, окно {}с, \
             выстрел {per_try} лампортов ({per_tx}×{relays}), бюджет {} SOL = {max_tries} выстрелов",
            tps as u64 * relays,
            self.cfg.strategy.exit_spam_window_secs,
            self.cfg.strategy.exit_spam_max_sol
        );

        loop {
            if fired >= max_tries {
                warn!(
                    "EXIT {mint}: бюджет пролива исчерпан ({fired} попыток ≈ {:.4} SOL, отбито отправкой {}) → в дешёвый опрос",
                    (fired * per_try) as f64 / LAMPORTS_PER_SOL,
                    send_errs.load(std::sync::atomic::Ordering::Relaxed)
                );
                return (false, attempts.load(std::sync::atomic::Ordering::Relaxed), true);
            }
            if std::time::Instant::now() >= deadline {
                let a = attempts.load(std::sync::atomic::Ordering::Relaxed);
                warn!(
                    "EXIT {mint}: окно пролива закрыто ({fired} попыток ≈ {:.4} SOL) → в дешёвый опрос",
                    (fired * per_try) as f64 / LAMPORTS_PER_SOL
                );
                return (false, a, true);
            }
            // Переподписываем пролив: blockhash живёт ~60-90с.
            if cached.is_none() || signed_at.elapsed() >= resign {
                let mut ixs: Vec<Instruction> = compute_budget(
                    self.cfg.strategy.sell_cu_limit,
                    self.cfg.strategy.sell_cu_price,
                )
                .to_vec();
                // Гейт: исполниться только когда раздача начислила нам сток.
                ixs.push(assert_token_amount(reward_ata, threshold, Op::Gte));
                ixs.push(sell_ix.clone());
                match self.rpc.latest_blockhash().await {
                    Ok(bh) => match self.relays.build_cached(&self.payer, &ixs, bh, &no_alts(), &[]) {
                        Ok(c) => {
                            cached = Some(c);
                            signed_at = std::time::Instant::now();
                        }
                        Err(e) => {
                            warn!("EXIT {mint}: не подписать пролив: {e}");
                            tokio::time::sleep(poll_ms(self.cfg.strategy.poll_ms)).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        warn!("EXIT {mint}: нет blockhash: {e}");
                        tokio::time::sleep(poll_ms(self.cfg.strategy.poll_ms)).await;
                        continue;
                    }
                }
            }
            // Один «выстрел» — не ждём ответа, чтобы держать частоту.
            if let Some(c) = cached.clone() {
                let relays = self.relays.clone();
                let a = Arc::clone(&attempts);
                let errs = Arc::clone(&send_errs);
                fired += 1;
                tokio::spawn(async move {
                    // Результат отправки НЕ глотаем: прошлый пролив ушёл в пустоту
                    // (4638 транз, на чейн ноль), и мы об этом не знали, потому что
                    // ошибку отбрасывали. Логируем первые несколько.
                    if let Err(e) = relays.send_cached(&c).await {
                        let n = errs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if n < 3 {
                            warn!("пролив: отправка отбита ({e})");
                        }
                    }
                    a.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                });
            }
            // Раз в ~секунду проверяем, не продались ли уже.
            if last_check.elapsed() >= std::time::Duration::from_millis(1000) {
                last_check = std::time::Instant::now();
                if let Ok(now) = self.rpc.token_amount(user_ata).await {
                    if now == 0 || now < bal / 2 {
                        return (
                            true,
                            attempts.load(std::sync::atomic::Ordering::Relaxed),
                            false,
                        );
                    }
                }
            }
            tokio::time::sleep(gap).await;
        }
    }

    /// Текущий профит позиции в % к затратам: (монета по кривой + полученный
    /// реворд по котировке) / стоимость входа − 1. None, если данных не хватает.
    async fn position_profit_pct(
        &self,
        mint: &Pubkey,
        reward_mint: &Pubkey,
        sell_amount: u64,
        reward_ata: &Pubkey,
        reward_baseline: u64,
    ) -> Option<f64> {
        let cost = self.cfg.strategy.buy_sol;
        if cost <= 0.0 {
            return None;
        }
        // Сколько SOL дадут за наши токены прямо сейчас (спот по кривой).
        let acc = self.rpc.account(&pumpfun::bonding_curve(mint)).await.ok()??;
        let curve = pumpfun::parse_curve(&acc.data).ok()?;
        let coin_sol = curve.sol_out_for_tokens(sell_amount) as f64 / LAMPORTS_PER_SOL;
        // Сколько стоит начисленный реворд (по котировке Jupiter).
        let reward_now = self.rpc.token_amount(reward_ata).await.unwrap_or(reward_baseline);
        let reward_raw = reward_now.saturating_sub(reward_baseline);
        let reward_sol = self.reward_value_sol(reward_mint, reward_raw).await;
        Some(((coin_sol + reward_sol) / cost - 1.0) * 100.0)
    }

    /// Снять маркер «в позиции» на старте: если бот упал посреди сделки, старый
    /// маркер остался бы висеть и вечно блокировал бы деплой. На старте мы флэт.
    pub fn clear_stale_busy(&self) {
        self.set_busy(false);
    }

    /// Создать/удалить маркер «в позиции» (см. `strategy.busy_marker`). Best-effort:
    /// ошибки ФС только логируем — они не должны ронять торговлю.
    fn set_busy(&self, busy: bool) {
        let path = &self.cfg.strategy.busy_marker;
        if busy {
            if let Err(e) = std::fs::write(path, b"1") {
                warn!("не смог создать busy-маркер {path}: {e}");
            }
        } else if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("не смог удалить busy-маркер {path}: {e}");
            }
        }
    }
}

fn apply_slip_up(lamports: u64, bps: u64) -> u64 {
    lamports + lamports * bps / 10_000
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Период опроса как Duration (мелкий хелпер для пролива).
fn poll_ms(ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(ms.max(50))
}
