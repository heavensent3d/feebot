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
    /// mint → (заполнение котла в %, когда измерено). Обновляется ФОНОВЫМ
    /// параллельным обходом: последовательный опрос стоит 851 мс на монету, то
    /// есть полный проход по 500 кандидатам занимал бы 426 секунд, и котлы за
    /// это время успевают смениться. При 20 одновременных запросах тот же обход
    /// укладывается в 33 секунды, и тик может выбирать САМЫЙ полный котёл, а не
    /// первый попавшийся.
    pot_cache: Arc<Mutex<HashMap<Pubkey, (f64, std::time::Instant)>>>,
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
            pot_cache: Arc::new(Mutex::new(HashMap::new())),
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
        crate::holders::qualified_float_usd(&self.rpc, &c.mint, price_usd, 20.0, &exclude)
            .await
            .ok()
            .map(|bd| bd.float_usd)
    }

    /// ФОНОВЫЙ ОБХОД КОТЛОВ. Тянет борду и опрашивает `/api/vault` по всем
    /// кандидатам ПАРАЛЛЕЛЬНО, складывая заполнение в `pot_cache`.
    ///
    /// Зачем отдельный цикл: заполнение котла — единственный сигнал «выплата
    /// близко», но он живёт в per-coin запросе. Последовательно это 851 мс на
    /// монету (замер), то есть 426 секунд на 500 кандидатов — за такой проход
    /// котлы полностью обновляются, и вход получался по случайной монете.
    /// При 20 одновременных запросах тот же проход занимает 33 секунды.
    /// HTTP к борде бесплатный, RPC здесь не трогается вовсе.
    pub async fn pot_scan_loop(self: Arc<Self>) {
        let conc = self.cfg.strategy.pot_scan_concurrency.max(1) as usize;
        let every = std::time::Duration::from_secs(self.cfg.strategy.pot_scan_interval_secs.max(5));
        loop {
            let started = std::time::Instant::now();
            let cands = match self.oracle.candidates().await {
                Ok(c) => c,
                Err(e) => {
                    warn!("обход котлов: борда недоступна ({e})");
                    tokio::time::sleep(every).await;
                    continue;
                }
            };
            let floor_cfg = self.cfg.strategy.payout_floor_sol;
            let mut fresh: HashMap<Pubkey, (f64, std::time::Instant)> = HashMap::new();
            let mut it = cands.into_iter();
            let mut set: tokio::task::JoinSet<Option<(Pubkey, f64)>> = tokio::task::JoinSet::new();
            loop {
                // Держим ровно `conc` запросов в полёте.
                while set.len() < conc {
                    let Some(c) = it.next() else { break };
                    let me = Arc::clone(&self);
                    set.spawn(async move {
                        let v = me.oracle.vault(&c.mint.to_string()).await.ok()?;
                        if v.graduated {
                            return None;
                        }
                        Some((c.mint, v.fill_pct(floor_cfg)))
                    });
                }
                match set.join_next().await {
                    Some(Ok(Some((mint, fill)))) => {
                        fresh.insert(mint, (fill, std::time::Instant::now()));
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            let ready = fresh
                .values()
                .filter(|(f, _)| *f >= self.cfg.strategy.min_vault_fill_pct)
                .count();
            info!(
                "обход котлов: {} монет за {:.1}с, готовых к входу {ready}",
                fresh.len(),
                started.elapsed().as_secs_f64()
            );
            *self.pot_cache.lock().await = fresh;
            tokio::time::sleep(every).await;
        }
    }

    /// Один проход отбора: берём кандидатов, фильтруем, открываем позиции.
    pub async fn tick_entries(self: &Arc<Self>) -> Result<()> {
        // Потолок занят целиком → не дёргаем RPC впустую (при max=1 это значит
        // «позиция уже открыта»).
        if self.slots.available_permits() == 0 {
            return Ok(());
        }
        let mut cands = self.oracle.candidates().await?;
        // РАНЖИРОВАНИЕ ПО КОТЛУ. Цикл ниже берёт ПЕРВОГО прошедшего кандидата,
        // поэтому порядок списка и есть выбор позиции. Фоновый обход
        // (pot_scan_loop) уже измерил заполнение котлов параллельно, так что
        // здесь это стоит одного взятия мьютекса: ставим вперёд самые полные.
        // Без него вход доставался случайной монете — котёл проверялся только
        // у того кандидата, до которого дошла очередь.
        {
            let pots = self.pot_cache.lock().await;
            if !pots.is_empty() {
                cands.sort_by(|a, b| {
                    let fa = pots.get(&a.mint).map(|(f, _)| *f).unwrap_or(-1.0);
                    let fb = pots.get(&b.mint).map(|(f, _)| *f).unwrap_or(-1.0);
                    fb.partial_cmp(&fa).unwrap_or(std::cmp::Ordering::Equal)
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
        let (mut sk_active, mut sk_cooldown, mut sk_rounds, mut sk_notpl) = (0, 0, 0, 0);
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
            // Эдж = КРОШЕЧНЫЙ флоат (наша доля большая). Над порогом доля мелкая → скип.
            if let Some(f) = float_override {
                if f > max_float {
                    sk_float += 1;
                    continue;
                }
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
            // Говорим ВСЛУХ, почему ни один кандидат не подошёл.
            info!(
                "tick: из {total} кандидатов не подошёл никто — отсев: держим {sk_active}, \
                 кулдаун {sk_cooldown}, нет раундов {sk_rounds}, нет шаблона {sk_notpl}, \
                 прескрин {sk_pre1}, реворды {sk_pre2}, флоат {sk_float}, EV {sk_eval}, \
                 сток занят {sk_rmint}"
            );
            let mut top: Vec<_> = reasons.into_iter().collect();
            top.sort_by(|a, b| b.1.cmp(&a.1));
            let top: Vec<String> = top.iter().take(5).map(|(r, n)| format!("{r} ×{n}")).collect();
            if !top.is_empty() {
                info!("tick: что именно режет — {}", top.join(" | "));
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
        let buy_lamports = (self.cfg.strategy.buy_sol * LAMPORTS_PER_SOL) as u64;
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
            info!("{mint}: уже выпустилась (pAMM) — пропуск в MVP");
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
        let reward_tp = pumpfun::coin_token_program(&self.rpc, &c.reward_mint).await;

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
        // OTC-монеты — Token-2022; берём реальную token-программу минта.
        let coin_tp = pumpfun::coin_token_program(&self.rpc, &mint).await;
        let tokens_out = curve.tokens_out_for_sol(buy_lamports);
        let min_tokens = tokens_out - tokens_out * self.cfg.strategy.slippage_bps / 10_000;

        // ── КОТЁЛ МОНЕТЫ: один бесплатный HTTP, два решения о входе ──
        // Делаем ДО кэша coin_fee, потому что это решение про КАЖДЫЙ вход, а не
        // про разовый скрейп: монета с закэшированными фисами всё равно обязана
        // проходить проверку «раунд близко».
        match self.oracle.vault(&mint.to_string()).await {
            // Монета, ушедшая на AMM, торгуется не программой pump, поэтому скрейп
            // coin_fee вернёт пусто — 31 RPC-вызов в мусор (а на free-RPC ещё и 429
            // на остальные пути).
            //
            // ⚠️ Смотреть ТОЛЬКО на `graduated`. `curveExists` к этому НЕ относится:
            // это наличие curve-ноги в котле, и у живых платящих монет оно сплошь
            // false. Проверено на STOCKS — distributed 0.177 SOL на 2 холдера при
            // `graduated:false, curveExists:false`, то есть условие с curve_exists
            // отбрасывало ровно тех, на кого мы и охотимся.
            Ok(v) if v.graduated => {
                info!("{mint}: ушла на AMM (graduated) — скип без RPC-скрейпа (кулдаун {}s)",
                      Self::NO_TEMPLATE_COOLDOWN_SECS);
                self.no_template
                    .lock()
                    .await
                    .insert(mint, std::time::Instant::now());
                return Ok(());
            }
            Ok(v) => {
                // КОТЁЛ ПУСТ → раунд в наш холд не придёт, и мы отсидим
                // hold_max_secs только чтобы выйти по бейлу в минус. Монета может
                // быть идеальной по капе/холдерам/истории выплат и всё равно быть
                // убыточной ПРЯМО СЕЙЧАС. Фильтруем долей, а не абсолютом:
                // floorSol у монет разный (видели 0.002 и 0.1).
                let floor = v.payout_floor(self.cfg.strategy.payout_floor_sol);
                let fill = v.fill_pct(self.cfg.strategy.payout_floor_sol);
                if fill < self.cfg.strategy.min_vault_fill_pct {
                    info!(
                        "{mint}: котёл заполнен {fill:.1}% (нужно ≥{:.0}%, {:.4}/{floor:.4} SOL) — раунд не успеет, скип",
                        self.cfg.strategy.min_vault_fill_pct, v.waiting_sol
                    );
                    // ⚠️ НЕ ставим кулдаун recent_trade: это «раунд не близко ПРЯМО
                    // СЕЙЧАС», а не «мы только что торговали». Котёл наполняется за
                    // минуты, и монета может подойти уже через тик. Кулдаун здесь
                    // отравлял бы весь пул кандидатов: за пару минут все они ушли бы
                    // в 120-секундный бан, и бот замолчал бы без причины.
                    return Ok(());
                }
                info!("{mint}: котёл заполнен {fill:.1}% ({:.4}/{floor:.4} SOL) — раунд близко, входим",
                      v.waiting_sol);
            }
            // Котёл не прочитался — не блокируем вход, идём дальше.
            Err(_) => {}
        }

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
                    info!("{mint}: нет coin_fee ({e}) — пропуск позиции (кулдаун {}s)", Self::NO_TEMPLATE_COOLDOWN_SECS);
                    self.no_template
                        .lock()
                        .await
                        .insert(mint, std::time::Instant::now());
                    return Ok(());
                }
            },
        };
        // ATA мемкоина и ATA reward-стока — КАЖДЫЙ под своей token-программой.
        let coin_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &me, &me, &mint, &coin_tp,
        );
        let reward_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &me, &me, &c.reward_mint, &reward_tp,
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
        ixs.extend([coin_ata_ix, reward_ata_ix, buy_ix]);

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
            self.emit_report(&c, &our_reward_ata, reward_baseline, lamports_before, attempts)
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
        let reward_value_sol = if reward_raw > 0 {
            self.jup
                .quote(&c.reward_mint, &WSOL_MINT, reward_raw, self.jup.cfg().slippage_bps)
                .await
                .map(|q| Jupiter::out_amount(&q) as f64 / LAMPORTS_PER_SOL)
                .unwrap_or(0.0)
        } else {
            0.0
        };

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

        for ta in accs {
            if ta.amount == 0 {
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
        let reward_sol = if reward_raw > 0 {
            self.jup
                .quote(reward_mint, &WSOL_MINT, reward_raw, self.jup.cfg().slippage_bps)
                .await
                .map(|q| Jupiter::out_amount(&q) as f64 / LAMPORTS_PER_SOL)
                .unwrap_or(0.0)
        } else {
            0.0
        };
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
