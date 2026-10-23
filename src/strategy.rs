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
use crate::programs::{TOKEN_2022_PROGRAM, WSOL_MINT};
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
use std::collections::{HashMap, HashSet};
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
    /// Будилка цикла входа: шреды будят немедленный опрос борды OTC.
    poll_now: Arc<Notify>,
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
            poll_now: Arc::new(Notify::new()),
        }
    }

    /// Будилка цикла входа (её дёргает шред-консьюмер).
    pub fn poll_now(&self) -> Arc<Notify> {
        self.poll_now.clone()
    }

    /// Консьюмер шред-потока: раунд по нашему стоку → немедленный выход;
    /// любая активность reward-кошелька → немедленный опрос борды на вход.
    pub async fn consume_shreds(self: Arc<Self>, mut rx: mpsc::Receiver<ShredEvent>) {
        use crate::programs::OTC_LAUNCHER_REWARDS;
        while let Some(ev) = rx.recv().await {
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
                info!("shred: раунд по нашему стоку в slot={} → триггер выхода", ev.slot);
            }
            if ev.touches(&OTC_LAUNCHER_REWARDS) {
                self.poll_now.notify_one();
            }
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

    /// Один проход отбора: берём кандидатов, фильтруем, открываем позиции.
    pub async fn tick_entries(self: &Arc<Self>) -> Result<()> {
        // Потолок занят целиком → не дёргаем RPC впустую (при max=1 это значит
        // «позиция уже открыта»).
        if self.slots.available_permits() == 0 {
            return Ok(());
        }
        let cands = self.oracle.candidates().await?;
        let sol_price = self.sol_price_usd().await;
        let buy_sol = self.cfg.strategy.buy_sol;
        let mut considered = 0usize;
        for c in cands {
            // Нет свободного слота — дальше перебирать незачем.
            if self.slots.available_permits() == 0 {
                break;
            }
            // Уже держим этот мемкоин.
            if self.active.lock().await.contains(&c.mint) {
                continue;
            }
            // Дешёвый прескрин без RPC.
            if matches!(self.filters.prescreen(&c), Verdict::Skip(_)) {
                continue;
            }
            // Реальный он-чейн флоат только для прошедших прескрин (экономим RPC).
            let float_override = self.float_override(&c, sol_price).await;
            match self.filters.evaluate(&c, buy_sol, sol_price, float_override) {
                Verdict::Skip(_reason) => continue,
                Verdict::Take => {}
            }
            // Не фармить два мемкоина на один и тот же reward-сток.
            {
                let mut rm = self.active_reward_mints.lock().await;
                if rm.contains(&c.reward_mint) {
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
                me.active.lock().await.remove(&mint);
                me.active_reward_mints.lock().await.remove(&reward_mint);
            });
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

        // НАШ ATA под reward-сток (Token-2022). Гейт выхода смотрит именно на него:
        // «нам начислили сток» — безопасный сигнал, что раунд нас зачёл.
        let our_reward_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &self.payer.pubkey(),
            &c.reward_mint,
            &TOKEN_2022_PROGRAM,
        );
        let reward_baseline = self.rpc.token_amount(&our_reward_ata).await.unwrap_or(0);

        // ───── ВХОД ─────
        let me = self.payer.pubkey();
        // OTC-монеты — Token-2022; берём реальную token-программу минта.
        let coin_tp = pumpfun::coin_token_program(&self.rpc, &mint).await;
        let tokens_out = curve.tokens_out_for_sol(buy_lamports);
        let min_tokens = tokens_out - tokens_out * self.cfg.strategy.slippage_bps / 10_000;

        // Шаблон buy_exact_sol_in из свежей реальной tx монеты (fail-safe: нет → скип).
        let buy_tpl = match pumpfun::fetch_template(&self.rpc, &mint, &pumpfun::DISC_BUY_EXACT_SOL_IN).await {
            Ok(t) => t,
            Err(e) => {
                info!("{mint}: нет шаблона buy ({e}) — пропуск позиции");
                return Ok(());
            }
        };
        // ATA мемкоина (его token-программа) + ATA reward-стока (Token-2022).
        let coin_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &me, &me, &mint, &coin_tp,
        );
        let reward_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &me, &me, &c.reward_mint, &TOKEN_2022_PROGRAM,
        );
        let buy_ix = match pumpfun::build_buy_exact_from_template(&buy_tpl, &me, &mint, &coin_tp, buy_lamports, min_tokens) {
            Ok(ix) => ix,
            Err(e) => {
                info!("{mint}: buy-шаблон кривой ({e}) — пропуск");
                return Ok(());
            }
        };

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
            .run_exit(&c, coin_tp, reward_baseline, &our_reward_ata, &notify)
            .await;
        self.deregister_exit(&our_reward_ata, &notify).await;
        self.deregister_exit(&mint, &notify).await;
        let (closed, attempts, sell_amount) = r?;
        if closed && !self.cfg.strategy.dry_run {
            self.emit_report(&c, &curve, &our_reward_ata, reward_baseline, sell_amount, attempts)
                .await;
        }
        Ok(())
    }

    /// Собрать подробный PnL по закрытой сделке и отдать репортеру (TG+журнал).
    async fn emit_report(
        &self,
        c: &Candidate,
        curve: &pumpfun::Curve,
        reward_ata: &Pubkey,
        reward_baseline: u64,
        sell_amount: u64,
        attempts: u64,
    ) {
        let sol_price = self.sol_price_usd().await;
        let coin_sell_sol = curve.sol_out_for_tokens(sell_amount) as f64 / LAMPORTS_PER_SOL;

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
            tips_lamports: 0,
            spam_attempts: attempts,
            spam_cost_lamports: attempts.saturating_mul(5000),
            ata_rent_lamports: 2 * 2_039_280,
        };
        let buy_sol = s.buy_sol;
        let net_sol = (coin_sell_sol - buy_sol) + reward_value_sol
            - fees.total_nonrefundable() as f64 / LAMPORTS_PER_SOL;

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
        let min_sol_output = 0u64; // широкий — лишь бы приземлилось
        let threshold = reward_baseline.saturating_add(self.cfg.strategy.exit_gate_epsilon);
        let gate = self.cfg.strategy.lighthouse_exit_gate;

        let nonce_pk = self
            .cfg
            .strategy
            .nonce_account
            .as_deref()
            .and_then(|s| Pubkey::from_str(s).ok());

        let spam_slots = self.cfg.strategy.exit_spam_slots;
        let poll = std::time::Duration::from_millis(self.cfg.strategy.poll_ms);

        // sell по шаблону реальной tx монеты (Token-2022). В dry sell_amount=0 → сеть не трогаем.
        let sell_ix = if sell_amount > 0 {
            let tpl = pumpfun::fetch_template(&self.rpc, &mint, &pumpfun::DISC_SELL).await?;
            Some(pumpfun::build_sell_from_template(&tpl, &me, &mint, &coin_tp, sell_amount, min_sol_output)?)
        } else {
            None
        };

        // Сборщик инструкций выхода. advance=Some → durable nonce (ПЕРВОЙ инструкцией).
        let build_ixs = |advance: Option<(Pubkey, Pubkey)>| -> Vec<Instruction> {
            let mut ixs: Vec<Instruction> = Vec::new();
            if let Some((npk, auth)) = advance {
                ixs.push(nonce::advance_ix(&npk, &auth));
            }
            ixs.extend(
                compute_budget(self.cfg.strategy.sell_cu_limit, self.cfg.strategy.sell_cu_price)
                    .to_vec(),
            );
            // Гейт на НАШ ATA стока: нам начислили ⇒ раунд нас зачёл ⇒ можно дампить.
            if gate {
                ixs.push(assert_token_amount(reward_ata, threshold, Op::Gte));
            }
            if let Some(ix) = &sell_ix {
                ixs.push(ix.clone());
            }
            ixs
        };

        if self.cfg.strategy.dry_run {
            info!(
                "[DRY] EXIT {mint}: гейт amount(ourReward {reward_ata})≥{threshold}, продать {sell_amount} токенов (nonce={})",
                nonce_pk.is_some()
            );
            return Ok((false, 0, sell_amount));
        }

        let mut attempts = 0u64;

        // ── durable nonce: подписываем ОДИН РАЗ, спамим кэш ──
        if let Some(npk) = nonce_pk {
            let nd = nonce::read_nonce(&self.rpc, &npk).await?;
            // Выход подписывает только payer; если authority nonce другой — tx не
            // соберётся. Требуем authority == payer (так делает `feebot init-nonce`).
            if nd.authority != me {
                return Err(anyhow!(
                    "nonce authority {} != payer {me} — пересоздай nonce с authority=payer (feebot init-nonce)",
                    nd.authority
                ));
            }
            let ixs = build_ixs(Some((npk, nd.authority)));
            let cached = self
                .relays
                .build_cached(&self.payer, &ixs, nd.blockhash, &no_alts(), &[])?;
            for round in 0..spam_slots {
                match self.relays.send_cached(&cached).await {
                    Ok(sig) if round % 10 == 0 => info!("EXIT {mint} nonce-спам #{round}: {sig}"),
                    Ok(_) => {}
                    Err(e) => warn!("EXIT {mint} отправка: {e}"),
                }
                attempts += 1;
                if let Ok(now) = self.rpc.token_amount(&user_ata).await {
                    if now == 0 || now < bal / 2 {
                        info!("EXIT {mint}: позиция закрыта (остаток {now})");
                        return Ok((true, attempts, sell_amount));
                    }
                }
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        } else {
            // ── свежий blockhash: переподписываем, blockhash обновляем раз в ~20 итераций ──
            let mut bh = self.rpc.latest_blockhash().await?;
            for round in 0..spam_slots {
                if round > 0 && round % 20 == 0 {
                    if let Ok(h) = self.rpc.latest_blockhash().await {
                        bh = h;
                    }
                }
                let ixs = build_ixs(None);
                match self
                    .relays
                    .broadcast_build(&self.payer, &ixs, bh, &no_alts(), &[])
                    .await
                {
                    Ok(sig) if round % 10 == 0 => info!("EXIT {mint} спам #{round}: {sig}"),
                    Ok(_) => {}
                    Err(e) => warn!("EXIT {mint} отправка: {e}"),
                }
                attempts += 1;
                if let Ok(now) = self.rpc.token_amount(&user_ata).await {
                    if now == 0 || now < bal / 2 {
                        info!("EXIT {mint}: позиция закрыта (остаток {now})");
                        return Ok((true, attempts, sell_amount));
                    }
                }
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }

        warn!("EXIT {mint}: гейт не сработал за {spam_slots} слотов — позиция висит, разбирайся вручную");
        Ok((false, attempts, sell_amount))
    }

    /// Сброс накопленных reward-стоков (Token-2022) в SOL через Jupiter.
    pub async fn sell_rewards(&self) -> Result<()> {
        let owner = self.payer.pubkey();
        let accs = self.rpc.token_accounts(&owner, &TOKEN_2022_PROGRAM).await?;
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
                Ok(tx) => match self.relays.broadcast(&tx).await {
                    Ok(sig) => info!(
                        "SELL reward {} → ~{:.4} SOL: {sig}",
                        ta.mint,
                        out as f64 / LAMPORTS_PER_SOL
                    ),
                    Err(e) => warn!("reward swap send {}: {e}", ta.mint),
                },
                Err(e) => warn!("reward swap build {}: {e}", ta.mint),
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        Ok(())
    }

    // ───────── helpers ─────────

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
