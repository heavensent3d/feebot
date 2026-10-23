//! feebot — фарм reward-раздач лаунчера otcdesks.cash.
//!
//! По умолчанию strategy.dry_run = true: бот всё СТРОИТ и логирует, но НИЧЕГО
//! не шлёт. Сначала посмотри логи и сверь layout'ы pump.fun/Lighthouse с
//! актуальным mainnet, и только потом выключай dry_run.

mod config;
mod filters;
mod holders;
mod incinerator;
mod jupiter;
mod keys;
mod lighthouse;
mod nonce;
mod oracle;
mod otc;
mod programs;
mod pumpfun;
mod pumpswap;
mod relay;
mod reporter;
mod rpc;
mod shreds;
mod strategy;
mod txbuild;

use crate::config::Config;
use crate::filters::Filters;
use crate::incinerator::Incinerator;
use crate::jupiter::Jupiter;
use crate::oracle::Oracle;
use crate::relay::RelaySet;
use crate::rpc::Rpc;
use crate::strategy::Bot;
use crate::txbuild::{build_signed, compute_budget, no_alts};
use anyhow::Result;
use clap::{Parser, Subcommand};
use solana_sdk::signature::{Signature, Signer};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "feebot", version, about = "OTC launcher reward farming bot")]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Полный цикл: вход/выход + продажа наград + инсинератор.
    Run,
    /// Разово вывести отфильтрованных кандидатов (всегда без отправки).
    Candidates,
    /// Разово продать накопленные награды.
    SellRewards,
    /// Разово закрыть пустые мемкоин-аккаунты.
    Burn,
    /// Тейлить шред-поток (Raiden Pulse) — проверить подписку/фильтр.
    Shreds,
    /// Замерить land-latency по каждому релею (ТРАТИТ SOL: комиссии + tip).
    Bench {
        /// Сколько транзакций на каждый релей.
        #[arg(long, default_value_t = 3)]
        iters: u32,
    },
    /// Создать durable-nonce аккаунт (authority = кошелёк бота) для sign-once выхода.
    InitNonce,
    /// Отправить тестовое сообщение в Telegram (проверка подключения).
    TestTelegram,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;

    let payer = Arc::new(keys::load_keypair(&cfg.wallet.keypair_path)?);
    info!("кошелёк бота: {}", keys::pubkey_str(&payer));

    let rpc = Rpc::new(&cfg.rpc.http, &cfg.rpc.commitment);
    let send_url = cfg.rpc.send_http.clone().unwrap_or_else(|| cfg.rpc.http.clone());
    let relays = RelaySet::new(send_url, &cfg.relays);
    let jup = Jupiter::new(cfg.jupiter.clone());
    let oracle = Oracle::new(&cfg.otc);
    let filters = Filters::new(cfg.filters.clone());

    match rpc.lamports(&payer.pubkey()).await {
        Ok(l) => info!("баланс: {:.4} SOL", l as f64 / 1e9),
        Err(e) => warn!("не прочитать баланс: {e}"),
    }
    if cfg.strategy.dry_run {
        info!("DRY-RUN включён: транзакции НЕ отправляются");
    }
    // Без durable nonce веерный выход с разными tip даёт разные подписи; при
    // sell_fraction<1 это грозит двойной продажей. Предупреждаем.
    if cfg.strategy.nonce_account.is_none()
        && relays.endpoint_count() > 1
        && cfg.strategy.sell_fraction < 1.0
    {
        warn!("нет nonce_account + sell_fraction<1 + несколько релеев → риск двойной продажи на выходе. Включи durable nonce (feebot init-nonce) или sell_fraction=1.0");
    }

    let inc_cfg = cfg.incinerator.clone();
    let reporter = Arc::new(reporter::Reporter::new(cfg.telegram.clone()));
    let bot = Arc::new(Bot::new(cfg.clone(), rpc.clone(), relays.clone(), jup, oracle, filters, payer.clone(), reporter));

    match cli.cmd {
        Cmd::Candidates => {
            let cands = bot.oracle.candidates().await?;
            let sol_price = bot.sol_price_usd().await;
            let buy_sol = cfg.strategy.buy_sol;
            info!("кандидатов с борды: {} (SOL=${sol_price:.2})", cands.len());
            for c in &cands {
                // candidates — быстрый обзор: EV на допущении флоата (без он-чейн).
                let v = bot.filters.evaluate(c, buy_sol, sol_price, None);
                let ev = bot.filters.expected_ev_sol(c, buy_sol, sol_price, None);
                info!(
                    "{} sym={} reward={} mcap={:?} vol={:?} liq={:?} EV={:?} → {:?}",
                    c.mint, c.symbol, c.reward_mint, c.market_cap_usd, c.volume_24h_usd, c.liquidity_usd, ev, v
                );
            }
        }
        Cmd::SellRewards => {
            bot.sell_rewards().await?;
        }
        Cmd::Burn => {
            run_burn(&rpc, &relays, &payer, &Incinerator::new(inc_cfg), cfg.strategy.dry_run).await?;
        }
        Cmd::Shreds => {
            run_shreds_tail(cfg.shreds.clone()).await?;
        }
        Cmd::Bench { iters } => {
            bench_relays(&rpc, &relays, &payer, iters).await?;
        }
        Cmd::InitNonce => {
            init_nonce(&rpc, &relays, &payer, cfg.strategy.dry_run).await?;
        }
        Cmd::TestTelegram => {
            bot.reporter.test_message().await;
        }
        Cmd::Run => {
            run_loops(bot, rpc, relays, payer, inc_cfg).await?;
        }
    }
    Ok(())
}

/// Тейл шред-потока: печатаем задевшие фильтр транзакции с метками программ.
async fn run_shreds_tail(shreds_cfg: config::ShredsConfig) -> Result<()> {
    use programs::{OTC_LAUNCHER_REWARDS, PUMP_PROGRAM, PUMPSWAP_PROGRAM};
    let (tx, mut rx) = tokio::sync::mpsc::channel::<shreds::ShredEvent>(10_000);
    let cfg2 = shreds_cfg.clone();
    tokio::spawn(async move {
        if let Err(e) = shreds::run(cfg2, tx).await {
            warn!("shreds: {e:#}");
        }
    });
    info!("тейлю шреды с {} — Ctrl+C для выхода", shreds_cfg.endpoint);
    while let Some(ev) = rx.recv().await {
        let mut tags = Vec::new();
        if ev.touches(&OTC_LAUNCHER_REWARDS) {
            tags.push("REWARD-WALLET");
        }
        if ev.uses_program(&PUMP_PROGRAM) {
            tags.push("pump");
        }
        if ev.uses_program(&PUMPSWAP_PROGRAM) {
            tags.push("pAMM");
        }
        info!(
            "slot={} {} filters={:?} accts={} sig={}",
            ev.slot,
            tags.join(","),
            ev.filters,
            ev.accounts.len(),
            ev.signature
        );
    }
    Ok(())
}

/// Замер land-latency по релеям: на каждый шлём self-transfer с его tip и ждём
/// подтверждения. ВНИМАНИЕ: тратит реальные SOL (комиссия + tip каждого релея).
async fn bench_relays(
    rpc: &Rpc,
    relays: &RelaySet,
    payer: &Arc<solana_sdk::signature::Keypair>,
    iters: u32,
) -> Result<()> {
    use solana_sdk::system_instruction;
    let n = relays.endpoint_count();
    info!("bench: {} релеев × {} итераций (тратит SOL!)", n, iters);

    for i in 0..n {
        let name = relays.endpoint_name(i).unwrap_or("?").to_string();
        let mut lats = Vec::new();
        for _ in 0..iters {
            // self-transfer 1 lamport + мелкий priority fee; tip добавит relay.
            let mut ixs = txbuild::compute_budget(20_000, 100_000).to_vec();
            ixs.push(system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 1));
            let bh = rpc.latest_blockhash().await?;
            let t0 = Instant::now();
            let sig = match relays.submit_to(i, payer, &ixs, bh, &txbuild::no_alts(), &[]).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("bench {name}: отправка не прошла: {e}");
                    continue;
                }
            };
            match await_confirm(rpc, &sig, Duration::from_secs(25)).await {
                Some(ms) => {
                    info!("bench {name}: {ms} ms  (sig {})", &sig[..sig.len().min(16)]);
                    lats.push(ms);
                }
                None => warn!("bench {name}: не подтвердилась за 25s (sig {sig})"),
            }
        }
        if !lats.is_empty() {
            lats.sort_unstable();
            let med = lats[lats.len() / 2];
            let min = *lats.first().unwrap();
            info!("bench {name}: landed {}/{}  min={}ms median={}ms", lats.len(), iters, min, med);
        } else {
            warn!("bench {name}: ни одной подтверждённой");
        }
    }
    Ok(())
}

/// Создать durable-nonce аккаунт с authority = кошелёк бота.
async fn init_nonce(
    rpc: &Rpc,
    relays: &RelaySet,
    payer: &Arc<solana_sdk::signature::Keypair>,
    dry_run: bool,
) -> Result<()> {
    use solana_sdk::signature::Keypair;
    use solana_sdk::system_instruction;
    let nonce_kp = Keypair::new();
    let nonce_pk = nonce_kp.pubkey();
    // Размер nonce-аккаунта системной программы = 80 байт; берём ренту под него.
    let rent = rpc
        .client
        .get_minimum_balance_for_rent_exemption(80)
        .await?;
    let ixs = system_instruction::create_nonce_account(
        &payer.pubkey(),
        &nonce_pk,
        &payer.pubkey(), // authority = payer (иначе выход не подпишется)
        rent,
    );
    info!("init-nonce: {nonce_pk} (рента {:.5} SOL)", rent as f64 / 1e9);
    if dry_run {
        info!("[DRY] nonce НЕ создан (dry_run). Для создания выключи dry_run.");
        return Ok(());
    }
    let bh = rpc.latest_blockhash().await?;
    let sig = relays
        .submit_primary(payer, &ixs, bh, &txbuild::no_alts(), &[&nonce_kp])
        .await?;
    // Сохраняем секрет nonce-аккаунта рядом (понадобится при закрытии).
    std::fs::write(
        "nonce-keypair.json",
        serde_json::to_string(&nonce_kp.to_bytes().to_vec())?,
    )?;
    info!("nonce создан: {sig}");
    info!("впиши в config.toml:  [strategy] nonce_account = \"{nonce_pk}\"");
    info!("секрет сохранён в nonce-keypair.json (нужен только для закрытия аккаунта)");
    Ok(())
}

/// Ждём подтверждения сигнатуры, возвращаем мс до появления статуса.
async fn await_confirm(rpc: &Rpc, sig: &str, timeout: Duration) -> Option<u64> {
    let sig = Signature::from_str(sig).ok()?;
    let t0 = Instant::now();
    loop {
        if let Ok(resp) = rpc.client.get_signature_statuses(&[sig]).await {
            if let Some(Some(st)) = resp.value.into_iter().next() {
                if st.err.is_none() {
                    return Some(t0.elapsed().as_millis() as u64);
                }
                return Some(t0.elapsed().as_millis() as u64); // приземлилась, пусть с ошибкой
            }
        }
        if t0.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Основные фоновые циклы.
async fn run_loops(
    bot: Arc<Bot>,
    rpc: Rpc,
    relays: RelaySet,
    payer: Arc<solana_sdk::signature::Keypair>,
    inc_cfg: config::IncineratorConfig,
) -> Result<()> {
    // Снять возможный залипший busy-маркер (краш посреди сделки) — на старте флэт.
    bot.clear_stale_busy();
    // 1) вход/выход. Шреды будят опрос немедленно; иначе — по таймеру.
    {
        let bot = Arc::clone(&bot);
        let poll_now = bot.poll_now();
        let period = std::time::Duration::from_millis(bot.cfg.strategy.poll_ms.max(250));
        tokio::spawn(async move {
            loop {
                if let Err(e) = bot.tick_entries().await {
                    warn!("tick_entries: {e:#}");
                }
                tokio::select! {
                    _ = poll_now.notified() => {}
                    _ = tokio::time::sleep(period) => {}
                }
            }
        });
    }

    // 2) продажа наград раз в ~минуту
    {
        let bot = Arc::clone(&bot);
        tokio::spawn(async move {
            loop {
                if let Err(e) = bot.sell_rewards().await {
                    warn!("sell_rewards: {e:#}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }

    // 3) инсинератор
    if inc_cfg.enabled {
        let inc = Incinerator::new(inc_cfg);
        let rpc = rpc.clone();
        let relays = relays.clone();
        let payer = Arc::clone(&payer);
        let dry = bot.cfg.strategy.dry_run;
        tokio::spawn(async move {
            loop {
                if let Err(e) = run_burn(&rpc, &relays, &payer, &inc, dry).await {
                    warn!("incinerator: {e:#}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(inc.interval_secs())).await;
            }
        });
    }

    // 4) шред-поток (Raiden Pulse): раунд по нашему стоку → мгновенный выход,
    //    активность reward-кошелька → немедленный опрос борды на вход.
    if bot.cfg.shreds.enabled {
        let shreds_cfg = bot.cfg.shreds.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<shreds::ShredEvent>(10_000);
        tokio::spawn(async move {
            if let Err(e) = shreds::run(shreds_cfg, tx).await {
                warn!("shreds: {e:#}");
            }
        });
        let consumer = Arc::clone(&bot);
        tokio::spawn(async move { consumer.consume_shreds(rx).await });
    } else {
        info!("shreds выключены — вход/выход по таймеру. Включи [shreds] для латенси-эджа.");
    }

    // 5) периодическая PnL-сводка в Telegram (ground truth — дельта баланса кошелька).
    {
        let every = bot.reporter.summary_every_secs();
        if every > 0 {
            let bot = Arc::clone(&bot);
            let owner = payer.pubkey();
            let start = rpc.lamports(&owner).await.unwrap_or(0);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(every)).await;
                    let now = bot.rpc.lamports(&owner).await.unwrap_or(start);
                    let net = (now as f64 - start as f64) / 1e9;
                    let price = bot.sol_price_usd().await;
                    bot.reporter.report_summary(Some(net), price).await;
                }
            });
        }
    }

    info!("feebot запущен. Ctrl+C для остановки.");
    tokio::signal::ctrl_c().await?;
    info!("останавливаюсь");
    Ok(())
}

/// Один проход инсинератора: закрыть пустые мемкоин-ATA, вернуть ренту.
async fn run_burn(
    rpc: &Rpc,
    relays: &RelaySet,
    payer: &Arc<solana_sdk::signature::Keypair>,
    inc: &Incinerator,
    dry_run: bool,
) -> Result<()> {
    let owner = payer.pubkey();
    let ixs = inc.plan(rpc, &owner).await?;
    if ixs.is_empty() {
        info!("инсинератор: нечего закрывать");
        return Ok(());
    }
    info!("инсинератор: к закрытию {} аккаунтов", ixs.len());

    // Чанкуем по 12 close-инструкций на транзакцию.
    for chunk in ixs.chunks(12) {
        let mut tx_ixs = compute_budget(60_000, 1_000).to_vec();
        tx_ixs.extend_from_slice(chunk);
        if dry_run {
            info!("[DRY] BURN: закрыл бы {} аккаунтов в одной tx", chunk.len());
            continue;
        }
        let bh = rpc.latest_blockhash().await?;
        let tx = build_signed(payer, &tx_ixs, bh, &no_alts(), &[])?;
        match relays.broadcast(&tx).await {
            Ok(sig) => info!("BURN {} аккаунтов: {sig}", chunk.len()),
            Err(e) => warn!("BURN send: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Ok(())
}
