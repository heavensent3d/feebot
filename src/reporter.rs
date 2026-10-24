//! Отчётность по сделкам: подробный PnL со всеми расходами (фисами) в Telegram
//! + локальный JSONL-журнал для последующей аналитики.
//!
//! Эндпоинт Telegram (сверено с core.telegram.org/bots/api):
//!   POST https://api.telegram.org/bot<token>/sendMessage, тело JSON
//!   {chat_id, text (≤4096 симв.), parse_mode:"HTML"}. Ответ с полем ok.
//! Шлём только per-trade + периодическую сводку — намного ниже лимитов Telegram.
//!
//! Про точность: суммы монеты/награды берём по факту, но разбивка по фисам
//! (base/priority/tip/спам) частью оценочная — ГРАУНД-ТРУТ даёт сводка по
//! изменению баланса кошелька. Это честно помечено в сообщениях.

use crate::config::TelegramConfig;
use serde::Serialize;
use std::sync::Mutex;
use tracing::{info, warn};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// Расходы по сделке (в лампортах).
#[derive(Debug, Clone, Default, Serialize)]
pub struct Fees {
    /// Базовая комиссия подписи (5000 × число приземлившихся tx).
    pub base_lamports: u64,
    /// Priority fee (cu_price × cu).
    pub priority_lamports: u64,
    /// Чаевые релеям (платятся только за приземлившуюся tx).
    pub tips_lamports: u64,
    /// Число спам-попыток выхода (упавшие на гейте).
    pub spam_attempts: u64,
    /// Оценка стоимости спама (приземлившиеся-и-упавшие × base).
    pub spam_cost_lamports: u64,
    /// Рента ATA (монеты + стока), ВОЗВРАТНАЯ при закрытии.
    pub ata_rent_lamports: u64,
}

impl Fees {
    pub fn total_nonrefundable(&self) -> u64 {
        self.base_lamports + self.priority_lamports + self.tips_lamports + self.spam_cost_lamports
    }
}

/// Полный отчёт по закрытой сделке.
#[derive(Debug, Clone, Serialize)]
pub struct TradeReport {
    pub mint: String,
    pub symbol: String,
    pub reward_symbol: String,
    /// Потрачено SOL на вход.
    pub buy_sol: f64,
    /// Выручка SOL с продажи монеты (по факту/оценке).
    pub coin_sell_sol: f64,
    /// Награда: сколько стока зачли.
    pub reward_amount: f64,
    /// Оценка стоимости награды в SOL (по котировке на момент закрытия).
    pub reward_value_sol: f64,
    pub fees: Fees,
    /// Итоговый net по сделке (SOL) и в USD.
    pub net_sol: f64,
    pub net_usd: f64,
    pub closed_at: u64,
}

#[derive(Default)]
struct Agg {
    trades: u64,
    wins: u64,
    gross_reward_sol: f64,
    coin_leg_sol: f64,
    fees_sol: f64,
    net_sol: f64,
}

pub struct Reporter {
    http: reqwest::Client,
    cfg: TelegramConfig,
    agg: Mutex<Agg>,
}

impl Reporter {
    pub fn new(cfg: TelegramConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            cfg,
            agg: Mutex::new(Agg::default()),
        }
    }

    /// Отчёт по закрытой сделке: лог + JSONL + Telegram.
    pub async fn report_trade(&self, r: &TradeReport) {
        {
            let mut a = self.agg.lock().unwrap();
            a.trades += 1;
            if r.net_sol > 0.0 {
                a.wins += 1;
            }
            a.gross_reward_sol += r.reward_value_sol;
            a.coin_leg_sol += r.coin_sell_sol - r.buy_sol;
            a.fees_sol += r.fees.total_nonrefundable() as f64 / LAMPORTS_PER_SOL;
            a.net_sol += r.net_sol;
        }
        self.append_journal(r);
        info!(
            "PnL {} net {:+.4} SOL (coin {:+.4}, reward {:+.4})",
            r.symbol,
            r.net_sol,
            r.coin_sell_sol - r.buy_sol,
            r.reward_value_sol
        );
        let text = format_trade(r);
        self.send_telegram(&text).await;
    }

    /// Периодическая сводка. `wallet_net_sol` — ground-truth net по балансу.
    pub async fn report_summary(&self, wallet_net_sol: Option<f64>, sol_price: f64) {
        // Снимаем значения в локали и ОСВОБОЖДАЕМ гард до await (guard не Send).
        let (trades, wins, gross, coin_leg, fees, net) = {
            let a = self.agg.lock().unwrap();
            (a.trades, a.wins, a.gross_reward_sol, a.coin_leg_sol, a.fees_sol, a.net_sol)
        };
        let wr = if trades > 0 {
            100.0 * wins as f64 / trades as f64
        } else {
            0.0
        };
        let gt = wallet_net_sol
            .map(|n| format!("{:+.4} SOL ({:+.2}$)", n, n * sol_price))
            .unwrap_or_else(|| "—".into());
        let text = format!(
            "📈 <b>Сводка</b>\nСделок: {trades} (винрейт {wr:.0}%)\nДоход с наград: {gross:+.4} SOL\nНога монет: {coin_leg:+.4} SOL\nФисы+типы+спам: {:+.4} SOL\nСумма net по сделкам: {net:+.4} SOL\n<b>Net по балансу (ground truth): {gt}</b>",
            -fees
        );
        info!("summary: {}", text.replace('\n', " | "));
        self.send_telegram(&text).await;
    }

    pub fn summary_every_secs(&self) -> u64 {
        self.cfg.summary_every_secs
    }

    /// Разовое тестовое сообщение — проверить, что Telegram подключён.
    pub async fn test_message(&self) {
        if !self.cfg.enabled || self.cfg.bot_token.is_empty() || self.cfg.chat_id.is_empty() {
            warn!("telegram выключен или нет bot_token/chat_id — нечего тестить");
            return;
        }
        self.send_telegram("✅ <b>feebot</b>: Telegram подключён. PnL-отчёты будут приходить сюда.")
            .await;
        info!("тестовое сообщение отправлено в Telegram");
    }

    /// Уведомление о старте/рестарте бота (режим, кошелёк, баланс, размер входа).
    pub async fn report_startup(&self, wallet: &str, balance_sol: f64, dry_run: bool, buy_sol: f64) {
        let mode = if dry_run {
            "🧪 DRY-RUN (транзакции НЕ шлёт)"
        } else {
            "🟢 LIVE (реальные сделки)"
        };
        let text = format!(
            "♻️ <b>feebot запущен</b>\nРежим: {mode}\nКошелёк: <code>{wallet}</code>\nБаланс: {balance_sol:.4} SOL\nРазмер входа: {buy_sol} SOL"
        );
        self.send_telegram(&text).await;
    }

    fn append_journal(&self, r: &TradeReport) {
        if self.cfg.journal_path.is_empty() {
            return;
        }
        if let Ok(line) = serde_json::to_string(r) {
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.cfg.journal_path)
            {
                let _ = writeln!(f, "{line}");
            }
        }
    }

    async fn send_telegram(&self, text: &str) {
        if !self.cfg.enabled || self.cfg.bot_token.is_empty() || self.cfg.chat_id.is_empty() {
            return;
        }
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.cfg.bot_token);
        // Текст режем до лимита Telegram (4096 символов).
        let mut t = text.to_string();
        if t.chars().count() > 4096 {
            t = t.chars().take(4090).collect::<String>() + "…";
        }
        let body = serde_json::json!({
            "chat_id": self.cfg.chat_id,
            "text": t,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        match self.http.post(&url).json(&body).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("telegram: HTTP {}", resp.status());
                }
            }
            Err(e) => warn!("telegram send: {e}"),
        }
    }
}

/// HTML-эскейпинг для Telegram parse_mode=HTML.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn lsol(l: u64) -> f64 {
    l as f64 / LAMPORTS_PER_SOL
}

fn format_trade(r: &TradeReport) -> String {
    let f = &r.fees;
    let coin_leg = r.coin_sell_sol - r.buy_sol;
    format!(
        "📊 <b>Сделка закрыта: {}</b> (<code>{}</code>)\n\
         Монета: куплено {:.4} → продано {:.4} = <b>{:+.4}</b> SOL\n\
         Награда: {:.4} {} ≈ <b>{:+.4}</b> SOL (по котировке)\n\
         ── Расходы (фисы) ──\n\
         base: {:.6} SOL\n\
         priority: {:.6} SOL\n\
         tip релеев: {:.6} SOL\n\
         спам-выход: {:.6} SOL (~{} попыток)\n\
         рента ATA: {:.6} SOL (возвратная)\n\
         ── Итого ──\n\
         <b>Net: {:+.4} SOL ({:+.2}$)</b>",
        esc(&r.symbol),
        esc(&r.mint),
        r.buy_sol,
        r.coin_sell_sol,
        coin_leg,
        r.reward_amount,
        esc(&r.reward_symbol),
        r.reward_value_sol,
        lsol(f.base_lamports),
        lsol(f.priority_lamports),
        lsol(f.tips_lamports),
        lsol(f.spam_cost_lamports),
        f.spam_attempts,
        lsol(f.ata_rent_lamports),
        r.net_sol,
        r.net_usd,
    )
}
