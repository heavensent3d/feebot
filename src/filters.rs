//! Отбор монет под лоукап-стратегию. ЭТО основной рычаг оптимизации дельты:
//! меняя пороги здесь, двигаем EV. Пока — простые, читаемые правила.

use crate::config::FilterConfig;
use crate::oracle::Candidate;
use std::collections::HashSet;

pub struct Filters {
    cfg: FilterConfig,
    denylist: HashSet<String>,
    allowlist: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Входим.
    Take,
    /// Пропускаем, с причиной (для логов и будущей статистики).
    Skip(String),
}

impl Filters {
    pub fn new(cfg: FilterConfig) -> Self {
        let denylist = cfg.reward_mint_denylist.iter().cloned().collect();
        let allowlist = cfg.reward_mint_allowlist.iter().cloned().collect();
        Self {
            cfg,
            denylist,
            allowlist,
        }
    }

    /// Дешёвые фильтры (без RPC): denylist/allowlist, капа, объём, ликвидность,
    /// pending. Он-чейн флоат считаем ТОЛЬКО для прошедших прескрин.
    pub fn prescreen(&self, c: &Candidate) -> Verdict {
        let rm = c.reward_mint.to_string();
        if self.denylist.contains(&rm) {
            return Verdict::Skip("reward_mint в denylist".into());
        }
        if !self.allowlist.is_empty() && !self.allowlist.contains(&rm) {
            return Verdict::Skip("reward_mint не в allowlist".into());
        }

        // Рыночная капа: только лоукапы.
        if let Some(mc) = c.market_cap_usd {
            if mc > self.cfg.max_market_cap_usd {
                return Verdict::Skip(format!("mcap {mc:.0} > max"));
            }
            if mc < self.cfg.min_market_cap_usd {
                return Verdict::Skip(format!("mcap {mc:.0} < min"));
            }
        }

        // Объём за 24ч: без него фисы не копятся и раунд не придёт, а на тонком
        // объёме монету сольют, пока ждём. Главный фильтр безопасности.
        if let Some(v) = c.volume_24h_usd {
            if v < self.cfg.min_volume_24h_usd {
                return Verdict::Skip(format!("vol24h {v:.0} < min (сольют пока ждём)"));
            }
        }

        // Ликвидность: чтобы монету и награду реально можно было продать.
        if let Some(l) = c.liquidity_usd {
            if l < self.cfg.min_liquidity_usd {
                return Verdict::Skip(format!("liq {l:.0} < min"));
            }
        }

        // ── Гейты по rewards-API (профиль работающего бота) ──
        // Монета должна уже РЕАЛЬНО платить холдерам («беру от 0.4 раунды»).
        if let Some(d) = c.distributed_sol {
            if d < self.cfg.min_distributed_sol {
                return Verdict::Skip(format!(
                    "роздано {d:.3} SOL < min {:.3} (монета ещё не платит)",
                    self.cfg.min_distributed_sol
                ));
            }
        }
        // СВЕЖЕСТЬ РАЗДАЧ — критично. Монета может иметь отличную историю
        // (роздано много, холдеров мало), но если последняя раздача была часы
        // назад — она мертва, и раунда за наш холд не случится. Именно на этом
        // мы купили мешок, который 17 часов не платил.
        if let Some(age) = c.last_payout_age_secs {
            if age > self.cfg.max_last_payout_age_secs {
                return Verdict::Skip(format!(
                    "последняя раздача {} мин назад > max {} мин (монета не раздаёт)",
                    age / 60,
                    self.cfg.max_last_payout_age_secs / 60
                ));
            }
        }
        // Чем меньше холдеров делили выплату — тем больше НАША доля. Это сам эдж.
        if let Some(h) = c.last_paid_holders {
            if h > self.cfg.max_last_paid_holders {
                return Verdict::Skip(format!(
                    "холдеров в выплате {h} > max {} (доля будет мелкой)",
                    self.cfg.max_last_paid_holders
                ));
            }
        }
        // Накоплено холдерам — чтобы раунд был близко/жирный.
        if self.cfg.min_owed_sol > 0.0 {
            if let Some(o) = c.owed_sol {
                if o < self.cfg.min_owed_sol {
                    return Verdict::Skip(format!("накоплено {o:.4} SOL < min {:.4}", self.cfg.min_owed_sol));
                }
            }
        }
        // Моментум: не входим в обвал (иначе теряем на цене больше, чем дадут реворды).
        if self.cfg.max_drop_24h_pct < 0.0 {
            if let Some(ch) = c.change_24h_pct {
                if ch < self.cfg.max_drop_24h_pct {
                    return Verdict::Skip(format!(
                        "обвал {ch:.0}% за 24ч (хуже {:.0}%)",
                        self.cfg.max_drop_24h_pct
                    ));
                }
            }
        }

        // Накопленная доля: раунд должен быть близко, но ещё не случиться.
        if let Some(p) = c.pending_reward_sol {
            if p < self.cfg.min_pending_reward_sol {
                return Verdict::Skip(format!("pending {p:.3} SOL < min (раунд далеко)"));
            }
            if p > self.cfg.max_pending_reward_sol {
                return Verdict::Skip(format!("pending {p:.3} SOL > max (не успеем)"));
            }
        }

        Verdict::Take
    }

    /// Полная оценка: прескрин + EV-гейт. `float_usd_override` — реальный он-чейн
    /// флоат (из `holders.rs`); None → падаем на допущение `qualified_float_pct`.
    pub fn evaluate(
        &self,
        c: &Candidate,
        buy_sol: f64,
        sol_price_usd: f64,
        float_usd_override: Option<f64>,
    ) -> Verdict {
        if let v @ Verdict::Skip(_) = self.prescreen(c) {
            return v;
        }
        if let Some(ev) = self.expected_ev_sol(c, buy_sol, sol_price_usd, float_usd_override) {
            if ev < self.cfg.min_expected_ev_sol {
                return Verdict::Skip(format!("EV {ev:+.4} SOL < min {:.4}", self.cfg.min_expected_ev_sol));
            }
        }
        Verdict::Take
    }

    /// Оценка EV входа в SOL. None, если данных не хватает (нет капы/курса).
    ///
    ///   qualified_float_usd = реальный он-чейн флоат, либо market_cap × pct%
    ///   our_share           ≈ buy_value_usd / (buy_value_usd + qualified_float_usd)
    ///   reward_sol          ≈ round_payout_sol × rounds × our_share
    ///   ev_sol              ≈ reward_sol − buy_sol×drawdown − fees
    pub fn expected_ev_sol(
        &self,
        c: &Candidate,
        buy_sol: f64,
        sol_price_usd: f64,
        float_usd_override: Option<f64>,
    ) -> Option<f64> {
        let mcap = c.market_cap_usd?;
        if sol_price_usd <= 0.0 || mcap <= 0.0 {
            return None;
        }
        let buy_value_usd = buy_sol * sol_price_usd;
        // Реальный флоат из holders.rs, иначе допущение % от капы.
        let qualified_float_usd = float_usd_override
            .unwrap_or_else(|| mcap * self.cfg.qualified_float_pct / 100.0)
            .max(0.0);
        let our_share = buy_value_usd / (buy_value_usd + qualified_float_usd).max(1e-9);
        let reward_sol = self.cfg.round_payout_sol * self.cfg.assumed_rounds_per_hold * our_share;
        let drawdown_sol = buy_sol * self.cfg.coin_drawdown_frac;
        Some(reward_sol - drawdown_sol - self.cfg.round_fees_sol)
    }
}
