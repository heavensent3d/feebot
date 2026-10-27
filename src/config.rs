//! Конфиг бота (TOML). См. config.example.toml.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rpc: RpcConfig,
    pub wallet: WalletConfig,
    #[serde(default)]
    pub relays: Vec<RelayConfig>,
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub filters: FilterConfig,
    #[serde(default)]
    pub incinerator: IncineratorConfig,
    #[serde(default)]
    pub jupiter: JupiterConfig,
    #[serde(default)]
    pub otc: OtcConfig,
    #[serde(default)]
    pub shreds: ShredsConfig,
    #[serde(default)]
    pub telegram: TelegramConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    /// Основной HTTP RPC (лучше платный: Helius/Triton/QuickNode).
    pub http: String,
    /// WebSocket для подписок (logsSubscribe/accountSubscribe).
    pub ws: String,
    /// Отдельный «быстрый» RPC только для рассылки tx (может совпадать с http).
    #[serde(default)]
    pub send_http: Option<String>,
    #[serde(default = "default_commitment")]
    pub commitment: String,
}

fn default_commitment() -> String {
    "processed".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletConfig {
    /// Путь к keypair-файлу (как у solana-keygen). Ключ НИКОГДА не хранится в конфиге.
    pub keypair_path: String,
}

/// Куда ещё, помимо основного RPC, веерно слать транзакции.
#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    pub name: String,
    pub url: String,
    /// "jsonrpc" (обычный sendTransaction), "jito" (sendBundle) или "astralane".
    #[serde(default = "default_kind")]
    pub kind: String,
    /// Tip в lamports (для jito/astralane/nozomi и т.п.). 0 = без tip.
    #[serde(default)]
    pub tip_lamports: u64,
    /// Явный tip-аккаунт; если пусто — для jito/astralane берём случайный из
    /// зашитого списка подходящих адресов.
    #[serde(default)]
    pub tip_account: Option<String>,
    /// Только для kind="astralane": добавить 3-й параметр {mevProtect:true}.
    /// (альтернатива — &mev-protect=true прямо в URL.)
    #[serde(default)]
    pub mev_protect: bool,
    #[serde(default)]
    pub auth_header: Option<String>,
}

fn default_kind() -> String {
    "jsonrpc".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    /// Размер входа в SOL (бот из разбора: 1.5–4 SOL).
    pub buy_sol: f64,
    /// Доля от выкупленных токенов, которую продаём на выходе (1.0 = всё).
    #[serde(default = "one")]
    pub sell_fraction: f64,
    /// Priority fee (micro-lamports per CU) для buy.
    #[serde(default = "default_buy_cu_price")]
    pub buy_cu_price: u64,
    #[serde(default = "default_buy_cu_limit")]
    pub buy_cu_limit: u32,
    /// Priority fee для спам-sell (разобранный бот гонял 1 и 136000).
    #[serde(default = "default_sell_cu_price")]
    pub sell_cu_price: u64,
    #[serde(default = "default_sell_cu_limit")]
    pub sell_cu_limit: u32,
    /// Макс. проскальзывание в bps при входе/выходе.
    #[serde(default = "default_slippage")]
    pub slippage_bps: u64,
    /// Сколько слотов подряд спамить выход, прежде чем сдаться.
    #[serde(default = "default_spam_slots")]
    pub exit_spam_slots: u64,
    /// Период опроса состояния (мс).
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    /// Сухой прогон: строить и логировать транзакции, но НЕ отправлять.
    #[serde(default = "truev")]
    pub dry_run: bool,
    /// Макс. одновременно открытых позиций.
    #[serde(default = "default_max_positions")]
    pub max_concurrent_positions: usize,
    /// Durable-nonce аккаунт для долгоживущего спам-выхода (опц.).
    /// Если None — спамим на свежем blockhash (переподписываем каждые ~40 слотов).
    #[serde(default)]
    pub nonce_account: Option<String>,
    /// Ставить ли Lighthouse-гейт на выход (ждать, что раунд выдал сток).
    #[serde(default = "truev")]
    pub lighthouse_exit_gate: bool,
    /// Насколько (в базовых единицах стока) должен вырасти баланс reward-кошелька,
    /// чтобы считать, что раунд прошёл. Подбирается под конкретный сток.
    #[serde(default = "default_gate_eps")]
    pub exit_gate_epsilon: u64,
    /// Lighthouse-гейт на входе: «мой ATA монеты ещё пуст» → из N веерных копий
    /// исполнится ровно одна (защита от двойной покупки).
    #[serde(default = "truev")]
    pub entry_gate: bool,
    /// Веерить вход на все релеи (безопасно только при entry_gate=true).
    #[serde(default)]
    pub fanout_entry: bool,
    /// Путь к маркер-файлу «в позиции»: бот создаёт его, пока открыта хотя бы
    /// одна позиция, и удаляет, когда флэт. Внешний деплой-скрипт читает его,
    /// чтобы не рестартить бота посреди сделки. Путь относительно cwd бота.
    #[serde(default = "default_busy_marker")]
    pub busy_marker: String,
    /// Если раунд не пришёл за время холда — аварийно продать монету по рынку
    /// (БЕЗ гейта), чтобы не зависнуть на мешке. true по умолчанию.
    #[serde(default = "truev")]
    pub exit_bail_on_timeout: bool,
    /// ── Модель «холд до раунда» (раунды OTC атомарны, влезть в гэп нельзя —
    /// надо быть холдером ДО раунда и ловить раунды, пока держим) ──
    /// Макс. qualified-флоат монеты (USD), чтобы наша доля была большой. Над этим
    /// порогом вход невыгоден (доля мелкая). ~$400 по умолчанию (эдж = крошечный флоат).
    #[serde(default = "default_max_float_usd")]
    pub max_qualified_float_usd: f64,
    /// Окно (сек), в котором считаем раунды монеты, чтобы понять что она «живая»
    /// (раунд придёт, пока держим).
    #[serde(default = "default_round_window_secs")]
    pub round_window_secs: u64,
    /// Минимум раундов монеты в окне, чтобы в неё входить (активный раундер).
    #[serde(default = "default_min_rounds")]
    pub min_rounds_in_window: usize,
    /// Макс. время холда (сек) в ожидании раунда; истекло без реворда → бейл.
    #[serde(default = "default_hold_max_secs")]
    pub hold_max_secs: u64,
    /// Кулдаун (сек) на повторный вход в ту же монету после сделки.
    #[serde(default = "default_post_trade_cooldown")]
    pub post_trade_cooldown_secs: u64,
    /// ЦЕНОВОЙ ПОЛ продажи в bps от стоимости входа (10300 = продать не дешевле
    /// 103% затрат). Ниже пола tx просто падает (дёшево) — убыток НЕ фиксируем.
    /// Так работает бот, который зарабатывает; min_sol_out=0 (дамп) нас и сливал.
    #[serde(default = "default_sell_floor_bps")]
    pub sell_floor_bps: u64,
    /// Пол для АВАРИЙНОЙ продажи по таймауту (9000 = согласны максимум на −10%),
    /// чтобы не зависнуть на мешке навсегда, но и не дампить в ноль.
    #[serde(default = "default_bail_floor_bps")]
    pub bail_floor_bps: u64,
    /// Получили реворд и позиция в профите ≥ этого % → НЕ продаём, держим дальше
    /// до следующего раунда (риск отбит, апсайд — ещё реворды и рост монеты).
    #[serde(default = "default_hold_more_profit_pct")]
    pub hold_more_profit_pct: f64,
    /// Максимум ревордов за один холд (чтобы не держать бесконечно).
    #[serde(default = "default_max_rewards_per_hold")]
    pub max_rewards_per_hold: u32,
    /// ЗАЩИТА ПРИБЫЛИ. Если реворд уже получен и мы держим ради следующего, а
    /// профит просел до этого % (конкуренты дампят после раздачи) — выходим,
    /// не отдавая прибыль обратно. 2.0 = выйти в безубыток с запасом на комиссии.
    #[serde(default = "default_breakeven_exit_pct")]
    pub breakeven_exit_pct: f64,
    /// Пол продажи при выходе в безубыток (10000 = ровно стоимость входа).
    #[serde(default = "default_breakeven_floor_bps")]
    pub breakeven_floor_bps: u64,
    /// Как часто (в тиках опроса) считать профит во время холда — расчёт не
    /// бесплатный (чтение кривой + котировка), поэтому не каждый тик.
    #[serde(default = "default_profit_check_every")]
    pub profit_check_every_polls: u32,
    /// ── ГОНКА ВЫХОДА ──
    /// Выход — это гонка: конкуренты льют 300-500 падающих гейтованных продаж в
    /// секунду, чтобы исполниться в первом слоте после раздачи. Опоздал на пару
    /// секунд → −20% вместо −1.4%. Сколько ВЫСТРЕЛОВ/сек делаем мы.
    ///
    /// Выстрел веерится на ВСЕ эндпоинты сразу (send_cached), поэтому реальный
    /// поток = tps × число релеев, и столько же комиссий за него платится. Если
    /// релеи в разных регионах, их лимиты складываются — так и набирается rps.
    ///
    /// ⚠️ ЦЕНА ПАДЕНИЯ. tip внутри упавшей tx действительно НЕ списывается (стейт
    /// откатывается), но base fee И priority fee — списываются. priority считается
    /// по ЗАПРОШЕННОМУ лимиту: sell_cu_price × sell_cu_limit. При 136000 × 120000
    /// это 16320 лампортов, то есть попытка стоит 5000+16320 = 21320, а не 5000.
    /// Пролив на весь холд (480с × 50/с = 24000 попыток) сжёг бы 0.51 SOL —
    /// больше самой сделки. Поэтому пролив идёт ОКНОМ ПО ТРИГГЕРУ и ограничен
    /// жёстким бюджетом `exit_spam_max_sol`.
    ///
    /// ⚠️ СЕНДЕР. Значение обязано совпадать с лимитом sendTransaction у релея,
    /// иначе лишние выстрелы просто отбрасываются. И сендер не должен штрафовать
    /// за падения: пролив падает ~99.9% времени (один филл на сотни попыток).
    ///  • Nozomi НЕ ГОДИТСЯ: новый ключ = 5 запросов/с на регион, плюс QoS-штраф
    ///    по доле неприземлившихся («landed 10% → приоритет 10%») и прямой запрет
    ///    на клиентские ресенды. Гонка на нём убивает собственный приоритет.
    ///  • Нужен staked sendTransaction без tip (Constant-K Operator — 15 tx/с,
    ///    Nexus — 200 tx/с) или свой TPU-коннект. Конкуренты льют 300-500 tx/с.
    /// 0 = гонка выключена, выход идёт дешёвым опросом. Это безопасный дефолт,
    /// пока под гонку нет сендера.
    #[serde(default = "default_exit_spam_tps")]
    pub exit_spam_tps: u32,
    /// Как часто (сек) переподписывать пролив свежим blockhash (он живёт ~60-90с).
    #[serde(default = "default_exit_resign_secs")]
    pub exit_resign_secs: u64,
    /// Насколько полон котёл монеты, чтобы вообще входить: `waitingSol` в % от
    /// `floorSol` (раунд запускается при достижении порога).
    ///
    /// Это главный рычаг профита. Монета может быть идеальной по всем прочим
    /// признакам и всё равно принести убыток, если раунд не придёт в наш холд.
    /// Замер 25.10.2026 на двух кандидатах, прошедших старые фильтры: OTC раздал
    /// 0.866 SOL на 3 холдера (котёл 33.9%), BOOBS 1.398 SOL на 4 (котёл **0%**,
    /// раздал 25 мин назад и опустел) — во второй мы бы просто отсидели холд и
    /// вышли в минус. Прежние `min/max_pending_reward_sol` этого не ловили: они в
    /// абсолютных SOL, а `floorSol` у монет разный (видели 0.002 и 0.1), поэтому
    /// порог имеет смысл только как ДОЛЯ.
    #[serde(default = "default_min_vault_fill_pct")]
    pub min_vault_fill_pct: f64,
    /// НАСТОЯЩИЙ порог выплаты холдерам, в SOL. Поле `floorSol` из /api/vault им
    /// не является (у большинства монет там 0.002), а сам OTC в интерфейсе пишет
    /// «Fees stay here until they reach 0.10 SOL». Фактические выплаты это
    /// подтверждают: по 30 монетам минимальная 0.0899 SOL, медиана 0.1904.
    /// Используется как нижняя граница: берём max(этого значения, floorSol).
    #[serde(default = "default_payout_floor_sol")]
    pub payout_floor_sol: f64,
    /// Минимальная НАША доля в выплате, в процентах: `buy_usd / (флоат + buy_usd)`.
    ///
    /// Реворд делится пропорционально токенам, так что это и есть весь
    /// заработок. Прежний гейт был абсолютным (`max_qualified_float_usd`) и
    /// намерения не выражал: один и тот же порог $400 означал 13% доли на входе
    /// 0.3 SOL и 43% на 1.5 SOL. Порог в процентах масштабируется с позицией сам.
    ///
    /// Замеры: у разобранного бота на ALPHA свободный флоат был 13.8M против его
    /// 40.6M токенов — доля 74%, реворды 0.31-0.56 SOL. Наша убыточная Fortress
    /// дала 1.63% при трёх получателях и реворд 0.0039 SOL.
    #[serde(default = "default_min_reward_share_pct")]
    pub min_reward_share_pct: f64,
    /// Минимальный ОЖИДАЕМЫЙ фис за позицию, SOL. Считается целиком из чейна:
    ///     pot(creator_vault) × 0.675 × наша_доля
    /// Round-trip по монете стоит ~3% позиции (замер: 0.0092 на входе 0.3 SOL),
    /// плюс комиссии — ниже этого входить бессмысленно.
    #[serde(default = "default_min_expected_reward_sol")]
    pub min_expected_reward_sol: f64,
    /// Входить ли, когда флоат ИЗМЕРИТЬ НЕ УДАЛОСЬ (ошибка RPC).
    /// true = не входить. Вслепую берутся ровно те монеты, где чужой вес в
    /// десятки раз больше нашего, а узнать это постфактум уже нечем.
    #[serde(default = "default_require_float_measure")]
    pub require_float_measure: bool,
    /// Сколько котлов опрашивать ОДНОВРЕМЕННО в фоновом обходе.
    /// Замер 27.10.2026 на 120 монетах: последовательно 851 мс на монету (500
    /// монет = 426 секунд), x20 = 33 секунды, x40 = 25 секунд. Это обычный HTTP
    /// к борде, RPC здесь не участвует.
    #[serde(default = "default_pot_scan_concurrency")]
    pub pot_scan_concurrency: u32,
    /// Пауза между полными обходами котлов (сек).
    #[serde(default = "default_pot_scan_interval_secs")]
    pub pot_scan_interval_secs: u64,
    /// Жёсткий потолок трат на пролив за одну позицию. Исчерпали → НЕ бейлим, а
    /// падаем в дешёвый опрос (шанс поймать реворд остаётся, деньги не горят).
    #[serde(default = "default_exit_spam_max_sol")]
    pub exit_spam_max_sol: f64,
    /// Сколько секунд лить после триггера. У разобранного бота от клейма воркера
    /// до раздачи ~37с, конкуренты начинали лить за ~13с до неё.
    #[serde(default = "default_exit_spam_window_secs")]
    pub exit_spam_window_secs: u64,
    /// Как часто (мс) опрашивать котёл монеты в ожидании триггера (бесплатный HTTP).
    #[serde(default = "default_exit_trigger_poll_ms")]
    pub exit_trigger_poll_ms: u64,
    /// Насколько должен подскочить `waitingSol`, чтобы считать это клеймом воркера
    /// (прекурсор раздачи) — ДОЛЯ от `floorSol`, не абсолют. 0 → триггер выключен.
    ///
    /// Было абсолютное `exit_trigger_jump_sol = 0.005`, и на первой же живой
    /// позиции это оказалось мертво: у STOCKS `floorSol = 0.002`, то есть мы
    /// требовали скачок в 2.5 раза БОЛЬШЕ всей суммы, при которой раздача вообще
    /// происходит. Триггер не мог сработать никогда, пролив не начинался, и
    /// позиция ушла бы в бейл по таймауту. Порог котла у монет разный (видели
    /// 0.002 и 0.1), поэтому осмыслен только относительный.
    #[serde(default = "default_exit_trigger_jump_frac")]
    pub exit_trigger_jump_frac: f64,
}

fn default_exit_spam_tps() -> u32 {
    // 1 выстрел/с (x3 релея = 3 tx/с) — темп разобранного ПРИБЫЛЬНОГО бота.
    // Замер его кошелька 27.10.2026: 4000 транзакций за 24 минуты, 99.8% из них
    // упавшие, то есть ~2.8 приземлившихся падений в секунду.
    //
    // Это работает ТОЛЬКО вместе с sell_cu_price = 1. У него на продаже стоит
    // цена CU в единицу при лимите 807 010, поэтому упавшая гейтованная попытка
    // стоит ровно базовые 5001 лампорт. У нас при цене 136 000 она стоила 21 320,
    // и ровно поэтому прошлый вывод был «лить нельзя, разоримся». Он берёт не
    // ценой за место в блоке, а объёмом попыток: суточный пролив обходится ему
    // около 1 SOL против +4..+18 SOL прибыли в день.
    //
    // Наш прошлый пролив (1546 выстрелов x 3 релея = 4638 транз) дал на чейне
    // РОВНО НОЛЬ, и причину мы не видели, потому что глотали ошибку отправки.
    // Теперь она логируется — если снова ноль, смотреть туда.
    1
}
fn default_exit_resign_secs() -> u64 {
    40
}
fn default_exit_spam_max_sol() -> f64 {
    0.08
}
fn default_min_vault_fill_pct() -> f64 {
    50.0
}
fn default_payout_floor_sol() -> f64 {
    0.10
}
fn default_min_reward_share_pct() -> f64 {
    50.0
}
fn default_min_expected_reward_sol() -> f64 {
    0.05
}
fn default_require_float_measure() -> bool {
    true
}
fn default_pot_scan_concurrency() -> u32 {
    20
}
fn default_pot_scan_interval_secs() -> u64 {
    15
}
fn default_exit_spam_window_secs() -> u64 {
    60
}
fn default_exit_trigger_poll_ms() -> u64 {
    1200
}
fn default_exit_trigger_jump_frac() -> f64 {
    // 0 = ТРИГГЕРА НЕТ, пролив идёт с момента входа. Это не упрощение, а
    // исправление неверной посылки.
    //
    // Триггер ждал РОСТА `rewards.lastClaimAt`, считая клейм воркера прекурсором
    // раздачи. Замер 27.10.2026 показал обратное — клейм приходит ПОСЛЕ неё:
    //   ALPHA:    раздача 14:51:03, клейм 15:00:03  (+9 минут)
    //   Fortress: раздача 09:51:00, клейм 09:55:21  (+4 минуты)
    // Поэтому триггер не срабатывал никогда: на Fortress пролив сделал РОВНО
    // НОЛЬ попыток, бот просидел сквозь раздачу и вышел по таймауту через четыре
    // минуты после неё, собрав 1.63% выплаты вместо своей доли.
    //
    // Разобранный бот ничего не предсказывает: у него пролив идёт непрерывно
    // (16 800 попыток в час), и гейтованная продажа исполняется в тот момент,
    // когда реворд падает на ATA. Пролив И ЕСТЬ триггер.
    0.0
}

fn default_breakeven_exit_pct() -> f64 {
    2.0
}
fn default_breakeven_floor_bps() -> u64 {
    10_000
}
fn default_profit_check_every() -> u32 {
    20
}

fn default_hold_more_profit_pct() -> f64 {
    20.0
}
fn default_max_rewards_per_hold() -> u32 {
    3
}
fn default_sell_floor_bps() -> u64 {
    // СЛИППЕДЖ-ГАРД, а НЕ тейк-профит. Работающий бот спокойно фиксирует минус
    // по монете (-1.4%..-21.5% в замерах) — прибыль даёт реворд, а не цена.
    // Пол выше 100% означал бы, что продажа никогда не исполнится и мы зависнем.
    8_000
}
fn default_bail_floor_bps() -> u64 {
    // Аварийный предел (как у работающего бота ~50% от входа): защита от
    // катастрофы/рага, а не от обычного минуса.
    5_000
}

fn default_max_float_usd() -> f64 {
    400.0
}
fn default_min_distributed() -> f64 {
    0.4
}
fn default_max_paid_holders() -> u64 {
    4
}
fn default_min_owed() -> f64 {
    0.0
}
fn default_max_drop_24h() -> f64 {
    -60.0
}
fn default_max_drop() -> f64 {
    default_max_drop_24h()
}
fn default_max_payout_age() -> u64 {
    // 6 часов — ГРУБАЯ проверка «монета вообще жива», а не тайминг входа.
    //
    // Было 30 минут с мыслью «должна раздавать прямо сейчас», и это оказалось
    // логическим противоречием с min_vault_fill_pct: свежая раздача означает
    // ПУСТОЙ котёл, потому что она его и опустошила. Два гейта требовали
    // взаимоисключающего, и на живых данных не проходил НИКТО.
    //
    // Замер 25.10.2026: STOCKS 4Gzss.. — роздано 0.891 SOL на 2 холдера, котёл
    // 88%, всё прочее в допуске, но раздача была 5127с назад → отказ. Pairs
    // 2yrbE.. — 0.847 SOL на 2 холдера, котёл 74%, возраст 11865с → отказ. Их
    // котлы полны ИМЕННО потому, что раздача была давно.
    //
    // Тайминг входа теперь даёт заполнение котла — это прямой сигнал «раунд
    // близко», а возраст последней раздачи был лишь его косвенным прокси.
    21_600
}
fn default_round_window_secs() -> u64 {
    600
}
fn default_min_rounds() -> usize {
    // 0 = выключено: монету из шред-раунда опознать НЕЛЬЗЯ (раунд ссылается на
    // сток, а не на мемкоин), поэтому отбор идёт по волюму+флоату, а не по
    // наблюдённым раундам. Инфра трекинга оставлена на будущее.
    0
}
fn default_hold_max_secs() -> u64 {
    // 120 секунд, а не 480. Разобранный бот держит ~53 секунды: купил ALPHA в
    // 14:50:08, раздача 14:51:03, продал 14:51:01.
    //
    // Длинный холд не просто бесполезен — он ВРЕДЕН. Полный котёл виден всем,
    // и пока мы сидим, в монету набегают остальные и размывают долю. На
    // Fortress мы держали почти 8 минут и получили 1.63% выплаты при трёх
    // получателях (равная доля была бы 33%). Чем позже входим и чем короче
    // держим, тем меньше успеет набежать конкурентов.
    120
}
fn default_post_trade_cooldown() -> u64 {
    120
}

fn default_busy_marker() -> String {
    ".position_open".into()
}

fn default_gate_eps() -> u64 {
    1
}

fn one() -> f64 {
    1.0
}
fn truev() -> bool {
    true
}
fn default_buy_cu_price() -> u64 {
    5_000_000
}
// Вход: ATA мемкоина + ATA стока (idempotent) + pump buy. По факту ~70-90k CU,
// ставим 120k с запасом.
fn default_buy_cu_limit() -> u32 {
    120_000
}
fn default_sell_cu_price() -> u64 {
    136_000
}
// Выход: nonce advance + CU + 1-2 Lighthouse-ассерта + pump sell. По факту ~60-75k,
// ставим 90k с запасом.
fn default_sell_cu_limit() -> u32 {
    90_000
}
fn default_slippage() -> u64 {
    1500
}
fn default_spam_slots() -> u64 {
    40
}
fn default_poll_ms() -> u64 {
    400
}
fn default_max_positions() -> usize {
    1
}

/// Фильтры отбора монет — ЭТО то, что будем оптимизировать потом.
#[derive(Debug, Clone, Deserialize)]
pub struct FilterConfig {
    /// Брать только монеты с market cap ниже порога (USD). Лоукап-стратегия.
    #[serde(default = "default_max_mcap")]
    pub max_market_cap_usd: f64,
    /// Минимальный market cap (отсечь совсем мёртвые/скам).
    #[serde(default = "default_min_mcap")]
    pub min_market_cap_usd: f64,
    /// ── Фильтры по rewards-API OTC (профиль работающего бота) ──
    /// Монета должна УЖЕ раздать холдерам ≥ этого (SOL) — доказала, что платит.
    /// У danek'а это «беру от 0.4 раунды».
    #[serde(default = "default_min_distributed")]
    pub min_distributed_sol: f64,
    /// Максимум холдеров, деливших последнюю выплату. Меньше = наша доля больше.
    /// Главный источник эджа (у реальной сделки было 3).
    #[serde(default = "default_max_paid_holders")]
    pub max_last_paid_holders: u64,
    /// Минимум накопленного холдерам (SOL) — чтобы раунд был близко/жирный.
    #[serde(default = "default_min_owed")]
    pub min_owed_sol: f64,
    /// Не входить, если цена за 24ч упала сильнее (%, отрицательное). 0 = выключено.
    #[serde(default = "default_max_drop")]
    pub max_drop_24h_pct: f64,
    /// Максимальный возраст ПОСЛЕДНЕЙ раздачи (сек). Монета с отличной историей,
    /// но не раздававшая часы — мертва: раунда за наш холд не будет. Это главный
    /// фильтр «живости», без него покупаются мёртвые монеты.
    #[serde(default = "default_max_payout_age")]
    pub max_last_payout_age_secs: u64,
    /// Накопленная доля держателей (SOL) должна быть не ниже — раунд близко.
    #[serde(default = "default_min_pending")]
    pub min_pending_reward_sol: f64,
    /// ...и не выше (иначе раунд уже прошёл/вот-вот, можем не успеть войти).
    #[serde(default = "default_max_pending")]
    pub max_pending_reward_sol: f64,
    /// Минимальный ожидаемый EV входа в SOL (после комиссий), иначе скип.
    #[serde(default = "default_min_ev")]
    pub min_expected_ev_sol: f64,
    /// Допущение: какая доля market cap (%) — это «квалифицированный флоат»
    /// (кошельки ≥ $20, конкурирующие за раздачу). Кривая/пул/creator-vault из
    /// раздачи исключены, поэтому на свежем лоукапе реальный флоат мал → ставь
    /// меньше. Используется в оценке нашей доли. ТОЧНОЕ значение — из он-чейн
    /// анализа холдеров (следующий шаг); пока это knob-допущение.
    #[serde(default = "default_max_float")]
    pub qualified_float_pct: f64,
    /// Сколько SOL раздаётся за раунд (порог раунда = 0.1 SOL доли держателей).
    #[serde(default = "default_round_payout")]
    pub round_payout_sol: f64,
    /// Сколько раундов рассчитываем собрать за одно удержание.
    #[serde(default = "default_rounds")]
    pub assumed_rounds_per_hold: f64,
    /// Ожидаемая просадка на round-trip самой монеты (доля от buy_sol).
    #[serde(default = "default_drawdown")]
    pub coin_drawdown_frac: f64,
    /// Комиссии+tip за цикл входа/выхода (SOL).
    #[serde(default = "default_round_fees")]
    pub round_fees_sol: f64,
    /// Мин. объём за 24ч (USD): фисы копятся от объёма, значит раунд придёт быстро,
    /// и монету не сольют в ноль, пока ждём. Главный фильтр «чтобы не срали на ебало».
    #[serde(default = "default_min_vol")]
    pub min_volume_24h_usd: f64,
    /// Мин. ликвидность (USD): чтобы и монету, и награду реально можно было продать.
    #[serde(default = "default_min_liq")]
    pub min_liquidity_usd: f64,
    /// Чёрный список reward-активов (не фармить монеты, платящие этим).
    #[serde(default)]
    pub reward_mint_denylist: Vec<String>,
    /// Если непусто — брать только монеты, платящие этими reward-активами.
    #[serde(default)]
    pub reward_mint_allowlist: Vec<String>,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            max_market_cap_usd: default_max_mcap(),
            min_market_cap_usd: default_min_mcap(),
            min_pending_reward_sol: default_min_pending(),
            max_pending_reward_sol: default_max_pending(),
            min_expected_ev_sol: default_min_ev(),
            qualified_float_pct: default_max_float(),
            round_payout_sol: default_round_payout(),
            assumed_rounds_per_hold: default_rounds(),
            coin_drawdown_frac: default_drawdown(),
            round_fees_sol: default_round_fees(),
            min_volume_24h_usd: default_min_vol(),
            min_liquidity_usd: default_min_liq(),
            min_distributed_sol: default_min_distributed(),
            max_last_paid_holders: default_max_paid_holders(),
            min_owed_sol: default_min_owed(),
            max_drop_24h_pct: default_max_drop(),
            max_last_payout_age_secs: default_max_payout_age(),
            reward_mint_denylist: vec![],
            reward_mint_allowlist: vec![],
        }
    }
}

fn default_max_mcap() -> f64 {
    15_000.0
}
fn default_min_mcap() -> f64 {
    2_000.0
}
fn default_min_pending() -> f64 {
    0.06
}
fn default_max_pending() -> f64 {
    0.10
}
fn default_min_ev() -> f64 {
    0.0
}
fn default_max_float() -> f64 {
    20.0
}
fn default_round_payout() -> f64 {
    0.1
}
fn default_rounds() -> f64 {
    1.0
}
fn default_drawdown() -> f64 {
    0.08
}
fn default_round_fees() -> f64 {
    0.01
}
fn default_min_vol() -> f64 {
    4_000.0
}
fn default_min_liq() -> f64 {
    2_000.0
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncineratorConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Период уборки (секунды).
    #[serde(default = "default_burn_interval")]
    pub interval_secs: u64,
    /// Закрывать только аккаунты с нулевым балансом.
    #[serde(default = "truev")]
    pub only_zero_balance: bool,
    /// Доп. список mint'ов, которые НИКОГДА не трогать (помимо авто-детекта стоков).
    #[serde(default)]
    pub keep_mints: Vec<String>,
    /// Макс. аккаунтов за один проход (чтобы не упереться в лимит tx).
    #[serde(default = "default_burn_batch")]
    pub max_per_run: usize,
}

impl Default for IncineratorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_burn_interval(),
            only_zero_balance: true,
            keep_mints: vec![],
            max_per_run: default_burn_batch(),
        }
    }
}

fn default_burn_interval() -> u64 {
    1800
}
fn default_burn_batch() -> usize {
    20
}

#[derive(Debug, Clone, Deserialize)]
pub struct JupiterConfig {
    #[serde(default = "default_jup_quote")]
    pub quote_url: String,
    #[serde(default = "default_jup_swap")]
    pub swap_url: String,
    /// Проскальзывание при сбросе наград (стоки неликвидные — ставим шире).
    #[serde(default = "default_reward_slippage")]
    pub slippage_bps: u64,
    /// Минимальная стоимость награды в SOL, ниже которой не продаём (dust).
    #[serde(default = "default_min_reward")]
    pub min_reward_sol: f64,
}

impl Default for JupiterConfig {
    fn default() -> Self {
        Self {
            quote_url: default_jup_quote(),
            swap_url: default_jup_swap(),
            slippage_bps: default_reward_slippage(),
            min_reward_sol: default_min_reward(),
        }
    }
}

fn default_jup_quote() -> String {
    // ⚠️ quote-api.jup.ag/v6 МЁРТВ: с нашего VDS он не отвечает вовсе (HTTP 000),
    // из-за чего КАЖДАЯ продажа реворда падала с «нет котировки» и заработанное
    // просто лежало на кошельке. Проверено 25.10.2026: пойманный реворд стоил
    // 0.2086 SOL, а сделка числилась убыточной. Актуальный путь — swap/v1.
    "https://lite-api.jup.ag/swap/v1/quote".into()
}
fn default_jup_swap() -> String {
    "https://lite-api.jup.ag/swap/v1/swap".into()
}
fn default_reward_slippage() -> u64 {
    3000
}
fn default_min_reward() -> f64 {
    0.01
}

#[derive(Debug, Clone, Deserialize)]
pub struct OtcConfig {
    /// База OTC API (см. docs: /api/coins, /api/pairs).
    #[serde(default = "default_otc_base")]
    pub api_base: String,
    /// Какой оракул рынка/раундов использовать: "api" или "onchain".
    #[serde(default = "default_oracle")]
    pub oracle: String,
    /// Сколько страниц борды тянуть (на борде ~52k монет).
    #[serde(default = "default_pages")]
    pub pages: usize,
    /// Размер страницы борды (API отдаёт до ~60).
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// Сортировка борды. РЕАЛЬНО API принимает только `volume24h`, `marketCap` и
    /// `change24h`; любое другое значение он игнорирует и отдаёт свой ДЕФОЛТНЫЙ
    /// порядок — а это именно свежие монеты, и он нам и нужен.
    ///
    /// Замер 25.10.2026 на 300 монетах дефолтного порядка: 295 с mcap ≤ $4000,
    /// медиана $2889, холдеров 1–8. Это ровно критерии разобранного бота (mcap
    /// ≤$4K, крошечное число получателей), и именно такие монеты раздают по
    /// 0.15–1.4 SOL на 2–4 холдера. Для сравнения, `volume24h desc` даёт топ с
    /// mcap $60k+ и миллионами выплаченных холдеров — там наша доля ничтожна, а
    /// половина монет вообще ушла на AMM и не торгуется программой pump.
    #[serde(default = "default_otc_sort")]
    pub sort: String,
}

fn default_pages() -> usize {
    // 10 страниц, а не 3. Замер 25.10.2026: из 4 монет, готовых ко входу прямо
    // сейчас (котёл ≥50% порога), в первых трёх страницах лежала ТОЛЬКО ОДНА,
    // остальные три — на страницах 3, 4 и 6. То есть бот видел четверть потока.
    // Стоит это лишь HTTP: борда отдаётся без RPC, а котёл всё равно читается
    // только для кандидата, прошедшего прескрин.
    10
}
fn default_page_size() -> usize {
    50
}
fn default_otc_sort() -> String {
    "fresh".to_string() // не входит в принимаемые API → дефолтный порядок (свежие)
}

impl Default for OtcConfig {
    fn default() -> Self {
        Self {
            api_base: default_otc_base(),
            oracle: default_oracle(),
            pages: default_pages(),
            page_size: default_page_size(),
            sort: default_otc_sort(),
        }
    }
}

fn default_otc_base() -> String {
    "https://otcdesks.cash".into()
}
fn default_oracle() -> String {
    "api".into()
}

/// Чтение шредов через Raiden Pulse (decoded shreds over gRPC).
#[derive(Debug, Clone, Deserialize)]
pub struct ShredsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Региональный Pulse-эндпоинт (plaintext gRPC, http://). FRA/AMS/NYC.
    #[serde(default = "default_pulse")]
    pub endpoint: String,
    /// Имя фильтра (любое). По умолчанию "otc".
    #[serde(default = "default_filter_name")]
    pub filter_name: String,
    /// account_required: tx должна трогать ВСЕ эти аккаунты.
    #[serde(default)]
    pub account_required: Vec<String>,
    /// account_include: tx должна трогать хотя бы один. Если все три списка
    /// пусты — по умолчанию следим за reward-кошельком OTC.
    #[serde(default)]
    pub account_include: Vec<String>,
    #[serde(default)]
    pub account_exclude: Vec<String>,
}

impl Default for ShredsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_pulse(),
            filter_name: default_filter_name(),
            account_required: vec![],
            account_include: vec![],
            account_exclude: vec![],
        }
    }
}

fn default_pulse() -> String {
    "http://fra.pulse.raiden.wtf:16000".into()
}
fn default_filter_name() -> String {
    "otc".into()
}

/// Отчётность: подробный PnL по сделкам в Telegram + локальный JSONL-журнал.
#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Токен бота от @BotFather.
    #[serde(default)]
    pub bot_token: String,
    /// ID чата/канала, куда слать (можно свой личный chat_id).
    #[serde(default)]
    pub chat_id: String,
    /// Период сводки, секунды (0 = без периодической сводки).
    #[serde(default = "default_summary_secs")]
    pub summary_every_secs: u64,
    /// Локальный JSONL-журнал сделок (пусто = не писать файл).
    #[serde(default = "default_journal")]
    pub journal_path: String,
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token: String::new(),
            chat_id: String::new(),
            summary_every_secs: default_summary_secs(),
            journal_path: default_journal(),
        }
    }
}

fn default_summary_secs() -> u64 {
    3600
}
fn default_journal() -> String {
    "feebot-journal.jsonl".into()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let p = path.as_ref();
        let raw = std::fs::read_to_string(p)
            .with_context(|| format!("не читается конфиг {}", p.display()))?;
        let cfg: Config = toml::from_str(&raw).context("битый TOML в конфиге")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(self.strategy.buy_sol > 0.0, "strategy.buy_sol должен быть > 0");
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.strategy.sell_fraction),
            "strategy.sell_fraction в [0,1]"
        );
        Ok(())
    }
}
