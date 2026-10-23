# Деплой на VDS (Linux)

Регион VDS = регион Raiden Pulse = регион сендеров. Для EU — **Frankfurt** (или Amsterdam).
Нужен **статический IP** (Raiden Pulse авторизует по IP).

## 1. Зависимости (Debian/Ubuntu)
```bash
sudo apt update && sudo apt install -y build-essential pkg-config libssl-dev git
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
```
(protoc НЕ нужен — proto компилит protox на чистом Rust.)

## 2. Код на сервер
Вариант А — git (без приватника!):
```bash
git clone <твой-репозиторий> feebot && cd feebot
```
Вариант Б — scp всей папки, но БЕЗ config.toml и ключа (их отдельно, ниже).

## 3. Сборка
```bash
cargo build --release
```
Бинарь: `target/release/feebot`.

## 4. Ключ кошелька (ОТДЕЛЬНО, не через git)
С Windows-машины (PowerShell/терминал):
```bash
scp "C:\Users\sigangel\.config\solana\feebot-hvn96.json" user@VDS_IP:~/.config/solana/feebot.json
```
На сервере закрой права:
```bash
mkdir -p ~/.config/solana && chmod 600 ~/.config/solana/feebot.json
```

## 5. Конфиг
Скопируй `config.toml` на сервер (scp) и поправь ПУТИ под Linux:
```toml
[wallet]
keypair_path = "/home/USER/.config/solana/feebot.json"
```
- `[rpc] http/ws` — твой RPC (с триала/платный), желательно во Франкфурте.
- `[[relays]]` — Nozomi/Astralane/Constant-K с реальными ключами.
- `[shreds] enabled = true`, `endpoint = "http://fra.pulse.raiden.wtf:16000"`, и **впиши IP этого VDS в allowlist Raiden** (дашборд).
- `[telegram]` — bot_token/chat_id.
- `dry_run = true` на первый запуск.

## 6. Порядок первого часа
```bash
# 6.1 dry — проверить, что видит борду, шреды, релеи принимают (ничего не тратит)
./target/release/feebot candidates
./target/release/feebot shreds        # Ctrl+C после пары событий
RUST_LOG=info ./target/release/feebot run   # посмотреть логи вход/выход в dry

# 6.2 телега
./target/release/feebot test-telegram

# 6.3 durable nonce (один раз, для sign-once выхода) — это РЕАЛЬНАЯ tx
# (сначала выключи dry_run в конфиге или оставь: init-nonce шлёт сам)
./target/release/feebot init-nonce     # впиши выданный nonce_account в config

# 6.4 бенч релеев С ЭТОГО VDS (тратит ~0.003 SOL) — тут латенси настоящая
./target/release/feebot bench --iters 5

# 6.5 боевой: dry_run = false, МАЛЫЙ buy_sol (0.05–0.1), запускаем под tmux
tmux new -s feebot
RUST_LOG=info ./target/release/feebot run
# отцепиться: Ctrl+b, затем d ; вернуться: tmux attach -t feebot
```

Смотри PnL в Telegram (per-trade + сводка). Первая реальная сделка подтвердит
pump-путь (buy_exact_sol_in + Token-2022 по шаблону). Только после 1–2 успешных
сделок увеличивай `buy_sol`.

## 7. Как сервис (чтобы жил после отключения SSH) — опц.
`/etc/systemd/system/feebot.service`:
```ini
[Unit]
Description=feebot
After=network-online.target

[Service]
User=USER
WorkingDirectory=/home/USER/feebot
Environment=RUST_LOG=info
ExecStart=/home/USER/feebot/target/release/feebot -c config.toml run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl daemon-reload && sudo systemctl enable --now feebot
journalctl -u feebot -f      # логи
```

## Чеклист перед `dry_run = false`
- [ ] IP VDS в allowlist Raiden, `feebot shreds` реально ловит события.
- [ ] Релеи принимают (`feebot bench` зелёный с этого VDS).
- [ ] `feebot candidates` показывает монеты и EV (борда живая).
- [ ] `init-nonce` сделан, `nonce_account` в конфиге (sign-once выход).
- [ ] `buy_sol` маленький (0.05–0.1), `sell_fraction = 1.0`.
- [ ] Telegram шлёт.
- [ ] На кошельке есть запас на tip/ренту/спам (не только сумма входов).
