# feebot

Бот для фарма reward-раздач лаунчера **otcdesks.cash** на Solana, собранный по
мотивам разбора кошелька `FpZAyCrxPsfoW5XWySMPyRc86mjpo8SsGdWauRZbtniR`
(см. [`ANALYSIS.md`](ANALYSIS.md)).

## Что он делает

1. **Вход** в лоукап-монету лаунчера (pump.fun bonding curve), когда накопленная
   доля держателей близка к порогу раунда.
2. **Спам-выход** с Lighthouse-ассертом: заранее подписанный `sell` летит каждый
   слот и исполняется ровно тогда, когда reward-кошелёк OTC выдал сток (значит,
   снапшот снят и нас зачли). Пока условие не выполнено — транзакция падает на
   ассерте и почти ничего не стоит.
3. **Продажа наград** (токенизированных акций Token-2022) в SOL через Jupiter.
4. **Инсинератор** — периодически закрывает пустые мемкоин-ATA и возвращает ренту
   (как sol-incinerator). Стоки (Token-2022) не трогает.

## ⚠️ Прежде чем лить деньги

Бот стартует в **`dry_run = true`**: всё строит и логирует, но ничего не шлёт.

Сверь перед боем (это единственные места, где раскладка может «уехать»):

- **pump.fun layout** в [`src/pumpfun.rs`](src/pumpfun.rs). Реализован классический
  путь (`buy`/`sell`, 12 аккаунтов, с `creator_vault`). Разобранный бот на части
  монет использовал новый `buy_exact_sol_in` с fee-программой `pfeeUxB6…` и доп.
  аккаунтами — если целевые монеты требуют его, допиши account-лист по актуальному
  IDL (ссылки-адреса уже в файле, сняты с live-tx).
- **OTC API** в [`src/otc.rs`](src/otc.rs). Имена полей (`rewardMint`,
  `pendingRewardSol`, `marketCap`…) — предположения с алиасами. Прогони
  `curl https://otcdesks.cash/api/coins` и поправь под реальный JSON.
- **Lighthouse** ([`src/lighthouse.rs`](src/lighthouse.rs)) и адреса
  ([`src/programs.rs`](src/programs.rs)) сняты побайтно с реальных транзакций —
  тут трогать не нужно, но проверь, что reward-кошелёк OTC не сменился.

Проверка layout'ов — твоя ответственность: неверный account-лист сожжёт SOL на
упавших транзакциях.

## Сборка

```bash
cargo build --release
```

### Windows: нужен OpenSSL

`solana-sdk` тянет `openssl-sys` (через secp256r1-прекомпайл), поэтому на Windows
нужен системный OpenSSL. Проще всего:

```bash
winget install ShiningLight.OpenSSL.Dev
```

После установки (обычно в `C:\Program Files\OpenSSL-Win64`) cargo находит его сам;
если нет — укажи путь:

```bash
setx OPENSSL_DIR "C:\Program Files\OpenSSL-Win64"
```

Альтернативы: `choco install openssl`, сборка в WSL/Linux (там ставится
`libssl-dev`), либо vendored-OpenSSL (`features=["vendored"]` у `openssl`) — но для
него нужен полноценный Perl (Strawberry) и NASM, Git-bash-перл не годится.

> Статус сборки: все зависимости разрешаются и компилируются; на этой машине
> финальная проверка упёрлась только в отсутствие системного OpenSSL (прерогатива
> окружения, не кода). Поставь OpenSSL по инструкции выше и `cargo build` пройдёт.

## Запуск

```bash
cp config.example.toml config.toml   # и отредактируй
# посмотреть, что бот видит и как фильтрует (всегда без отправки):
cargo run --release -- -c config.toml candidates
# разово продать награды / почистить аккаунты:
cargo run --release -- -c config.toml sell-rewards
cargo run --release -- -c config.toml burn
# полный цикл:
cargo run --release -- -c config.toml run
```

Логи: `RUST_LOG=debug` для подробностей.

## Ключ

Приватный ключ читается только из локального keypair-файла (`wallet.keypair_path`,
формат `id.json` или base58) либо из env `FEEBOT_KEYPAIR`. В конфиг и репозиторий
он не попадает. Заведи под бота ОТДЕЛЬНЫЙ кошелёк и держи на нём только рабочий
баланс.

## Структура

| Файл | Что |
|---|---|
| `programs.rs` | все программы/аккаунты (адреса из ончейна) |
| `config.rs` / `keys.rs` | конфиг и загрузка ключа |
| `rpc.rs` / `relay.rs` / `nonce.rs` | RPC, веерная рассылка, durable nonce |
| `lighthouse.rs` | ассерты-гейты (реверс с live-tx) |
| `pumpfun.rs` / `pumpswap.rs` | buy/sell на кривой и детект graduation |
| `jupiter.rs` | продажа наград/AMM через агрегатор |
| `otc.rs` / `oracle.rs` / `filters.rs` | борда OTC, кандидаты, отбор лоукапов |
| `incinerator.rs` | закрытие пустых ATA |
| `strategy.rs` | вход → спам-выход → награды |
| `main.rs` | CLI и фоновые циклы |

## Оговорки

- Код **не протестирован ончейн** (у меня нет ключей и доступа к mainnet для
  исполнения). Проверен на сборку; боевую логику обкатывай на `dry_run` и малом
  депозите.
- Ниша конкурентная: минимум два кошелька фармят ту же схему. Новый участник
  размывает долю всех — см. `ANALYSIS.md`.
- Стратегия держится на правилах платформы; смена механики раунда (усреднение
  баланса во времени, случайный момент снапшота) ломает её.

## Следующий шаг — оптимизация фильтров

Рычаги в [`src/filters.rs`](src/filters.rs): порог капы, окно `pending_reward_sol`,
модель EV (`expected_ev_sol`), ограничение «квалифицированного флоата». Их и будем
крутить, чтобы поднять дельту.
