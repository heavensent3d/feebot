//! Отправка транзакций через быстрые гейтвеи (Astralane/Jito/Nozomi/Temporal),
//! а не в общий mempool мейннета.
//!
//! Модель:
//!  • submit_primary — для ВХОДА: строим и шлём на ОДИН (первый) эндпоинт, ровно
//!    один раз. Веер на входе опасен: две копии с разными tip могут обе
//!    исполниться → двойная покупка. (Чтобы безопасно веерить и вход, нужен
//!    Lighthouse-ассерт «мой ATA ещё пуст» — см. заметку в strategy.rs.)
//!  • broadcast_build — для ВЫХОДА: веер на ВСЕ эндпоинты, у каждого свой tip.
//!    Выход идемпотентен (durable nonce + Lighthouse-гейт), приземлится одна.
//!  • broadcast — для уже подписанных tx (Jupiter/burn): веер тех же байт.
//!
//! Astralane: эндпоинт вида https://fr.gateway.astralane.io/iris?api-key=KEY
//! (ключ в URL), метод sendTransaction, tip — отдельной инструкцией на astra…-аккаунт.

use crate::config::RelayConfig;
use crate::txbuild::build_signed;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use rand::seq::SliceRandom;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::system_instruction;
use solana_sdk::transaction::VersionedTransaction;
use std::str::FromStr;
use tokio::task::JoinSet;
use tracing::warn;

/// Жёсткий минимум tip у Nozomi (ниже — транзакция молча дропается).
const NOZOMI_MIN_TIP: u64 = 1_000_000;

/// Jito tip accounts (mainnet).
const JITO_TIPS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Astralane tip accounts (из их docs, endpoints-and-configs). Любой подходит;
/// рандомизируем, чтобы не толкаться в один write-lock.
const ASTRALANE_TIPS: &[&str] = &[
    "astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF",
    "astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm",
    "astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk",
    "astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL",
    "astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK",
    "astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC",
    "astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk",
    "astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK",
    // «Recently Added» (регистр важен!) — свежие из endpoints-and-configs.
    "AstrA1ejL4UeXC2SBP4cpeEmtcFPZVLxx3XGKXyCW6to",
    "AsTra79FET4aCKWspPqeSFvjJNyp96SvAnrmyAxqg5b7",
    "AstrABAu8CBTyuPXpV4eSCJ5fePEPnxN8NqBaPKQ9fHR",
    "AsTRADtvb6tTmrsqULQ9Wji9PigDMjhfEMza6zkynEvV",
    "AsTRAEoyMofR3vUPpf9k68Gsfb6ymTZttEtsAbv8Bk4d",
    "AStrAJv2RN2hKCHxwUMtqmSxgdcNZbihCwc1mCSnG83W",
    "Astran35aiQUF57XZsmkWMtNCtXGLzs8upfiqXxth2bz",
    "AStRAnpi6kFrKypragExgeRoJ1QnKH7pbSjLAKQVWUum",
    "ASTRaoF93eYt73TYvwtsv6fMWHWbGmMUZfVZPo3CRU9C",
];

/// Nozomi (Temporal) tip accounts — из их docs. Мин. tip 0.001 SOL.
const NOZOMI_TIPS: &[&str] = &[
    "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
    "noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
    "noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
    "noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
    "noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
    "nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
    "nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
    "nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
    "noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
    "nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
    "nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
    "nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
    "nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
    "nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
    "nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
    "nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
    "nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb",
];

/// Подписанный выход под конкретный эндпоинт (durable-nonce спам без переподписи).
#[derive(Clone)]
pub struct CachedTx {
    index: usize,
    wire: String,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    /// Обычный sendTransaction (в т.ч. Constant-K staked-эндпоинт — он без tip).
    JsonRpc,
    Jito,
    Astralane,
    Nozomi,
}

#[derive(Clone)]
struct Endpoint {
    name: String,
    url: String,
    kind: Kind,
    tip_lamports: u64,
    tip_account: Option<Pubkey>,
    mev_protect: bool,
    auth: Option<String>,
}

#[derive(Clone)]
pub struct RelaySet {
    http: reqwest::Client,
    endpoints: Vec<Endpoint>,
}

impl RelaySet {
    /// `fallback_send_url` используется, только если в конфиге нет ни одного релея
    /// (тогда шлём простым sendTransaction в указанный RPC, без tip).
    pub fn new(fallback_send_url: String, relays: &[RelayConfig]) -> Self {
        let mut endpoints = Vec::new();
        for r in relays {
            let kind = match r.kind.as_str() {
                "jito" => Kind::Jito,
                "astralane" => Kind::Astralane,
                "nozomi" => Kind::Nozomi,
                // Constant-K staked-эндпоинт — обычный sendTransaction без tip.
                "constant-k" | "constantk" => Kind::JsonRpc,
                _ => Kind::JsonRpc,
            };
            let mut tip = r.tip_lamports;
            if kind == Kind::Nozomi && tip > 0 && tip < NOZOMI_MIN_TIP {
                warn!(
                    "релей {}: tip {} < минимума Nozomi {}, поднимаю (иначе tx молча дропнется)",
                    r.name, tip, NOZOMI_MIN_TIP
                );
                tip = NOZOMI_MIN_TIP;
            }
            endpoints.push(Endpoint {
                name: r.name.clone(),
                url: r.url.clone(),
                kind,
                tip_lamports: tip,
                tip_account: r.tip_account.as_deref().and_then(|s| Pubkey::from_str(s).ok()),
                mev_protect: r.mev_protect,
                auth: r.auth_header.clone(),
            });
        }
        if endpoints.is_empty() {
            endpoints.push(Endpoint {
                name: "fallback".into(),
                url: fallback_send_url,
                kind: Kind::JsonRpc,
                tip_lamports: 0,
                tip_account: None,
                mev_protect: false,
                auth: None,
            });
        }
        Self {
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(16)
                .build()
                .expect("reqwest client"),
            endpoints,
        }
    }

    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    pub fn endpoint_name(&self, i: usize) -> Option<&str> {
        self.endpoints.get(i).map(|e| e.name.as_str())
    }

    /// Отправить на КОНКРЕТНЫЙ эндпоинт по индексу (для bench/замеров).
    pub async fn submit_to(
        &self,
        i: usize,
        payer: &Keypair,
        base_ixs: &[Instruction],
        blockhash: Hash,
        alts: &[AddressLookupTableAccount],
        extra_signers: &[&Keypair],
    ) -> Result<String> {
        let ep = self.endpoints.get(i).ok_or_else(|| anyhow!("нет эндпоинта #{i}"))?;
        let tx = self.build_for(ep, payer, base_ixs, blockhash, alts, extra_signers)?;
        let wire = B64.encode(bincode::serialize(&tx)?);
        send_one(&self.http, ep, &wire).await
    }

    /// Подписать выход ОДИН РАЗ под каждый эндпоинт (для durable nonce):
    /// потом гоняем те же байты в `send_cached` без переподписи и без чтения RPC.
    pub fn build_cached(
        &self,
        payer: &Keypair,
        base_ixs: &[Instruction],
        blockhash: Hash,
        alts: &[AddressLookupTableAccount],
        extra_signers: &[&Keypair],
    ) -> Result<Vec<CachedTx>> {
        let mut out = Vec::with_capacity(self.endpoints.len());
        for (i, ep) in self.endpoints.iter().enumerate() {
            let tx = self.build_for(ep, payer, base_ixs, blockhash, alts, extra_signers)?;
            out.push(CachedTx {
                index: i,
                wire: B64.encode(bincode::serialize(&tx)?),
            });
        }
        Ok(out)
    }

    /// Разослать заранее подписанные (кэшированные) выходы. Возвращает первую sig.
    pub async fn send_cached(&self, cached: &[CachedTx]) -> Result<String> {
        let mut set = JoinSet::new();
        for c in cached {
            let Some(ep) = self.endpoints.get(c.index) else { continue };
            let http = self.http.clone();
            let ep = ep.clone();
            let wire = c.wire.clone();
            set.spawn(async move { send_one(&http, &ep, &wire).await });
        }
        first_ok(set).await
    }

    /// ВХОД: один эндпоинт (первый), ровно один раз.
    pub async fn submit_primary(
        &self,
        payer: &Keypair,
        base_ixs: &[Instruction],
        blockhash: Hash,
        alts: &[AddressLookupTableAccount],
        extra_signers: &[&Keypair],
    ) -> Result<String> {
        let ep = self.endpoints.first().ok_or_else(|| anyhow!("нет эндпоинтов"))?;
        let tx = self.build_for(ep, payer, base_ixs, blockhash, alts, extra_signers)?;
        let wire = B64.encode(bincode::serialize(&tx)?);
        send_one(&self.http, ep, &wire).await
    }

    /// ВЫХОД: веер на все эндпоинты, у каждого свой tip. Возвращает первую sig.
    pub async fn broadcast_build(
        &self,
        payer: &Keypair,
        base_ixs: &[Instruction],
        blockhash: Hash,
        alts: &[AddressLookupTableAccount],
        extra_signers: &[&Keypair],
    ) -> Result<String> {
        let mut set = JoinSet::new();
        for ep in &self.endpoints {
            let tx = self.build_for(ep, payer, base_ixs, blockhash, alts, extra_signers)?;
            let wire = B64.encode(bincode::serialize(&tx)?);
            let http = self.http.clone();
            let ep = ep.clone();
            set.spawn(async move { send_one(&http, &ep, &wire).await });
        }
        first_ok(set).await
    }

    /// Уже подписанная tx (Jupiter/burn): веерим те же байты.
    pub async fn broadcast(&self, tx: &VersionedTransaction) -> Result<String> {
        let wire = B64.encode(bincode::serialize(tx).map_err(|e| anyhow!("serialize tx: {e}"))?);
        let mut set = JoinSet::new();
        for ep in &self.endpoints {
            let http = self.http.clone();
            let ep = ep.clone();
            let wire = wire.clone();
            set.spawn(async move { send_one(&http, &ep, &wire).await });
        }
        first_ok(set).await
    }

    /// Собрать и подписать tx под конкретный эндпоинт (добавив его tip).
    fn build_for(
        &self,
        ep: &Endpoint,
        payer: &Keypair,
        base_ixs: &[Instruction],
        blockhash: Hash,
        alts: &[AddressLookupTableAccount],
        extra_signers: &[&Keypair],
    ) -> Result<VersionedTransaction> {
        use solana_sdk::signature::Signer;
        let mut ixs = base_ixs.to_vec();
        if let Some(dest) = self.tip_account_for(ep) {
            ixs.push(system_instruction::transfer(&payer.pubkey(), &dest, ep.tip_lamports));
        }
        build_signed(payer, &ixs, blockhash, alts, extra_signers)
    }

    fn tip_account_for(&self, ep: &Endpoint) -> Option<Pubkey> {
        if ep.tip_lamports == 0 {
            return None;
        }
        if let Some(a) = ep.tip_account {
            return Some(a);
        }
        let pool = match ep.kind {
            Kind::Jito => JITO_TIPS,
            Kind::Astralane => ASTRALANE_TIPS,
            Kind::Nozomi => NOZOMI_TIPS,
            Kind::JsonRpc => return None, // staked/обычному RPC tip ни к чему
        };
        pool.choose(&mut rand::thread_rng())
            .and_then(|s| Pubkey::from_str(s).ok())
    }
}

/// Возвращает ПЕРВЫЙ успешный ответ (не ждём медленные релеи), иначе последнюю ошибку.
async fn first_ok(mut set: JoinSet<Result<String>>) -> Result<String> {
    let mut last_err = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(sig)) => return Ok(sig),
            Ok(Err(e)) => last_err = Some(e),
            Err(e) => last_err = Some(anyhow!("join: {e}")),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("все релеи отказали")))
}

async fn send_one(http: &reqwest::Client, ep: &Endpoint, b64: &str) -> Result<String> {
    let body = match ep.kind {
        Kind::Jito => serde_json::json!({
            "jsonrpc":"2.0","id":1,"method":"sendBundle",
            "params":[[b64], {"encoding":"base64"}]
        }),
        Kind::Astralane => {
            // sendTransaction + опц. 3-й параметр {mevProtect:true}
            let mut params = serde_json::json!([
                b64, {"encoding":"base64","skipPreflight":true}
            ]);
            if ep.mev_protect {
                params.as_array_mut().unwrap().push(serde_json::json!({"mevProtect": true}));
            }
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":params})
        }
        // Nozomi и Constant-K/обычный RPC: простой sendTransaction.
        Kind::JsonRpc | Kind::Nozomi => serde_json::json!({
            "jsonrpc":"2.0","id":1,"method":"sendTransaction",
            "params":[b64, {
                "encoding":"base64","skipPreflight":true,
                "maxRetries":0,"preflightCommitment":"processed"
            }]
        }),
    };

    let mut req = http.post(&ep.url).json(&body);
    if let Some(h) = &ep.auth {
        if let Some((k, v)) = h.split_once(':') {
            req = req.header(k.trim(), v.trim());
        }
    }
    let resp = req.send().await.map_err(|e| anyhow!("{} send: {e}", ep.name))?;
    let status = resp.status();
    // Читаем тело как текст — фришные тиры на троттлинге отдают не-JSON (HTML/пусто),
    // и resp.json() падал опаковой ошибкой. Теперь — понятный текст.
    let text = resp.text().await.map_err(|e| anyhow!("{} body: {e}", ep.name))?;
    let v: serde_json::Value = serde_json::from_str(&text).map_err(|_| {
        let snip: String = text.chars().take(120).collect();
        anyhow!("{} не-JSON ответ (HTTP {}): {}", ep.name, status, snip)
    })?;
    if let Some(err) = v.get("error") {
        return Err(anyhow!("{} rpc error: {err}", ep.name));
    }
    v.get("result")
        .and_then(|r| r.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            // Показываем ТЕЛО ответа: «нет result» само по себе ничего не говорит,
            // а релей может отказать по своей причине (мало tip, не тот тариф,
            // троттлинг) и вернуть её в поле, которого мы не ждали.
            let snip: String = text.chars().take(200).collect();
            anyhow!("{} нет result (HTTP {}): {}", ep.name, status, snip)
        })
}
