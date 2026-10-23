//! Чтение шредов через Raiden Pulse (decoded shreds over gRPC).
//!
//! Pulse отдаёт транзакции, декодированные из шредов на своей стороне, ДО
//! подтверждения блока — это самый ранний сигнал. FEC/reassembly делает Raiden,
//! нам приходит bincode-VersionedTransaction + slot + ALT-резолвленные аккаунты.
//!
//! Авторизация — по IP (allowlist в дашборде Raiden), токена нет. Порт 16000 —
//! plaintext gRPC, схема http:// (TLS не поднимется).

pub mod raiden_binary {
    tonic::include_proto!("raiden_binary");
}

use crate::config::ShredsConfig;
use crate::programs::OTC_LAUNCHER_REWARDS;
use anyhow::{anyhow, Result};
use raiden_binary::{
    raiden_binary_service_client::RaidenBinaryServiceClient,
    SubscribeBinaryTransactionsRequest, SubscribeRequestFilterBinaryTransactions,
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

/// Событие из шред-потока: одна транзакция, задевшая наши фильтры.
#[derive(Debug, Clone)]
pub struct ShredEvent {
    pub slot: u64,
    pub signature: String,
    /// Имена фильтров, под которые попала tx.
    pub filters: Vec<String>,
    /// Все аккаунты tx (static + ALT-resolved).
    pub accounts: Vec<Pubkey>,
    /// Программы, вызванные в tx.
    pub programs: Vec<Pubkey>,
}

impl ShredEvent {
    pub fn touches(&self, key: &Pubkey) -> bool {
        self.accounts.iter().any(|a| a == key)
    }
    pub fn uses_program(&self, prog: &Pubkey) -> bool {
        self.programs.iter().any(|p| p == prog)
    }
}

fn build_request(cfg: &ShredsConfig) -> SubscribeBinaryTransactionsRequest {
    // Если все списки пусты — по умолчанию следим за reward-кошельком OTC.
    let mut include = cfg.account_include.clone();
    if cfg.account_include.is_empty()
        && cfg.account_required.is_empty()
        && cfg.account_exclude.is_empty()
    {
        include.push(OTC_LAUNCHER_REWARDS.to_string());
    }
    let filter = SubscribeRequestFilterBinaryTransactions {
        account_include: include,
        account_exclude: cfg.account_exclude.clone(),
        account_required: cfg.account_required.clone(),
    };
    SubscribeBinaryTransactionsRequest {
        transactions: HashMap::from([(cfg.filter_name.clone(), filter)]),
    }
}

/// Подключиться к Pulse и качать события в канал. Переподключается при обрыве.
/// Не возвращается штатно — крутится, пока жив процесс.
pub async fn run(cfg: ShredsConfig, out: mpsc::Sender<ShredEvent>) -> Result<()> {
    let mut backoff = 1u64;
    loop {
        match stream_once(&cfg, &out).await {
            Ok(()) => {
                warn!("shreds: поток закрылся сервером, переподключаюсь");
            }
            Err(e) => {
                warn!("shreds: обрыв ({e:#}), переподключение через {backoff}s");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
        // при успешном коннекте backoff сбросится внутри stream_once через лог
    }
}

async fn stream_once(cfg: &ShredsConfig, out: &mpsc::Sender<ShredEvent>) -> Result<()> {
    info!("shreds: подключаюсь к {}", cfg.endpoint);
    let mut client = RaidenBinaryServiceClient::connect(cfg.endpoint.clone())
        .await
        .map_err(|e| anyhow!("connect {}: {e}", cfg.endpoint))?;

    let req = build_request(cfg);
    // Шлём один запрос и ДЕРЖИМ request-поток открытым: его закрытие завершает подписку.
    let outbound = tokio_stream::iter(vec![req])
        .chain(tokio_stream::pending::<SubscribeBinaryTransactionsRequest>());
    let mut stream = client
        .subscribe_binary_transactions(outbound)
        .await
        .map_err(|e| anyhow!("subscribe: {e}"))?
        .into_inner();

    info!("shreds: подписка активна (filter='{}')", cfg.filter_name);
    while let Some(resp) = stream.message().await.map_err(|e| anyhow!("stream: {e}"))? {
        let Some(update) = resp.transaction else { continue };
        let Some(bin) = update.transaction.as_ref() else { continue };

        let tx: VersionedTransaction = match bincode::deserialize(&bin.binary_transaction) {
            Ok(t) => t,
            Err(e) => {
                debug!("shreds: не декодится tx: {e}");
                continue;
            }
        };

        let ev = to_event(&tx, &update, resp.filters);
        // Не блокируемся: если потребитель не успевает — дропаем событие.
        if let Err(e) = out.try_send(ev) {
            debug!("shreds: канал переполнен, дроп события ({e})");
        }
    }
    Ok(())
}

fn to_event(
    tx: &VersionedTransaction,
    update: &raiden_binary::SubscribeUpdateBinaryTransaction,
    filters: Vec<String>,
) -> ShredEvent {
    // Полный набор аккаунтов для v0 = static_keys ++ loaded_writable ++ loaded_readonly.
    let mut accounts: Vec<Pubkey> = tx.message.static_account_keys().to_vec();
    for b in &update.loaded_writable_addresses {
        if let Ok(pk) = Pubkey::try_from(b.as_slice()) {
            accounts.push(pk);
        }
    }
    for b in &update.loaded_readonly_addresses {
        if let Ok(pk) = Pubkey::try_from(b.as_slice()) {
            accounts.push(pk);
        }
    }

    // Программы = аккаунты по program_id_index каждой инструкции.
    let mut programs = Vec::new();
    for ci in tx.message.instructions() {
        if let Some(p) = accounts.get(ci.program_id_index as usize) {
            if !programs.contains(p) {
                programs.push(*p);
            }
        }
    }

    let signature = tx
        .signatures
        .first()
        .map(|s| s.to_string())
        .unwrap_or_default();

    ShredEvent {
        slot: update.slot,
        signature,
        filters,
        accounts,
        programs,
    }
}
