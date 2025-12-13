use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use bitcoin::ScriptBuf;
use btraversal::{ScriptBufExt, ScriptType};
use kernel::{
    ChainType, ChainstateManager, ContextBuilder,
    core::{ScriptPubkeyExt, TransactionExt, TxInExt, TxOutExt, TxOutPointExt, TxidExt},
};
use rayon::prelude::*;

const CHAIN: ChainType = ChainType::Mainnet;
const DATA_DIR: &str = "/data1";
const BLOCKS_DIR: &str = "/data1/blocks/";
const RESULTS: &str = "/data1/ttls.sqlite";
const BITCOIN_DATA_ENV: &str = "BITCOIN_DIR";
const BLOCKS_DIR_ENV: &str = "BLOCKS_DIR";
const RESULTS_TABLE_ENV: &str = "RESULTS_TABLE";

#[derive(Debug)]
struct Record {
    txid: [u8; 32],
    vout: u32,
    script_type: ScriptType,
    amount: i64,
    created_height: u32,
    spend_height: Option<u32>,
}

fn open_database(path: impl AsRef<Path>) -> sql::Connection {
    let conn = sql::Connection::open(path).unwrap();
    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS utxo (
            txid           BLOB    NOT NULL,
            vout           INTEGER NOT NULL,
            script         INTEGER NOT NULL,
            amount         INTEGER NOT NULL,
            created_height INTEGER NOT NULL,
            spend_height   INTEGER,
            PRIMARY KEY (txid, vout) ON CONFLICT IGNORE
        )
        ",
        [],
    )
    .unwrap();
    conn
}

fn insert_output<'a>(conn: &'a mut sql::Connection, records: impl Iterator<Item = &'a Record>) {
    let tx = conn.transaction().unwrap();
    let mut stmt = tx
        .prepare(
            "
        INSERT INTO utxo (txid, vout, script, amount, created_height, spend_height)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ",
        )
        .unwrap();
    for record in records {
        stmt.execute(sql::params![
            record.txid,
            record.vout,
            record.script_type,
            record.amount,
            record.created_height,
            record.spend_height
        ])
        .unwrap();
    }
    drop(stmt);
    tx.commit().unwrap();
}

fn update_spend_height<'a>(
    conn: &'a mut sql::Connection,
    outpoints: impl Iterator<Item = &'a ([u8; 32], u32)>,
    spend_height: u32,
) {
    let tx = conn.transaction().unwrap();
    let mut stmt = tx
        .prepare(
            "
        UPDATE utxo
        SET spend_height = ?1
        WHERE txid = ?2 AND vout = ?3
        ",
        )
        .unwrap();
    for (txid, vout) in outpoints {
        stmt.execute(sql::params![spend_height, txid, vout])
            .unwrap();
    }
    drop(stmt);
    tx.commit().unwrap();
}

fn main() {
    println!("Reading environment variables...");
    let bitcoin_dir = std::env::var(BITCOIN_DATA_ENV).unwrap_or(DATA_DIR.to_string());
    println!("Bitcoin directory: {bitcoin_dir}");
    let blocks_dir = std::env::var(BLOCKS_DIR_ENV).unwrap_or(BLOCKS_DIR.to_string());
    println!("Blocks directory: {blocks_dir}");
    let sqlite_table = std::env::var(RESULTS_TABLE_ENV).unwrap_or(RESULTS.to_string());
    println!("Results table: {sqlite_table}");
    println!("Initializing Bitcoin Kernel...");
    let context = ContextBuilder::new().chain_type(CHAIN).build().unwrap();
    let chainman = ChainstateManager::new(&context, &bitcoin_dir, &blocks_dir).unwrap();
    chainman.import_blocks().unwrap();
    println!("Opening sqlite connection...");
    let mut conn = open_database(sqlite_table);
    let chain = chainman.active_chain();
    let tip_height = chain.height();
    for entry in chain.iter() {
        if entry.height() % 100 == 0 {
            println!(
                "{} / {} => {}",
                entry.height(),
                tip_height,
                entry.block_hash()
            );
        }
        let block = chainman.read_block_data(&entry).unwrap();
        let output_records = Arc::new(Mutex::new(Vec::new()));
        let input_records = Arc::new(Mutex::new(Vec::new()));
        block
            .transactions()
            .par_bridge()
            .map(|tx| {
                let txid = tx.txid().to_bytes();
                for (vout, output) in tx.outputs().enumerate() {
                    let amount = output.value();
                    let script = ScriptBuf::from_bytes(output.script_pubkey().to_bytes());
                    #[allow(deprecated)]
                    if script.is_provably_unspendable() {
                        continue;
                    }
                    let record = Record {
                        txid,
                        vout: vout as u32,
                        script_type: script.script_type(),
                        amount,
                        created_height: entry.height() as u32,
                        spend_height: None,
                    };
                    let mut record_lock = output_records.lock().unwrap();
                    record_lock.push(record);
                }
                for input in tx.inputs() {
                    let txid = input.outpoint().txid().to_bytes();
                    let vout = input.outpoint().index();
                    let mut record_lock = input_records.lock().unwrap();
                    record_lock.push((txid, vout));
                }
            })
            .collect::<Vec<()>>();
        insert_output(&mut conn, output_records.lock().unwrap().iter());
        update_spend_height(
            &mut conn,
            input_records.lock().unwrap().iter(),
            entry.height() as u32,
        );
        let mut coinbase_outputs = Vec::new();
        let coinbase = block.transactions().next().unwrap();
        let txid = coinbase.txid().to_bytes();
        for (vout, output) in coinbase.outputs().enumerate() {
            let amount = output.value();
            let script = ScriptBuf::from_bytes(output.script_pubkey().to_bytes());
            #[allow(deprecated)]
            if script.is_provably_unspendable() {
                continue;
            }
            let record = Record {
                txid,
                vout: vout as u32,
                script_type: 0x07,
                amount,
                created_height: entry.height() as u32,
                spend_height: None,
            };
            coinbase_outputs.push(record);
        }
        insert_output(&mut conn, coinbase_outputs.iter());
    }
    println!("Result written to {RESULTS}");
}
