use std::{fs::File, io::Write};

use kernel::{
    ChainType, ChainstateManager, ContextBuilder,
    core::{TransactionExt, TxInExt, TxOutPointExt, TxidExt},
};

const CHAIN: ChainType = ChainType::Mainnet;
const DATA_DIR: &str = "/data";
const BLOCKS_DIR: &str = "/data/blocks/";

fn main() {
    let context = ContextBuilder::new().chain_type(CHAIN).build().unwrap();
    let chainman = ChainstateManager::new(&context, DATA_DIR, BLOCKS_DIR).unwrap();
    chainman.import_blocks().unwrap();
    let mut file = File::create_new("./cache_hints.bin").unwrap();
    let chain = chainman.active_chain();
    let tip = chain.tip().height();
    for entry in chain.iter() {
        let block = chainman.read_block_data(&entry).unwrap();
        println!("{}/{}", entry.height(), tip);
        let total_inputs: u32 = 0;
        let mut outpoints = Vec::new();
        for tx in block.transactions().skip(1) {
            for input in tx.inputs() {
                let index = input.outpoint().index();
                let txid = input.outpoint().txid().to_bytes();
                outpoints.push((index, txid));
            }
        }
        file.write_all(&total_inputs.to_le_bytes()).unwrap();
        for outpoint in outpoints {
            file.write_all(&outpoint.0.to_le_bytes()).unwrap();
            file.write_all(&outpoint.1).unwrap();
        }
    }
}
