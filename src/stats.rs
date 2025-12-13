use kernel::{ChainType, ChainstateManager, ContextBuilder, core::TransactionExt};

const CHAIN: ChainType = ChainType::Mainnet;
const DATA_DIR: &str = "/data1";
const BLOCKS_DIR: &str = "/data1/blocks/";

fn compute_avg(block_ouputs: &[i64]) -> i64 {
    block_ouputs.iter().sum::<i64>() / block_ouputs.len() as i64
}

fn main() {
    let context = ContextBuilder::new().chain_type(CHAIN).build().unwrap();
    let chainman = ChainstateManager::new(&context, DATA_DIR, BLOCKS_DIR).unwrap();
    chainman.import_blocks().unwrap();
    let chain = chainman.active_chain();
    let tip_height = chain.height();
    let mut total_inputs = 0;
    let mut total_outputs = 0;
    let mut outputs_per_block = Vec::new();
    for entry in chain.iter() {
        if entry.height() % 10_000 == 0 {
            println!("{} / {}", entry.height(), tip_height);
        }
        let block = chainman.read_block_data(&entry).unwrap();
        let mut block_outputs = 0;
        for tx in block.transactions().skip(1) {
            block_outputs += tx.output_count() as i64;
            total_outputs += tx.output_count();
            total_inputs += tx.input_count();
        }
        outputs_per_block.push(block_outputs);
    }
    let max_outputs_in_blk = outputs_per_block.iter().max().unwrap();
    let avg_outputs_per_blk = compute_avg(&outputs_per_block);
    println!("=== Summary =============================");
    println!("Chain height:        {tip_height}");
    println!("Total inputs:        {total_inputs}");
    println!("Total outputs:       {total_outputs}");
    println!("Avg outputs per blk: {avg_outputs_per_blk}");
    println!("Max outputs in blk:  {max_outputs_in_blk}");
}
