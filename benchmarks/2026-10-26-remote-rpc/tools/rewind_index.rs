//! Moves the durable tip of an index copy back to a lower height, as a deep reorg with no
//! replacement blocks does. Benchmark use only: it makes an index that is behind its node.
//!
//! Usage: `rewind_index <INDEX_DIR> <HEIGHT>`

use std::env;

use ztreamer_indexer::index::{BlockId, Index};

const MAP_SIZE: usize = 64 << 30;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let [_, dir, height] = args.as_slice() else {
        return Err("usage: rewind_index <INDEX_DIR> <HEIGHT>".into());
    };
    let height: u32 = height.parse()?;
    let index = Index::open_existing(dir, MAP_SIZE)?;
    let before = index.state()?;
    println!("before: {before:?}");
    let ancestor = index.read_block(before.generation(), height)?;
    let after = index.replace_deep_suffix(
        before.generation(),
        BlockId::new(ancestor.height, ancestor.hash),
        Vec::new(),
        None,
    )?;
    println!("after: {after:?}");
    index.verify_continuity()?;
    println!("continuity: ok");
    Ok(())
}
