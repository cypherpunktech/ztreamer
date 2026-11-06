//! Embedded reads keep Zakura's retained payloads through indexing and serialization.

use std::sync::Arc;

use tower::ServiceExt as _;
use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    serialization::{ZcashDeserializeInto as _, ZcashSerialize as _},
};
use zakura_state::{Config, Request};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_GENESIS_BYTES};
use ztreamer_backend::{BlockHandle, StateBackend, TransactionHandle};
use ztreamer_chain::types::RawIndexBlock;
use ztreamer_embedded::InProcessBackend;

#[tokio::test]
async fn reads_preserve_retained_payloads() {
    let (mut state, read, _tip, _change) =
        zakura_state::init(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0).await.unwrap();
    let expected: Block = BLOCK_MAINNET_1_BYTES.as_slice().zcash_deserialize_into().unwrap();
    for bytes in [&*BLOCK_MAINNET_GENESIS_BYTES, &*BLOCK_MAINNET_1_BYTES] {
        let block: Arc<Block> = bytes.as_slice().zcash_deserialize_into().unwrap();
        (&mut state).oneshot(Request::CommitCheckpointVerifiedBlock(block.into())).await.unwrap();
    }

    let backend = InProcessBackend::new(read);
    let block = backend.block(expected.hash()).await.unwrap().unwrap();
    assert!(matches!(block, BlockHandle::Owned(_)));
    let retained = block.clone();
    drop(block);
    let raw = RawIndexBlock::try_from(retained.view()).unwrap();
    assert_eq!(raw.bytes, *BLOCK_MAINNET_1_BYTES);
    assert_eq!(raw.hash, expected.hash());
    assert_eq!(raw.txids, expected.transactions.iter().map(|tx| tx.hash()).collect::<Vec<_>>());

    let expected_tx = &expected.transactions[0];
    let mined = backend.transaction(expected_tx.hash()).await.unwrap().unwrap();
    assert!(matches!(
        mined.transaction,
        TransactionHandle::Standalone(_) | TransactionHandle::Block { .. }
    ));
    let retained_tx = mined.clone();
    drop(mined);
    drop(retained);
    assert_eq!(retained_tx.height, Height(1));
    assert_eq!(
        retained_tx.transaction.zcash_serialize_to_vec().unwrap(),
        expected_tx.zcash_serialize_to_vec().unwrap()
    );
}
