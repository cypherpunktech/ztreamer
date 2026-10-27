//! Verify transparent balance semantics through the production gRPC server.

use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::time::timeout;
use tokio_stream::StreamExt;
use tower::ServiceExt;
use zakura_chain::{
    block, parameters::Network, serialization::ZcashDeserializeInto, transaction::Transaction,
    transparent,
};
use zakura_state::{Config, Request};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_GENESIS_BYTES};
use ztreamer_indexer::index::{Index, IndexState};
use ztreamer_protocol::proto::{self, compact_tx_streamer_client::CompactTxStreamerClient};
use ztreamer_service::{CompactService, test_utils::LocalServer};

#[tokio::test]
async fn streamed_balance_deduplicates_and_limits_addresses() {
    timeout(Duration::from_secs(60), async {
        let network = Network::Mainnet;
        let genesis: Arc<block::Block> =
            BLOCK_MAINNET_GENESIS_BYTES.as_slice().zcash_deserialize_into().unwrap();
        let mut block_one: Arc<block::Block> =
            BLOCK_MAINNET_1_BYTES.as_slice().zcash_deserialize_into().unwrap();

        // Genesis has no funds. Block 1's founders' reward is an indexed P2SH output.
        let reward = &block_one.transactions[0].outputs()[1];
        let expected_balance = 12_500;
        assert_eq!(u64::from(reward.value), expected_balance as u64);
        let funded = reward.address(&network).unwrap().to_string();
        let unfunded =
            transparent::Address::from_script_hash(network.t_addr_kind(), [0x42; 20]).to_string();
        assert_ne!(funded, unfunded);

        // Turn the miner's existing P2PK output into an indexed P2PKH output.
        // This synthetic block is committed as checkpoint-verified below, bypassing PoW checks.
        let second_address =
            transparent::Address::from_pub_key_hash(network.t_addr_kind(), [0x24; 20]);
        let block_one_mut = Arc::make_mut(&mut block_one);
        let Transaction::V1 { outputs, .. } = Arc::make_mut(&mut block_one_mut.transactions[0])
        else {
            panic!("block 1 coinbase must be a V1 transaction");
        };
        assert_eq!(u64::from(outputs[0].value), 50_000);
        outputs[0].lock_script = second_address.script();
        let second_funded = outputs[0].address(&network).unwrap().to_string();
        assert_eq!(second_funded, second_address.to_string());
        assert_ne!(funded, second_funded);
        assert_ne!(unfunded, second_funded);
        Arc::make_mut(&mut block_one_mut.header).merkle_root =
            block_one_mut.transactions.iter().collect();

        let dir = tempfile::tempdir().unwrap();
        let index =
            Arc::new(Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", genesis.hash()).unwrap());
        let (mut state, read, _tip, _change) =
            zakura_state::init(Config::ephemeral(), &network, block::Height::MAX, 0).await.unwrap();
        for block in [genesis, block_one] {
            (&mut state)
                .oneshot(Request::CommitCheckpointVerifiedBlock(block.into()))
                .await
                .unwrap();
        }

        let service = CompactService::new(index, IndexState::default(), "main", read);
        let server = LocalServer::serve(service).await;
        let mut client = CompactTxStreamerClient::connect(server.endpoint.clone()).await.unwrap();

        let a = funded.as_str();
        let b = unfunded.as_str();
        let c = second_funded.as_str();
        let mut observed = Vec::new();
        for (case, addresses, expected) in [
            ("empty stream", vec![], 0),
            ("unfunded address", vec![b], 0),
            ("funded address", vec![a], expected_balance),
            ("second funded address", vec![c], 50_000),
            ("two funded addresses", vec![a, c], 62_500),
            ("two funded addresses reversed", vec![c, a], 62_500),
            ("repeated funded addresses", vec![a, c, a, c], 62_500),
            ("unfunded then funded", vec![b, a], expected_balance),
            ("adjacent duplicates", vec![a, a], expected_balance),
            ("nonconsecutive duplicates", vec![a, b, a], expected_balance),
            ("independent request", vec![a, a], expected_balance),
        ] {
            let addresses: Vec<String> = addresses.into_iter().map(str::to_owned).collect();
            let unary = client
                .get_taddress_balance(proto::AddressList { addresses: addresses.clone() })
                .await
                .unwrap()
                .into_inner()
                .value_zat;
            assert_eq!(unary, expected, "unary balance: {case}");

            let streamed = client
                .get_taddress_balance_stream(tokio_stream::iter(
                    addresses.into_iter().map(|address| proto::Address { address }),
                ))
                .await
                .unwrap()
                .into_inner()
                .value_zat;
            observed.push((case, streamed, unary));
        }

        let invalid = client
            .get_taddress_balance_stream(tokio_stream::iter(
                [a, a, "invalid"].map(|address| proto::Address { address: address.to_owned() }),
            ))
            .await
            .unwrap_err();
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);

        let addresses: Vec<_> = std::iter::once(funded.clone())
            .chain((0..10_000u64).map(|i| {
                let mut hash = [0x42; 20];
                hash[..8].copy_from_slice(&i.to_le_bytes());
                transparent::Address::from_script_hash(network.t_addr_kind(), hash).to_string()
            }))
            .collect();
        assert_eq!(addresses.iter().collect::<HashSet<_>>().len(), 10_001);

        // Exactly 10,000 distinct addresses are allowed; duplicates at capacity are free.
        let mut at_limit = addresses[..10_000].to_vec();
        at_limit.extend([funded.clone(), addresses[9_999].clone()]);
        let balance = client
            .get_taddress_balance_stream(tokio_stream::iter(
                at_limit.into_iter().map(|address| proto::Address { address }),
            ))
            .await
            .unwrap()
            .into_inner()
            .value_zat;
        assert_eq!(balance, expected_balance, "duplicates at the address limit");

        // Keep the input open to require rejection as soon as the limit is exceeded.
        let over_limit =
            tokio_stream::iter(addresses.into_iter().map(|address| proto::Address { address }))
                .chain(tokio_stream::pending());
        let exhausted =
            timeout(Duration::from_secs(30), client.get_taddress_balance_stream(over_limit))
                .await
                .expect("over-limit stream must be rejected before the client closes it")
                .unwrap_err();
        assert_eq!(exhausted.code(), tonic::Code::ResourceExhausted);

        let balance = client
            .get_taddress_balance_stream(tokio_stream::iter([proto::Address { address: funded }]))
            .await
            .unwrap()
            .into_inner()
            .value_zat;
        assert_eq!(balance, expected_balance, "request after an address limit error");

        drop(client);
        server.stop().await;

        // Collect all responses and stop the server before asserting the regression.
        for (case, streamed, unary) in observed {
            assert_eq!(streamed, unary, "streamed balance: {case}");
        }
    })
    .await
    .expect("balance test setup, requests, and shutdown must complete within 60 seconds");
}
