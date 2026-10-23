//! Commitment tree requests: tree states and note commitment subtree roots.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tower::ServiceExt;
use zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex};
use zakura_state::{ReadRequest, ReadResponse};
use ztreamer_protocol::proto;

use super::{
    CompactService, RpcStream, ServingSnapshot, ensure_tip_ready, index_status, source_status,
};

impl CompactService {
    pub(crate) async fn tree_state(
        &self,
        request: proto::BlockId,
    ) -> Result<proto::TreeState, Status> {
        let record = self.record(request).await?;
        let source = self.zakura.clone();
        let hash = block::Hash(record.hash);
        let (sapling, orchard, ironwood) = tokio::try_join!(
            source.clone().oneshot(ReadRequest::SaplingTree(hash.into())),
            source.clone().oneshot(ReadRequest::OrchardTree(hash.into())),
            source.oneshot(ReadRequest::IronwoodTree(hash.into())),
        )
        .map_err(source_status)?;

        let sapling_tree = match sapling {
            ReadResponse::SaplingTree(tree) => {
                tree.map(|tree| hex::encode(tree.to_rpc_bytes())).unwrap_or_default()
            }
            _ => return Err(Status::internal("unexpected Sapling tree response")),
        };
        let orchard_tree = match orchard {
            ReadResponse::OrchardTree(tree) => {
                tree.map(|tree| hex::encode(tree.to_rpc_bytes())).unwrap_or_default()
            }
            _ => return Err(Status::internal("unexpected Orchard tree response")),
        };
        let ironwood_tree = match ironwood {
            ReadResponse::IronwoodTree(tree) => {
                tree.map(|tree| hex::encode(tree.to_rpc_bytes())).unwrap_or_default()
            }
            _ => return Err(Status::internal("unexpected Ironwood tree response")),
        };

        Ok(proto::TreeState {
            network: self.chain_name.to_string(),
            height: u64::from(record.height),
            hash: hash.to_string(),
            time: record.time,
            sapling_tree,
            orchard_tree,
            ironwood_tree,
        })
    }

    fn subtree_stream(
        &self,
        roots: Vec<(Vec<u8>, u32)>,
        snapshot: ServingSnapshot,
    ) -> RpcStream<proto::SubtreeRoot> {
        let index = Arc::clone(&self.index);
        let (sender, receiver) = mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            for (root_hash, height) in roots {
                let block = match snapshot.volatile(height) {
                    Some(block) => block.clone(),
                    None => match index.read_block(snapshot.generation, height) {
                        Ok(block) => block,
                        Err(error) => {
                            let _ = sender.blocking_send(Err(index_status(error)));
                            return;
                        }
                    },
                };
                if sender
                    .blocking_send(Ok(proto::SubtreeRoot {
                        root_hash,
                        completing_block_hash: block.hash.to_vec(),
                        completing_block_height: u64::from(height),
                    }))
                    .is_err()
                {
                    return;
                }
            }
        });
        Box::pin(ReceiverStream::new(receiver))
    }

    pub(crate) async fn latest_tree_state(&self) -> Result<proto::TreeState, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        self.tree_state(proto::BlockId { height: u64::from(tip.height), hash: tip.hash.to_vec() })
            .await
    }

    pub(crate) async fn subtree_roots(
        &self,
        request: proto::GetSubtreeRootsArg,
    ) -> Result<RpcStream<proto::SubtreeRoot>, Status> {
        let start_index = NoteCommitmentSubtreeIndex(
            request
                .start_index
                .try_into()
                .map_err(|_| Status::invalid_argument("subtree start index exceeds u16"))?,
        );
        let limit = match request.max_entries {
            0 => None,
            limit => Some(NoteCommitmentSubtreeIndex(
                limit
                    .try_into()
                    .map_err(|_| Status::invalid_argument("subtree entry limit exceeds u16"))?,
            )),
        };
        let source = self.zakura.clone();
        let response = match proto::ShieldedProtocol::try_from(request.shielded_protocol) {
            Ok(proto::ShieldedProtocol::Sapling) => {
                source.oneshot(ReadRequest::SaplingSubtrees { start_index, limit }).await
            }
            Ok(proto::ShieldedProtocol::Orchard) => {
                source.oneshot(ReadRequest::OrchardSubtrees { start_index, limit }).await
            }
            Ok(proto::ShieldedProtocol::Ironwood) => {
                source.oneshot(ReadRequest::IronwoodSubtrees { start_index, limit }).await
            }
            Err(_) => return Err(Status::invalid_argument("invalid shielded protocol")),
        }
        .map_err(source_status)?;
        let roots = match response {
            ReadResponse::SaplingSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_bytes().to_vec(), subtree.end_height.0))
                .collect(),
            ReadResponse::OrchardSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            ReadResponse::IronwoodSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            _ => return Err(Status::internal("unexpected subtree response")),
        };
        Ok(self.subtree_stream(roots, self.snapshot()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    use zakura_chain::parameters::Network;
    use zakura_state::Config;
    use ztreamer_indexer::index::Index;

    use crate::service::test_utils::{hash, index_through, record};

    #[test]
    fn subtree_stream_resolves_volatile_completion_blocks() {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let index =
                Arc::new(Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap());
            let state = index_through(&index, 1_005);
            let (_state_service, read_service, _tip, _change) =
                zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
                    .await
                    .expect("ephemeral state initializes");
            let service = CompactService::new(index, state, "main", read_service);
            service.publish_head(state, vec![record(1_006), record(1_007)]).unwrap();

            let mut stream = service.subtree_stream(vec![(vec![7; 32], 1_007)], service.snapshot());
            let root = stream.next().await.unwrap().unwrap();

            assert_eq!(root.root_hash, vec![7; 32]);
            assert_eq!(root.completing_block_hash, hash(1_007));
            assert_eq!(root.completing_block_height, 1_007);
        });
    }
}
