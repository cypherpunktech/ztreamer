//! Commitment tree requests: tree states and note commitment subtree roots.

use std::sync::Arc;

use tonic::Status;
use zakura_chain::subtree::NoteCommitmentSubtreeIndex;
use ztreamer_protocol::proto;

use super::{
    CompactService, ProducedStream, RpcStream, ServingSnapshot, ensure_tip_ready, index_status,
};

impl CompactService {
    pub(crate) async fn tree_state(
        &self,
        request: proto::BlockId,
    ) -> Result<proto::TreeState, Status> {
        let record = self.record(request).await?;
        let hash = record.hash;
        let trees = self.backend.tree_state(hash).await?;

        Ok(proto::TreeState {
            network: self.chain_name.to_string(),
            height: u64::from(record.height),
            hash: hash.to_string(),
            time: record.time,
            sapling_tree: trees
                .sapling
                .map(|tree| hex::encode(tree.to_rpc_bytes()))
                .unwrap_or_default(),
            orchard_tree: trees
                .orchard
                .map(|tree| hex::encode(tree.to_rpc_bytes()))
                .unwrap_or_default(),
            ironwood_tree: trees
                .ironwood
                .map(|tree| hex::encode(tree.to_rpc_bytes()))
                .unwrap_or_default(),
        })
    }

    fn subtree_stream(
        &self,
        roots: Vec<(Vec<u8>, u32)>,
        snapshot: ServingSnapshot,
    ) -> RpcStream<proto::SubtreeRoot> {
        let index = Arc::clone(&self.index);
        ProducedStream::spawn_blocking(move |sender| {
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
                        completing_block_hash: block.hash.0.to_vec(),
                        completing_block_height: u64::from(height),
                    }))
                    .is_err()
                {
                    return;
                }
            }
        })
    }

    pub(crate) async fn latest_tree_state(&self) -> Result<proto::TreeState, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        self.tree_state(proto::BlockId { height: u64::from(tip.height), hash: tip.hash.0.to_vec() })
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
        let backend = &self.backend;
        let roots = match proto::ShieldedProtocol::try_from(request.shielded_protocol) {
            Ok(proto::ShieldedProtocol::Sapling) => backend
                .sapling_subtrees(start_index, limit)
                .await?
                .into_values()
                .map(|subtree| (subtree.root.to_bytes().to_vec(), subtree.end_height.0))
                .collect(),
            Ok(proto::ShieldedProtocol::Orchard) => backend
                .orchard_subtrees(start_index, limit)
                .await?
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            Ok(proto::ShieldedProtocol::Ironwood) => backend
                .ironwood_subtrees(start_index, limit)
                .await?
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            Err(_) => return Err(Status::invalid_argument("invalid shielded protocol")),
        };
        Ok(self.subtree_stream(roots, self.snapshot()))
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;

    use crate::{
        service::test_utils::{hash, record},
        test_utils::EphemeralService,
    };

    #[tokio::test]
    async fn subtree_stream_resolves_volatile_completion_blocks() {
        let ctx = EphemeralService::sealed_through(1_005).await;
        ctx.service.publish_head(ctx.state, vec![record(1_006), record(1_007)]).unwrap();

        let mut stream =
            ctx.service.subtree_stream(vec![(vec![7; 32], 1_007)], ctx.service.snapshot());
        let root = stream.next().await.unwrap().unwrap();

        assert_eq!(root.root_hash, vec![7; 32]);
        assert_eq!(root.completing_block_hash, hash(1_007).0);
        assert_eq!(root.completing_block_height, 1_007);
    }
}
