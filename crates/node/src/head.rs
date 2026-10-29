//! The embedded [`CanonicalBlockSource`]: the best chain of the in-process Zakura node.

use zakura_chain::block;
use ztreamer_chain::types::RawIndexBlock;
use ztreamer_indexer::index::BlockId;
use ztreamer_sync::head::{CanonicalBlockSource, HeadError};

use crate::NodeClient;

#[tonic::async_trait]
impl CanonicalBlockSource for NodeClient {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        Ok(NodeClient::tip(self).map(|(height, hash)| BlockId::new(height.0, hash)))
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        let block = NodeClient::block(self, block::Height(height)).await;
        match block {
            Ok(Some(block)) => {
                let block = RawIndexBlock::try_from(&*block)?;
                if block.height.0 != height {
                    return Err(HeadError::Height { expected: height, actual: block.height.0 });
                }
                Ok(Some(block))
            }
            Ok(None) => Ok(None),
            Err(error) => Err(HeadError::Node(error.to_string())),
        }
    }
}
