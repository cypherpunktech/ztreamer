//! The chain name that a zakura node gives in `getblockchaininfo`, and the [`Network`] of it.

use zakura_chain::parameters::Network;

/// A chain that ztreamerd cannot serve: only mainnet and the default testnet have a
/// [`Network`] that ztreamerd can make from the chain name.
#[derive(Debug, thiserror::Error)]
#[error("remote zakura runs chain {0:?}, ztreamerd serves only \"main\" and \"test\"")]
pub struct UnknownChain(String);

/// The chain name that a zakura node gives in `getblockchaininfo`. [`Network::try_from`] maps it
/// to the network of the node.
///
/// ```
/// use zakura_chain::parameters::Network;
/// use ztreamer_remote::NodeChain;
///
/// assert_eq!(Network::try_from(NodeChain("main")).unwrap(), Network::Mainnet);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct NodeChain<'a>(pub &'a str);

impl TryFrom<NodeChain<'_>> for Network {
    type Error = UnknownChain;

    /// The network whose BIP70 name is the chain name: `main` is mainnet, `test` is the default
    /// testnet.
    fn try_from(chain: NodeChain<'_>) -> Result<Self, UnknownChain> {
        [Self::Mainnet, Self::new_default_testnet()]
            .into_iter()
            .find(|network| network.bip70_network_name() == chain.0)
            .ok_or_else(|| UnknownChain(chain.0.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_chain_name_gives_the_network() {
        let network = |name| Network::try_from(NodeChain(name));
        assert_eq!(network("main").unwrap(), Network::Mainnet);
        assert_eq!(network("test").unwrap(), Network::new_default_testnet());
        let error = network("regtest").unwrap_err();
        assert_eq!(
            error.to_string(),
            r#"remote zakura runs chain "regtest", ztreamerd serves only "main" and "test""#
        );
    }
}
