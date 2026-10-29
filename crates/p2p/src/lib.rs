//! Zakura custom-service transport for [`ztreamer_service::CompactService`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod service;

pub use service::P2pCompactService;
