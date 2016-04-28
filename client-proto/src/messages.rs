#[cfg(feature = "serde_macros")]
include!("messages.rs.in");

#[cfg(not(feature = "serde_macros"))]
include!(concat!(env!("OUT_DIR"), "/messages.rs"));
