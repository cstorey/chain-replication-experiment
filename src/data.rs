#[cfg(feature = "serde_macros")]
include!("data.rs.in");

#[cfg(not(feature = "serde_macros"))]
include!(concat!(env!("OUT_DIR"), "/data.rs"));
