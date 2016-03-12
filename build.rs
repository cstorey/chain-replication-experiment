#[cfg(not(feature = "serde_macros"))]
extern crate syntex;
#[cfg(not(feature = "serde_macros"))]
extern crate serde_codegen;

use std::env;
use std::path::Path;

#[cfg(not(feature = "serde_macros"))]
fn process() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    for m in ["data", "config_data"].iter() {
        let mut registry = syntex::Registry::new();
        serde_codegen::register(&mut registry);

        let src = format!("src/{}.rs.in", m);
        let dst = Path::new(&out_dir).join(format!("{}.rs", m));
        registry.expand("", Path::new(&src), &Path::new(&dst)).expect("expanding");
    }
}
#[cfg(feature = "serde_macros")]
fn process() {
}

pub fn main() {
    process()
}
