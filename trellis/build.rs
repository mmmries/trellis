//! Makes cargo rebuild the engine when a file is added to `migrations/`:
//! `refinery::embed_migrations!` reads the directory at compile time but
//! tells cargo only about the files it found, so a new migration alone would
//! leave the previous set embedded.

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
