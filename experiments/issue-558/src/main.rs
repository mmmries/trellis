//! Scratch experiments for issue #558 (design note for epic #556). No Trellis code.
//!
//! Every subcommand connects to the throwaway cluster started by `cluster.sh`
//! (`EXP_SOCK`, `EXP_PORT`; defaults `/home/mike/exp558/a/sock`, 54321).

mod exp1;
mod exp2;
mod pg;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("exp1-epoch") => exp1::epoch(&args[1..]).await,
        Some("exp1-snapshot") => exp1::snapshot(&args[1..]).await,
        Some("exp2") => exp2::run(&args[1..]).await,
        _ => {
            eprintln!(
                "usage: exp558 exp1-epoch | exp1-snapshot [--rows N --writers N --samplers N --secs N] | exp2 [--mode literal|lsn] [scenario...]"
            );
            std::process::exit(64)
        }
    }
}
