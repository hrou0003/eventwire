use std::process;

mod codec;
mod protocol;
mod server;
mod state;

fn main() {
    if let Err(err) = server::run() {
        eprintln!("server error: {err}");
        process::exit(1);
    }
}
