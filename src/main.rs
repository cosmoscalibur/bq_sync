fn main() {
    std::process::exit(bq_sync::cli::run(std::env::args_os()));
}
