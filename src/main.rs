#[tokio::main]
async fn main() {
    match athenacli::run().await {
        Ok(_) => {},
        Err(error) => {
            eprintln!("FATAL ERROR: {}", error);
            std::process::exit(1);
        }
    };
}