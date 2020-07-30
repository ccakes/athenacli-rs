use ascii_table::{AsciiTable, Column, Align};
use structopt::StructOpt;

use std::path::PathBuf;

mod athena;
mod error;
use error::Error;

type Result<T> = std::result::Result<T, error::Error>;

/// Basic Athena CLI
#[derive(StructOpt)]
#[structopt(name = "athenacli")]
struct Config {
    /// AWS region
    #[structopt(short = "r", long = "region", env = "AWS_REGION")]
    region: String,

    /// database name to connect to
    #[structopt(short = "d", long = "database")]
    database: String,

    /// S3 bucket name for results (eg s3://my-results)
    #[structopt(short = "b", long = "results")]
    pub result_bucket: String,

    /// Athena workgroup to use
    #[structopt(short = "w", long = "workgroup")]
    pub workgroup: Option<String>,

    /// run a single SQL statement, can be repeated
    #[structopt(short = "c", long = "command")]
    command: Option<Vec<String>>,

    /// execute one or more SQL statements from a file, then exit
    #[structopt(short = "f", long = "file")]
    file: Option<PathBuf>,

    /// Logging verbosity (repeat for more detail)
    #[structopt(short = "v", parse(from_occurrences))]
    verbose: u64,
}

pub async fn run() -> Result<()> {
    let args = Config::from_args();

    // Init logging
    // Derive verbosity from args
    let log_level = match args.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace"
    };

    let filter = tracing_subscriber::filter::EnvFilter::try_from_env("ATHENACLI_LOG")
        .unwrap_or_else(|_| tracing_subscriber::filter::EnvFilter::new(format!("athenacli={}", log_level)))
        .add_directive(tracing_subscriber::filter::LevelFilter::WARN.into());

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting tracing default failed");

    if args.command.is_some() && args.file.is_some() {
        tracing::error!("cannot specify both --command and --file!");
        std::process::exit(1);
    }

    if args.command.is_none() && args.file.is_none() {
        tracing::error!("must specify either --command or --file!");
        std::process::exit(1);
    }

    let queries: Vec<_> = match args.file {
        Some(ref path) if !path.exists() => {
            tracing::error!(path = %path.display(), "input file does not exist");
            std::process::exit(1);
        },
        Some(ref path) => {
            let contents = std::fs::read_to_string(path)?;
            let ast = sqlparser::parser::Parser::parse_sql(
                &sqlparser::dialect::GenericDialect {},
                &contents
            )?;
            ast.into_iter().map(|sth| sth.to_string()).collect()
        },
        None => args.command.unwrap(),
    };

    let athena = athena::Athena::new(&args.region, &args.database, &args.result_bucket, args.workgroup.clone())?;

    tracing::debug!(
        region = %args.region,
        database = %args.database,
        results_bucket = %args.result_bucket,
        workgroup = ?args.workgroup,
        "executing query"
    );
    for query in queries.into_iter() {
        match athena.query(&query).await {
            Ok(result) => {
                tracing::info!(
                    rows = %result.rows,
                    data_scanned = %result.data_scanned(),
                    execution_time = %result.total_time(),
                    "query complete"
                );

                // Return early if we have an empty resultset
                if result.rows == 0 { return Ok(()); }

                // Now set up our table
                let mut table = AsciiTable::default();

                for (idx, col) in result.columns.iter().enumerate() {
                    table.columns.insert(idx, Column {
                        header: col.into(),
                        align: Align::Left,
                        ..Default::default()
                    });
                }

                table.print(result.data);
            },
            Err(error) => {
                tracing::error!(%error, "error running query");
                Err(error)?
            }
        };
    }

    Ok(())
}