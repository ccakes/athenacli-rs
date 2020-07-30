#[derive(Debug)]
pub enum Error {
    AthenaError,
    ConnectionError,
    GetQueryResultsError(String),
    InvalidProxy(http::uri::InvalidUri),
    InvalidRegion,
    InvalidSql(sqlparser::parser::ParserError),
    TracingFormat,
    QueryError,
    QueryCancelled,
    QueryFailed(String),

    IoErr(std::io::Error),
}

unsafe impl Send for Error {}
unsafe impl Sync for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::AthenaError => write!(f, "Athena API error encountered"),
            Self::ConnectionError => write!(f, "Error connecting to AWS"),
            Self::GetQueryResultsError(error) => write!(f, "Error getting query results: {}", error),
            Self::InvalidProxy(error) => write!(f, "Invalid proxy URI: {}", error),
            Self::InvalidRegion => write!(f, "Invalid region specified"),
            Self::InvalidSql(error) => write!(f, "Invalid SQL in provided file: {}", error),
            Self::TracingFormat => write!(f, "ATHENACLI_LOG contained invalid format"),
            Self::QueryError => write!(f, "Unknown error executing query"),
            Self::QueryCancelled => write!(f, "Query cancelled"),
            Self::QueryFailed(reason) => write!(f, "Query failed: {}", reason),

            Self::IoErr(error) => write!(f, "I/O error: {}", error),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl From<http::uri::InvalidUri> for Error {
    fn from(error: http::uri::InvalidUri) -> Self {
        Self::InvalidProxy(error)
    }
}

impl From<rusoto_core::RusotoError<rusoto_athena::GetQueryResultsError>> for Error {
    fn from(error: rusoto_core::RusotoError<rusoto_athena::GetQueryResultsError>) -> Self {
        Self::GetQueryResultsError(error.to_string())
    }
}

impl From<rusoto_signature::region::ParseRegionError> for Error {
    fn from(_error: rusoto_signature::region::ParseRegionError) -> Self {
        Self::InvalidRegion
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::IoErr(error)
    }
}

impl From<sqlparser::parser::ParserError> for Error {
    fn from(error: sqlparser::parser::ParserError) -> Self {
        Self::InvalidSql(error)
    }
}

impl From<tracing_subscriber::filter::FromEnvError> for Error {
    fn from(_error: tracing_subscriber::filter::FromEnvError) -> Self {
        Self::TracingFormat
    }
}