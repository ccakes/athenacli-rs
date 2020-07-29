use crate::Error;

use hyper::client::HttpConnector;
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use rusoto_core::credential::ChainProvider;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_athena::{
    Athena as AthenaTrait,
    AthenaClient,

    GetQueryExecutionInput,
    GetQueryExecutionOutput,
    GetQueryResultsInput,
    ResultConfiguration,
    StartQueryExecutionInput,
    QueryExecution,
    QueryExecutionContext,
    QueryExecutionStatistics,
    QueryExecutionStatus,
};

use std::str::FromStr;
use std::time::{Duration, Instant};

mod types;
pub use self::types::*;

pub struct Athena {
    client: AthenaClient,
    database: String,
    result_bucket: String,
    workgroup: Option<String>
}

impl Athena {
    pub fn new(region: &str, database: &str, result_bucket: &str, workgroup: Option<String>) -> crate::Result<Self> {
        // Create a new AthenaClient, using a HTTPS_PROXY if configured in the environment
        let client = match std::env::var("HTTPS_PROXY") {
            Ok(proxy_uri) => {
                let proxy = Proxy::new(Intercept::All, proxy_uri.parse()?);
                let proxy_connector = ProxyConnector::from_proxy(HttpConnector::new(), proxy)?;

                let http = HttpClient::from_connector(proxy_connector);
                AthenaClient::new_with(http, ChainProvider::new(), Region::from_str(&region)?)
            }
            Err(_) => AthenaClient::new(Region::from_str(&region)?),
        };

        Ok(Self {
            client,
            database: database.into(),
            result_bucket: result_bucket.into(),
            workgroup
        })
    }

    pub async fn query(&self, query: &str) -> crate::Result<QueryResult> {
        // Start the query
        let query_req = StartQueryExecutionInput {
            client_request_token: Some(uuid::Uuid::new_v4().to_string()),
            // Use default settings for encrypting results - should use bucket settings. Open to PRs
            // to make this more configurable
            result_configuration: Some(ResultConfiguration {
                encryption_configuration: Default::default(),
                output_location: Some(self.result_bucket.clone()),
            }),
            query_execution_context: Some(QueryExecutionContext{
                database: Some(self.database.clone()),
                ..Default::default()
            }),
            query_string: query.to_owned(),
            work_group: self.workgroup.clone()
        };

        // Fetch the execution ID to use for later requests
        let query_execution_id = self.client.start_query_execution(query_req).await
            .map_err(|error| {
                tracing::error!(%error, "error starting query execution");
                Error::AthenaError
            })?
            .query_execution_id.expect("missing execution id");
        tracing::trace!(%query_execution_id);

        // Now we poll Athena waiting for the query to finish. If we get transient API errors we retry up to 5
        // times before giving up
        let start = Instant::now();
        let mut err_count = 0u8;
        let mut result = loop {
            let res = match self.client.get_query_execution(GetQueryExecutionInput {
                query_execution_id: query_execution_id.clone()
            }).await {
                Ok(res) => res,
                Err(error) => {
                    err_count += 1;

                    if err_count > 5 {
                        tracing::error!(%error, "error getting query execution status");
                        Err(Error::AthenaError)?;
                    }

                    tokio::time::delay_for(Duration::from_millis(250)).await;
                    continue;
                }
            };

            // Epic destructuring	ᕦ( ͡° ͜ʖ ͡°)ᕤ
            // Work out the current status of the query. Athena _can_ occasionally retry queries and I *don't*
            // handle that here - ie a query can transition QUEUED -> RUNNING -> FAILED -> QUEUED.
            //
            // I'd like to eventually handle it but this was just a quick implementation.
            match res {
                GetQueryExecutionOutput {
                    query_execution: Some(QueryExecution {
                        query_execution_id: Some(ref query_execution_id),
                        statistics: Some(QueryExecutionStatistics {
                            data_scanned_in_bytes: Some(data_scanned_bytes),
                            query_queue_time_in_millis: Some(query_queue_time_ms),
                            total_execution_time_in_millis: Some(total_execution_time_ms),
                            ..
                        }),
                        status: Some(QueryExecutionStatus {
                            state: Some(ref state),
                            ..
                        }),
                        ..
                    })
                } if state == "SUCCEEDED" => {
                    break QueryResult {
                        query_execution_id: query_execution_id.into(),
                        data_scanned_bytes,
                        query_queue_time_ms,
                        total_execution_time_ms,
                        rows: 0,
                        columns: vec![],
                        data: vec![]
                    }
                },
                GetQueryExecutionOutput {
                    query_execution: Some(QueryExecution {
                        status: Some(QueryExecutionStatus {
                            state: Some(ref state),
                            state_change_reason: Some(ref state_change_reason),
                            ..
                        }),
                        ..
                    }),
                    ..
                } => {
                    tracing::error!(result = %state, reason = %state_change_reason);
                    match state.as_str() {
                        "FAILED" => Err(Error::QueryFailed(state_change_reason.to_owned()))?,
                        "CANCELLED" => Err(Error::QueryCancelled)?,
                        _ => unimplemented!()
                    };
                },
                GetQueryExecutionOutput {
                    query_execution: Some(QueryExecution {
                        status: Some(QueryExecutionStatus {
                            state: Some(ref state),
                            ..
                        }),
                        ..
                    }),
                    ..
                } if state == "RUNNING" || state == "QUEUED" => {
                    tracing::trace!(
                        %state,
                        time_taken = %humantime::format_duration(start.elapsed()).to_string()
                    );
                    tokio::time::delay_for(Duration::from_secs(1)).await;
                },
                v @ _ => {
                    tracing::debug!(debug = ?v);
                    Err(Error::QueryError)?
                }
            }
            // tracing::debug!("query: {} -> {:?}", query_execution_id, status.state);
        };

        // Fetch results in a loop and append
        let mut result_req = GetQueryResultsInput {
            next_token: None,
            query_execution_id: result.query_execution_id.clone(),
            ..Default::default()
        };

        loop {
            let res = self.client.get_query_results(result_req.clone()).await?;
            let resultset = res.result_set.expect("result_set was none");

            // Get resultset metadata
            let cols = resultset.result_set_metadata.expect("missing result_set_metadata")
                .column_info.expect("missing column_info");
            result.columns = cols.into_iter().map(|c| c.name).collect();

            // 
            let rows = resultset.rows.expect("missing result_set.rows");
            rows.into_iter()
                .skip(1) // headers..
                .for_each(|row| {
                    let row = row.data.expect("missing row.data");

                    let new: Vec<String> = row.into_iter()
                        .map(|field| {
                            field.var_char_value.unwrap_or_else(String::new)
                        })
                        .collect();
                    
                    result.append_row(new);
                });

            tracing::trace!(
                state = %"SUCCEEDED",
                rows_read = %result.rows
            );

            if res.next_token.is_some() {
                result_req.next_token = res.next_token;
                continue;
            }

            break;
        }

        Ok(result)
    }
}