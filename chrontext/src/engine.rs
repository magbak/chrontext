use crate::combiner::{Combiner, CombinerError};
use crate::preparing::TimeSeriesQueryPrepper;
use crate::preprocessing::Preprocessor;
use crate::pushdown_setting::PushdownSetting;
use crate::rewriting::StaticQueryRewriter;
use crate::splitter::parse_sparql_select_query;
use crate::static_sparql::execute_sparql_query;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::frame::DataFrame;
use sparesults::QuerySolution;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum OrchestrationError {
    InconsistentDatatype(String, String, String),
}

impl Display for OrchestrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrchestrationError::InconsistentDatatype(s1, s2, s3) => {
                write!(
                    f,
                    "Inconsistent datatypes {} and {} for variable {}",
                    s1, s2, s3
                )
            }
        }
    }
}

impl Error for OrchestrationError {}

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    time_series_database: Box<dyn TimeSeriesQueryable>,
    endpoint: String,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        endpoint: String,
    ) -> Engine {
        Engine {
            pushdown_settings,
            time_series_database,
            endpoint
        }
    }

    pub async fn execute_hybrid_query(
        &mut self,
        query: &str,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let parsed_query = parse_sparql_select_query(query)?;
        debug!("Parsed query: {:?}", &parsed_query);
        let mut preprocessor = Preprocessor::new();
        let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed_query);
        debug!("Constraints: {:?}", variable_constraints);
        let mut rewriter = StaticQueryRewriter::new(&variable_constraints);
        let (static_rewrite, mut basic_time_series_queries) =
            rewriter.rewrite_query(preprocessed_query).unwrap();
        debug!("Produced static rewrite: {}", static_rewrite);
        debug!(
            "Produced basic time series queries: {:?}",
            basic_time_series_queries
        );

        let StaticQueryRewriter {
            rewritten_filters, ..
        } = rewriter;

        let mut time_series = self
            .execute_time_series_queries(time_series_queries)
            .await?;
        debug!("Time series: {:?}", time_series);
        let mut combiner = Combiner::new(self.endpoint.to_string(), Default::default(), Box::new(()), vec![], Default::default(), vec![]);
        let solution_mappings = combiner.combine_static_and_time_series_results(
            &parsed_query,
            static_result_df,
            &mut time_series,
        ).await?;
        Ok(solution_mappings.mappings.collect()?)
    }

    async fn execute_time_series_queries(
        &mut self,
        time_series_queries: Vec<TimeSeriesQuery>,
    ) -> Result<Vec<(TimeSeriesQuery, DataFrame)>, Box<dyn Error>> {
        let mut out = vec![];
        for tsq in time_series_queries {
            let df_res = self.time_series_database.execute(&tsq).await;
            match df_res {
                Ok(df) => {
                    match tsq.validate(&df) {
                        Ok(_) => {}
                        Err(err) => return Err(Box::new(err)),
                    }
                    out.push((tsq, df))
                }
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }
}

