pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
pub mod solution_mapping;
pub(crate) mod static_subqueries;
pub(crate) mod time_series_queries;

use crate::query_context::Context;

use crate::combiner::solution_mapping::SolutionMappings;
use crate::preparing::TimeSeriesQueryPrepper;
use crate::pushdown_setting::PushdownSetting;
use crate::static_sparql::QueryExecutionError;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesValidationError};
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CombinerError {
    TimeSeriesQueryError(Box<dyn Error>),
    StaticQueryExecutionError(QueryExecutionError),
    InconsistentDatatype(String, String, String),
    TimeSeriesValidationError(TimeSeriesValidationError)
}

impl Display for CombinerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CombinerError::InconsistentDatatype(s1, s2, s3) => {
                write!(
                    f,
                    "Inconsistent datatypes {} and {} for variable {}",
                    s1, s2, s3
                )
            }
            CombinerError::TimeSeriesQueryError(tsqe) => {
                write!(f, "Time series query error {}", tsqe)
            }
            CombinerError::StaticQueryExecutionError(sqee) => {
                write!(f, "Static query execution error {}", sqee)
            }
            CombinerError::TimeSeriesValidationError(v) => {
                write!(f, "Time series validation error {}", v)
            }
        }
    }
}

impl Error for CombinerError {}

pub struct Combiner {
    counter: u16,
    endpoint: String,
    pub time_series_database: Box<dyn TimeSeriesQueryable>,
    prepper: TimeSeriesQueryPrepper,
}

impl Combiner {
    pub fn new(
        endpoint: String,
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> Combiner {
        let prepper = TimeSeriesQueryPrepper::new(
            pushdown_settings,
            basic_time_series_queries,
            rewritten_filters,
        );
        Combiner {
            counter: 0,
            endpoint,
            time_series_database,
            prepper,
        }
    }

    pub async fn combine_static_and_time_series_results(
        &mut self,
        static_query_map: HashMap<Context, Query>,
        query: &Query,
    ) -> Result<SolutionMappings, CombinerError> {
        let context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = query
        {
            Ok(self
                .lazy_graph_pattern(pattern, None, static_query_map, None, &context)
                .await?)
        } else {
            panic!("Only select queries supported")
        }
    }
}
