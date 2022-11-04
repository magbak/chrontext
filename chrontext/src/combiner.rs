mod join_timeseries;
pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
mod solution_mapping;
pub(crate) mod static_subqueries;
pub(crate) mod time_series_queries;

use crate::query_context::{Context, PathEntry};

use crate::combiner::solution_mapping::SolutionMappings;
use crate::preparing::TimeSeriesQueryPrepper;
use crate::pushdown_setting::PushdownSetting;
use crate::rewriting::subqueries::SubQueryInContext;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use oxrdf::Variable;
use polars::frame::DataFrame;
use polars::prelude::{col, Expr, IntoLazy, LazyFrame, UniqueKeepStrategy};
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::error::Error;

#[derive(Debug)]
pub enum CombinerError {
    TimeSeriesQueryError(Box<dyn Error>),
}

pub struct Combiner {
    counter: u16,
    endpoint: String,
    time_series_database: Box<dyn TimeSeriesQueryable>,
    static_subqueries_in_context: Vec<SubQueryInContext>,
    prepper: TimeSeriesQueryPrepper,
}

impl Combiner {
    pub fn new(
        endpoint: String,
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        rewritten_filters: HashMap<Context, Expression>,
        static_subqueries_in_context: Vec<SubQueryInContext>,
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
            static_subqueries_in_context,
            prepper,
        }
    }

    pub async fn combine_static_and_time_series_results(
        &mut self,
        static_query_map: HashMap<Context, Query>,
        query: &Query,
    ) -> Result<SolutionMappings, CombinerError> {
        let mut context = Context::new();
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
