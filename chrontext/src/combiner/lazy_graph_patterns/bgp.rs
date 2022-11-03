use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{IntoLazy, LazyFrame};
use spargebra::term::TriplePattern;
use std::collections::HashMap;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::{ConstrainingSolutionMapping, update_constraints};
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::lf_wrap::WrapLF;

impl Combiner {
    pub(crate) fn lazy_bgp(
        &mut self,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let mut new_prepared_time_series_queries = prepared_time_series_queries;
        let mut updated_constraints= constraints;

        if let Some(tsqs) = new_prepared_time_series_queries {
            if let Some(tsq) = tsqs.get(context) {
                return self.execute_attach_time_series_query(tsq, &updated_constraints.unwrap())
            }
        }
        if let Some(ConstrainingSolutionMapping{ solution_mapping, datatypes }) = updated_constraints {
            let columns = solution_mapping.get_column_names().iter().map(|x|x.clone()).collect();
            return Ok(LazyGraphPatternReturn::new(solution_mapping.lazy(), columns))
        } else {
            panic!("Should never happen")
        }
    }
}
