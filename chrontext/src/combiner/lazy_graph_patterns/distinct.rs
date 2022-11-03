use std::collections::HashMap;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_distinct(
        &mut self,
        inner: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let LazyGraphPatternReturn{ lf, columns } = self.lazy_graph_pattern(
            columns,
            inner,
            constraints,
            prepared_time_series_queries,
            &context.extension_with(PathEntry::DistinctInner),
        )?;
        Ok(LazyGraphPatternReturn::new(lf.unique_stable(None, UniqueKeepStrategy::First), columns.unwrap()))
    }
}
