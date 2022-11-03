use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::LazyFrame;
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;

impl Combiner {
    pub(crate) fn lazy_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let left_lf = self.lazy_graph_pattern(
            columns,
            input_lf,
            left,
            prepared_time_series_queries,
            &context.extension_with(PathEntry::JoinLeftSide),
        )?;
        let right_lf = self.lazy_graph_pattern(
            columns,
            left_lf,
            right,
            prepared_time_series_queries,
            &context.extension_with(PathEntry::JoinRightSide),
        );
        right_lf
    }
}
