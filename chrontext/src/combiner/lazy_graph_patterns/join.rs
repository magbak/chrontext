use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::GraphPattern;
use std::collections::HashMap;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;

impl Combiner {
    pub(crate) fn lazy_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result< ConstrainingSolutionMapping, CombinerError> {
        let left_prepared_time_series_queries;
        let right_prepared_time_series_queries;
        let left_context = context.extension_with(PathEntry::JoinLeftSide);
        let right_context = context.extension_with(PathEntry::JoinRightSide);
        let left_lf = self.lazy_graph_pattern(
            left,
            constraints,
            prepared_time_series_queries,
            &left_context,
        )?;
        let right_lf = self.lazy_graph_pattern(
            right,
            Some(left_lf),
            prepared_time_series_queries,
            &right_context,
        );
        right_lf
    }
}
