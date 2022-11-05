use super::TimeSeriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use log::debug;
use spargebra::algebra::GraphPattern;
use crate::combiner::solution_mapping::SolutionMappings;

impl TimeSeriesQueryPrepper {
    pub fn prepare_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        try_groupby_complex_query: bool,
        solution_mappings: &mut SolutionMappings,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!(
                "Encountered union inside left join, not supported for complex groupby pushdown"
            );
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let mut left_prepare = self.prepare_graph_pattern(
                left,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::UnionLeftSide),
            );
            let mut right_prepare = self.prepare_graph_pattern(
                right,
                try_groupby_complex_query,
                solution_mappings,
                &context.extension_with(PathEntry::UnionRightSide),
            );
            left_prepare.with_time_series_queries_from(right_prepare);
            left_prepare
        }
    }
}
