use std::collections::HashMap;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::{concat, LazyFrame};
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let mut left_columns = columns.clone();
        let original_timeseries_columns = get_timeseries_identifier_names(time_series);
        let mut left_lf = self.lazy_graph_pattern(
            &mut left_columns,
            input_lf.clone(),
            left,
            &context.extension_with(PathEntry::UnionLeftSide),
        );
        let mut right_columns = columns.clone();
        let mut right_input_lf = input_lf;
        for t in &original_timeseries_columns {
            if !left_columns.contains(t) {
                right_columns.remove(t);
                right_input_lf = right_input_lf.drop_columns([t]);
            }
        }
        let right_lf = self.lazy_graph_pattern(
            &mut right_columns,
            right_input_lf,
            right,
            &context.extension_with(PathEntry::UnionRightSide),
        );

        for t in &original_timeseries_columns {
            if !right_columns.contains(t) {
                left_columns.remove(t);
                left_lf = left_lf.drop_columns([t]);
            }
        }
        left_columns.extend(right_columns.drain());
        let original_columns: Vec<String> = columns.iter().cloned().collect();
        for o in original_columns {
            if !left_columns.contains(&o) {
                columns.remove(&o);
            }
        }
        columns.extend(left_columns.drain());

        let output_lf = concat(vec![left_lf, right_lf], true, true).expect("Concat problem");
        output_lf
            .unique(None, UniqueKeepStrategy::First)
            .collect()
            .expect("Union error")
            .lazy()
    }
}
