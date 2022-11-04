use std::collections::HashMap;
use log::debug;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::{col, Expr, LazyFrame, LiteralValue};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use crate::combiner::CombinerError;
use crate::combiner::static_subqueries::split_static_queries;
use crate::combiner::time_series_queries::split_time_series_queries;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        solution_mapping: Option<SolutionMapping>,
        mut static_query_map: HashMap<Context, Query>,
        mut prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<SolutionMapping, CombinerError> {
        let left_prepared_time_series_queries = split_time_series_queries(&mut prepared_time_series_queries, &left_context);
        let right_prepared_time_series_queries = split_time_series_queries(&mut prepared_time_series_queries, &right_context);
        let left_static_query_map = split_static_queries(&mut static_query_map, &left_context);
        let right_static_query_map = split_static_queries(&mut static_query_map, &right_context);
        assert!(static_query_map.is_empty());
        assert!(
            if let Some(tsqs) = &prepared_time_series_queries {
                tsqs.is_empty()
            } else {
                true
            }
        );
        let minus_column = "minus_column".to_string() + self.counter.to_string().as_str();
        self.counter += 1;
        debug!("Left graph pattern {}", left);
        let mut left_df = self
            .lazy_graph_pattern(
                left,
                input_lf,
                left_static_query_map,
                left_prepared_time_series_queries,
                &context.extension_with(PathEntry::MinusLeftSide),
            )
            .with_column(Expr::Literal(LiteralValue::Int64(1)).alias(&minus_column))
            .with_column(col(&minus_column).cumsum(false).keep_name())
            .collect()
            .expect("Minus collect left problem");

        debug!("Minus left hand side: {:?}", left_df);
        //TODO: determine only variables actually used before copy
        let right_df = self
            .lazy_graph_pattern(
                right,
                left_df.clone().lazy(),
                right_static_query_map,
                right_prepared_time_series_queries,
                &context.extension_with(PathEntry::MinusRightSide),
            )
            .select([col(&minus_column)])
            .collect()
            .expect("Minus right df collect problem");
        left_df = left_df
            .filter(
                &left_df
                    .column(&minus_column)
                    .unwrap()
                    .is_in(right_df.column(&minus_column).unwrap())
                    .unwrap()
                    .not(),
            )
            .expect("Filter minus left hand side problem");
        left_df.drop(&minus_column).unwrap().lazy()
    }
}
