use std::collections::HashMap;
use log::debug;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use spargebra::algebra::GraphPattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let minus_column = "minus_column".to_string() + self.counter.to_string().as_str();
        self.counter += 1;
        debug!("Left graph pattern {}", left);
        let mut left_df = self
            .lazy_graph_pattern(
                columns,
                input_lf,
                left,
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
                columns,
                left_df.clone().lazy(),
                right,
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
