use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{DataFrame, LazyFrame};
use spargebra::algebra::OrderExpression;
use std::collections::{HashMap, HashSet};
use crate::combiner::solution_mapping::SolutionMappings;
use super::Combiner;

impl Combiner {
    pub fn lazy_order_expression(
        &mut self,
        oexpr: &OrderExpression,
        solution_mappings: SolutionMappings,
        context: &Context,
    ) -> (SolutionMappings, bool, Context) {
        match oexpr {
            OrderExpression::Asc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                (
                    self.lazy_expression(expr, lazy_frame, None, None, &inner_context),
                    true,
                    inner_context,
                )
            }
            OrderExpression::Desc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                (
                    self.lazy_expression(expr, lazy_frame, columns, prepared_time_series_queries, &inner_context),
                    false,
                    inner_context,
                )
            }
        }
    }
}
