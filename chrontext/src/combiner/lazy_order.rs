use crate::combiner::lazy_expressions::lazy_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{DataFrame, LazyFrame};
use spargebra::algebra::OrderExpression;
use std::collections::{HashMap, HashSet};
use super::Combiner;

impl Combiner {
    pub fn lazy_order_expression(
        &mut self,
        oexpr: &OrderExpression,
        lazy_frame: LazyFrame,
        columns: &HashSet<String>,
        prepared_time_series_queries: &mut Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> (LazyFrame, bool, Context) {
        match oexpr {
            OrderExpression::Asc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                (
                    self.lazy_expression(expr, lazy_frame, columns, prepared_time_series_queries, &inner_context),
                    true,
                    inner_context,
                )
            }
            OrderExpression::Desc(expr) => {
                let inner_context = context.extension_with(PathEntry::OrderingOperation);
                (
                    self.lazy_expression(expr, lazy_frame, columns, time_series, &inner_context),
                    false,
                    inner_context,
                )
            }
        }
    }
}
