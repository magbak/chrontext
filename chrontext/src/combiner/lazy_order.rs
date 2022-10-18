use crate::combiner::lazy_expressions::lazy_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{DataFrame, LazyFrame};
use spargebra::algebra::OrderExpression;
use std::collections::HashSet;

pub fn lazy_order_expression(
    oexpr: &OrderExpression,
    lazy_frame: LazyFrame,
    columns: &HashSet<String>,
    time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
    context: &Context,
) -> (LazyFrame, bool, Context) {
    match oexpr {
        OrderExpression::Asc(expr) => {
            let inner_context = context.extension_with(PathEntry::OrderingOperation);
            (
                lazy_expression(expr, lazy_frame, columns, time_series, &inner_context),
                true,
                inner_context,
            )
        }
        OrderExpression::Desc(expr) => {
            let inner_context = context.extension_with(PathEntry::OrderingOperation);
            (
                lazy_expression(expr, lazy_frame, columns, time_series, &inner_context),
                false,
                inner_context,
            )
        }
    }
}
