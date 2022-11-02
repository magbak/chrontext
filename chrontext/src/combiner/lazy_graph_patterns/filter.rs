use polars::prelude::LazyFrame;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use super::Combiner;

impl Combiner {
    pub(crate) fn lazy_filter(&mut self, input_lf:Option<LazyFrame>, context:&Context) -> LazyFrame {
        let mut inner_lf = self.lazy_graph_pattern(
                    columns,
                    input_lf,
                    inner,
                    &context.extension_with(PathEntry::FilterInner),
                );
                let expression_context = context.extension_with(PathEntry::FilterExpression);
                inner_lf =
                    lazy_expression(expr, inner_lf, columns, time_series, &expression_context);
                inner_lf = inner_lf
                    .filter(col(&expression_context.as_str()))
                    .drop_columns([&expression_context.as_str()]);
                inner_lf
    }
}
