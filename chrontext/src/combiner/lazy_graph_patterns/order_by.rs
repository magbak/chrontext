use std::collections::HashMap;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::{col, Expr, LazyFrame};
use spargebra::algebra::{GraphPattern, OrderExpression};
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::combiner::lazy_order::lazy_order_expression;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_order_by(
        &mut self,
        inner: &GraphPattern,
        expression: &Vec<OrderExpression>,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let mut inner_lf = self.lazy_graph_pattern(
                    columns,
                    input_lf,
                    inner,
                    &context.extension_with(PathEntry::OrderByInner),
                );
                let order_expression_contexts: Vec<Context> = (0..expression.len())
                    .map(|i| context.extension_with(PathEntry::OrderByExpression(i as u16)))
                    .collect();
                let mut asc_ordering = vec![];
                let mut inner_contexts = vec![];
                for i in 0..expression.len() {
                    let (lf, reverse, inner_context) = lazy_order_expression(
                        expression.get(i).unwrap(),
                        inner_lf,
                        columns,
                        time_series,
                        order_expression_contexts.get(i).unwrap(),
                    );
                    inner_lf = lf;
                    inner_contexts.push(inner_context);
                    asc_ordering.push(reverse);
                }
                inner_lf = inner_lf.sort_by_exprs(
                    inner_contexts
                        .iter()
                        .map(|c| col(c.as_str()))
                        .collect::<Vec<Expr>>(),
                    asc_ordering.iter().map(|asc| !asc).collect::<Vec<bool>>(),
                    true,
                );
                inner_lf = inner_lf.drop_columns(
                    inner_contexts
                        .iter()
                        .map(|x| x.as_str())
                        .collect::<Vec<&str>>(),
                );
                inner_lf
    }
}

