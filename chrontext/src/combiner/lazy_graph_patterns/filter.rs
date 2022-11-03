use std::collections::HashMap;
use super::Combiner;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{col, Expr, IntoLazy, LazyFrame};
use polars_core::prelude::JoinType;
use spargebra::algebra::{Expression, GraphPattern};
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::{ConstrainingSolutionMapping, update_constraints};
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::preparing::graph_patterns::GPPrepReturn;

impl Combiner {
    pub(crate) async fn lazy_filter(
        &mut self,
        inner: &GraphPattern,
        expression: &Expression,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let mut inner_lf = self.lazy_graph_pattern(
            columns,
            constraints,
            prepared_time_series_queries,
            &context.extension_with(PathEntry::FilterInner),
        ).await?;
        let expression_context = context.extension_with(PathEntry::FilterExpression);
        inner_lf = lazy_expression(expression, inner_lf, columns, time_series, &expression_context);
        inner_lf = inner_lf
            .filter(col(&expression_context.as_str()))
            .drop_columns([&expression_context.as_str()]);
        inner_lf
    }
}
