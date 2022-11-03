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
    pub(crate) fn lazy_filter(
        &mut self,
        inner: &GraphPattern,
        expression: &Expression,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let mut new_prepared_time_series_queries = prepared_time_series_queries;
        let mut updated_constraints = constraints;
        if let Some(query) = self.static_query_map.get(context) {
            let (static_result_df, datatypes) = self.execute_static_query(
                query,
                &constraints,
            );
            let GPPrepReturn{ time_series_queries, .. } = self.prepper.prepare_filter(expression, inner, false, context);
            new_prepared_time_series_queries = time_series_queries;
            updated_constraints = Some(update_constraints(&mut updated_constraints, static_result_df, datatypes))
        }

        if let Some(tsqs) = new_prepared_time_series_queries {
            if let Some(tsq) = tsqs.get(context) {
                let ts_df = self.time_series_database.execute(tsq).await.map_err(|x|CombinerError::TimeSeriesQueryError(x))?;
                let ts_lf = ts_df.lazy();
                if let Some(ConstrainingSolutionMapping { solution_mapping, .. }) = updated_constraints {
                    let on:Vec<Expr> = tsq.get_identifier_variables().iter().map(|x|col(x.as_str())).collect();
                    let mut lf = solution_mapping.lazy().join(ts_lf, on.as_slice(), on.as_slice(), JoinType::Inner );
                    lf = lazy_expression(expression, inner_lf, columns, time_series, &expression_context);
                    return Ok(LazyGraphPatternReturn::new(lf, columns))
                } else {
                    panic!("Constraining solution mapping always available if tsq is")
                }
            }
        }

        let mut inner_lf = self.lazy_graph_pattern(
            columns,
            input_lf,
            inner,
            &context.extension_with(PathEntry::FilterInner),
        )?;
        let expression_context = context.extension_with(PathEntry::FilterExpression);
        inner_lf = lazy_expression(expression, inner_lf, columns, time_series, &expression_context);
        inner_lf = inner_lf
            .filter(col(&expression_context.as_str()))
            .drop_columns([&expression_context.as_str()]);
        inner_lf
    }
}
