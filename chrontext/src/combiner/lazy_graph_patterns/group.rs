use std::collections::HashMap;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::{NamedNode, Variable};
use polars::prelude::{col, Expr, IntoLazy, LazyFrame};
use polars_core::prelude::JoinType;
use spargebra::algebra::{AggregateExpression, GraphPattern};
use crate::combiner::{CombinerError, get_timeseries_identifier_names};
use crate::combiner::constraining_solution_mapping::{ConstrainingSolutionMapping, update_constraints};
use crate::combiner::lazy_aggregate::sparql_aggregate_expression_as_lazy_column_and_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::lf_wrap::WrapLF;

impl Combiner {
    pub(crate) fn lazy_group(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyFrame, CombinerError> {
        let mut new_prepared_time_series_queries = prepared_time_series_queries;
        let mut updated_constraints = constraints;
        if let Some(query) = self.static_query_map.get(&context) {
            let (static_result_df, datatypes) = self.execute_static_query(
                query,
                &constraints,
            );
            let mut wrap_lf = WrapLF::new(static_result_df.lazy());
            let GPPrepReturn{ time_series_queries, .. } = self
                .prepper
                .prepare_group(inner, by, aggregates, false, &mut wrap_lf, &context);
            new_prepared_time_series_queries = time_series_queries;
            updated_constraints = Some(update_constraints(&mut updated_constraints, wrap_lf.lf.collect().unwrap(), datatypes))
        }

        if let Some(tsqs) = &prepared_time_series_queries {
            if let Some(tsq) = tsqs.get(context) {
                let ts_df = self.time_series_database.execute(tsq).await.map_err(|x|CombinerError::TimeSeriesQueryError(x))?;
                let ts_lf = ts_df.lazy();
                assert!(updated_constraints.is_some());
                if let Some(ConstrainingSolutionMapping { solution_mapping, .. }) = updated_constraints {
                    let on = tsq.get_groupby_column().unwrap();
                    let lf = solution_mapping.lazy().join(ts_lf, [on], [on], JoinType::Inner );
                    return Ok(lf)
                }
            }
        }

        let inner_context = context.extension_with(PathEntry::GroupInner);
        let (mut lazy_inner, mut columns) = self.lazy_graph_pattern(columns, output_lf, inner, &inner_context);
        let by: Vec<Expr> = variables.iter().map(|v| col(v.as_str())).collect();

        let mut aggregate_expressions = vec![];
        let mut aggregate_inner_contexts = vec![];
        for i in 0..aggregates.len() {
            let aggregate_context = context.extension_with(PathEntry::GroupAggregation(i as u16));
            let (v, a) = aggregates.get(i).unwrap();
            let (lf, expr, used_context) =
                sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    a,
                    &column_variables,
                    columns,
                    lazy_inner,
                    time_series,
                    &aggregate_context,
                );
            lazy_inner = lf;
            aggregate_expressions.push(expr);
            if let Some(aggregate_inner_context) = used_context {
                aggregate_inner_contexts.push(aggregate_inner_context);
            }
        }

        let lazy_group_by = lazy_inner.groupby(by.as_slice());

        let aggregated_lf = lazy_group_by
            .agg(aggregate_expressions.as_slice())
            .drop_columns(
                aggregate_inner_contexts
                    .iter()
                    .map(|x| x.as_str())
                    .collect::<Vec<&str>>(),
            );
        columns.clear();
        for v in variables {
            columns.insert(v.as_str().to_string());
        }
        for (v, _) in aggregates {
            columns.insert(v.as_str().to_string());
        }
        Ok(aggregated_lf)
    }
}
