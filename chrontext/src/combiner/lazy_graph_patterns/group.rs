use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use polars::prelude::LazyFrame;
use spargebra::algebra::{AggregateExpression, GraphPattern};

impl Combiner {
    pub(crate) fn lazy_group(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        input_lf: Option<LazyFrame>,
        context: &Context,
    ) -> LazyFrame {
        let inner_context = context.extension_with(PathEntry::GroupInner);
        if self.static_query_map.contains_key(&inner_context) {
            let static_result_df = self.execute_static_query(
                self.static_query_map.get(&inner_context).unwrap(),
                &input_lf,
            );
            let gprepreturn = self
                .prepper
                .prepare_group(inner, by, aggregates, false, &context);

        }

        let mut lazy_inner = self.lazy_graph_pattern(columns, input_lf, inner, &inner_context);
        let by: Vec<Expr> = variables.iter().map(|v| col(v.as_str())).collect();

        let time_series_identifier_names = get_timeseries_identifier_names(time_series);
        let mut column_variables = vec![];
        for v in columns.iter() {
            if time_series_identifier_names.contains(v) {
                continue;
            }
            column_variables.push(v.clone());
        }
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
        aggregated_lf
    }
}
