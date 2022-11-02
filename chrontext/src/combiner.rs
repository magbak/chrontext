mod join_timeseries;
pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
mod lazy_order;
mod lazy_triple;
mod lazy_graph_patterns;

use crate::combiner::lazy_aggregate::sparql_aggregate_expression_as_lazy_column_and_expression;
use crate::query_context::{Context, PathEntry};

use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use polars::frame::DataFrame;
use polars::prelude::{col, Expr, IntoLazy, LazyFrame, UniqueKeepStrategy};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use spargebra::Query;
use std::collections::HashSet;

pub struct Combiner {
    counter: u16,
}

impl Combiner {
    pub fn new() -> Combiner {
        Combiner { counter: 0 }
    }

    pub fn combine_static_and_time_series_results(
        &mut self,
        query: &Query,
        static_result_df: DataFrame,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
    ) -> LazyFrame {
        let project_variables;
        let inner_graph_pattern;
        let mut distinct = false;
        let mut context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = query
        {
            if let GraphPattern::Project { inner, variables } = pattern {
                project_variables = variables.clone();
                inner_graph_pattern = inner;
                context = context.extension_with(PathEntry::ProjectInner);
            } else if let GraphPattern::Distinct { inner } = pattern {
                context = context.extension_with(PathEntry::DistinctInner);
                if let GraphPattern::Project { inner, variables } = inner.as_ref() {
                    distinct = true;
                    project_variables = variables.clone();
                    inner_graph_pattern = inner;
                    context = context.extension_with(PathEntry::ProjectInner);
                } else {
                    panic!("Wrong!");
                }
            } else {
                panic!("Also wrong!");
            }
        } else {
            panic!("Wrong!!!");
        }
        let mut columns = static_result_df
            .get_column_names()
            .iter()
            .map(|c| c.to_string())
            .collect();

        let mut lf = static_result_df.lazy();
        lf = self.lazy_graph_pattern(&mut columns, lf, inner_graph_pattern, time_series, &context);
        let projections = project_variables
            .iter()
            .map(|c| col(c.as_str()))
            .collect::<Vec<Expr>>();
        lf = lf.select(projections.as_slice());
        if distinct {
            lf = lf.unique_stable(None, UniqueKeepStrategy::First);
        }
        lf
    }

    fn lazy_group_without_pushdown(
        &mut self,
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        inner: &Box<GraphPattern>,
        variables: &Vec<Variable>,
        aggregates: &Vec<(Variable, AggregateExpression)>,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context,
    ) -> LazyFrame {
        let mut lazy_inner = self.lazy_graph_pattern(
            columns,
            input_lf,
            inner,
            time_series,
            &context.extension_with(PathEntry::GroupInner),
        );
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

fn get_timeseries_identifier_names(
    time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
) -> Vec<String> {
    time_series.iter().fold(vec![], |mut coll, (tsq, _)| {
        coll.extend(
            tsq.get_identifier_variables()
                .iter()
                .map(|x| x.as_str().to_string()),
        );
        coll
    })
}
