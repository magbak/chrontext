mod join_timeseries;
pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
mod lazy_order;
mod lazy_triple;

use crate::combiner::join_timeseries::join_tsq;
use crate::combiner::lazy_aggregate::sparql_aggregate_expression_as_lazy_column_and_expression;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::combiner::lazy_order::lazy_order_expression;
use crate::combiner::lazy_triple::lazy_triple_pattern;
use crate::query_context::{Context, PathEntry};

use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use oxrdf::Variable;
use polars::frame::DataFrame;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LiteralValue, UniqueKeepStrategy};
use spargebra::algebra::{AggregateExpression, GraphPattern};
use spargebra::Query;
use std::collections::HashSet;
use std::ops::Not;

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

    fn lazy_graph_pattern(
        &mut self,
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        graph_pattern: &GraphPattern,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context,
    ) -> LazyFrame {
        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                //No action, handled statically
                let mut output_lf = input_lf;
                let bgp_context = context.extension_with(PathEntry::BGP);
                for p in patterns {
                    output_lf =
                        lazy_triple_pattern(columns, output_lf, p, time_series, &bgp_context);
                }
                output_lf
            }
            GraphPattern::Path { .. } => {
                //No action, handled statically
                input_lf
            }
            GraphPattern::Join { left, right } => {
                let left_lf = self.lazy_graph_pattern(
                    columns,
                    input_lf,
                    left,
                    time_series,
                    &context.extension_with(PathEntry::JoinLeftSide),
                );
                let right_lf = self.lazy_graph_pattern(
                    columns,
                    left_lf,
                    right,
                    time_series,
                    &context.extension_with(PathEntry::JoinRightSide),
                );
                right_lf
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let left_join_distinct_column = context.as_str();
                let mut left_df = self
                    .lazy_graph_pattern(
                        columns,
                        input_lf,
                        left,
                        time_series,
                        &context.extension_with(PathEntry::LeftJoinLeftSide),
                    )
                    .with_column(
                        Expr::Literal(LiteralValue::Int64(1)).alias(&left_join_distinct_column),
                    )
                    .with_column(col(&left_join_distinct_column).cumsum(false).keep_name())
                    .collect()
                    .expect("Left join collect left problem");

                let ts_identifiers = get_timeseries_identifier_names(time_series);
                let mut right_lf = self.lazy_graph_pattern(
                    columns,
                    left_df.clone().lazy(),
                    right,
                    time_series,
                    &context.extension_with(PathEntry::LeftJoinRightSide),
                );

                if let Some(expr) = expression {
                    let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
                    right_lf =
                        lazy_expression(expr, right_lf, columns, time_series, &expression_context);
                    right_lf = right_lf
                        .filter(col(&expression_context.as_str()))
                        .drop_columns([&expression_context.as_str()]);
                }

                let right_df = right_lf.collect().expect("Collect right problem");

                for id in ts_identifiers {
                    if !columns.contains(&id) {
                        left_df = left_df.drop(&id).expect("Drop problem");
                    }
                }
                left_df = left_df
                    .filter(
                        &left_df
                            .column(&left_join_distinct_column)
                            .expect("Did not find left helper")
                            .is_in(
                                right_df
                                    .column(&left_join_distinct_column)
                                    .expect("Did not find right helper"),
                            )
                            .expect("Is in problem")
                            .not(),
                    )
                    .expect("Filter problem");

                for c in right_df.get_column_names_owned().iter() {
                    if !left_df.get_column_names().contains(&c.as_str()) {
                        left_df = left_df
                            .lazy()
                            .with_column(Expr::Literal(LiteralValue::Null).alias(c))
                            .collect()
                            .expect("Not ok");
                        left_df
                            .with_column(
                                left_df
                                    .column(c)
                                    .expect("Col c prob")
                                    .cast(right_df.column(c).unwrap().dtype())
                                    .expect("Cast error"),
                            )
                            .expect("TODO: panic message");
                    }
                }

                let mut output_lf =
                    concat(vec![left_df.lazy(), right_df.lazy()], true, true).expect("Concat error");
                output_lf = output_lf.drop_columns(&[&left_join_distinct_column]);
                output_lf = output_lf
                    .collect()
                    .expect("Left join collect problem")
                    .lazy();
                output_lf
            }
            GraphPattern::Filter { expr, inner } => {
                let mut inner_lf = self.lazy_graph_pattern(
                    columns,
                    input_lf,
                    inner,
                    time_series,
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
            GraphPattern::Union { left, right } => {
                let mut left_columns = columns.clone();
                let original_timeseries_columns = get_timeseries_identifier_names(time_series);
                let mut left_lf = self.lazy_graph_pattern(
                    &mut left_columns,
                    input_lf.clone(),
                    left,
                    time_series,
                    &context.extension_with(PathEntry::UnionLeftSide),
                );
                let mut right_columns = columns.clone();
                let mut right_input_lf = input_lf;
                for t in &original_timeseries_columns {
                    if !left_columns.contains(t) {
                        right_columns.remove(t);
                        right_input_lf = right_input_lf.drop_columns([t]);
                    }
                }
                let right_lf = self.lazy_graph_pattern(
                    &mut right_columns,
                    right_input_lf,
                    right,
                    time_series,
                    &context.extension_with(PathEntry::UnionRightSide),
                );

                for t in &original_timeseries_columns {
                    if !right_columns.contains(t) {
                        left_columns.remove(t);
                        left_lf = left_lf.drop_columns([t]);
                    }
                }
                left_columns.extend(right_columns.drain());
                let original_columns: Vec<String> = columns.iter().cloned().collect();
                for o in original_columns {
                    if !left_columns.contains(&o) {
                        columns.remove(&o);
                    }
                }
                columns.extend(left_columns.drain());

                let output_lf = concat(vec![left_lf, right_lf], true, true).expect("Concat problem");
                output_lf
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .expect("Union error")
                    .lazy()
            }
            GraphPattern::Graph { name: _, inner } => self.lazy_graph_pattern(
                columns,
                input_lf,
                inner,
                time_series,
                &context.extension_with(PathEntry::GraphInner),
            ),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                let inner_context = context.extension_with(PathEntry::ExtendInner);
                let mut inner_lf =
                    self.lazy_graph_pattern(columns, input_lf, inner, time_series, &inner_context);
                if !columns.contains(variable.as_str()) {
                    inner_lf =
                        lazy_expression(expression, inner_lf, columns, time_series, &inner_context)
                            .rename([inner_context.as_str()], &[variable.as_str()]);
                    columns.insert(variable.as_str().to_string());
                }
                inner_lf
            }
            GraphPattern::Minus { left, right } => {
                let minus_column = "minus_column".to_string() + self.counter.to_string().as_str();
                self.counter += 1;
                debug!("Left graph pattern {}", left);
                let mut left_df = self
                    .lazy_graph_pattern(
                        columns,
                        input_lf,
                        left,
                        time_series,
                        &context.extension_with(PathEntry::MinusLeftSide),
                    )
                    .with_column(Expr::Literal(LiteralValue::Int64(1)).alias(&minus_column))
                    .with_column(col(&minus_column).cumsum(false).keep_name())
                    .collect()
                    .expect("Minus collect left problem");

                debug!("Minus left hand side: {:?}", left_df);
                //TODO: determine only variables actually used before copy
                let right_df = self
                    .lazy_graph_pattern(
                        columns,
                        left_df.clone().lazy(),
                        right,
                        time_series,
                        &context.extension_with(PathEntry::MinusRightSide),
                    )
                    .select([col(&minus_column)])
                    .collect()
                    .expect("Minus right df collect problem");
                left_df = left_df
                    .filter(
                        &left_df
                            .column(&minus_column)
                            .unwrap()
                            .is_in(right_df.column(&minus_column).unwrap())
                            .unwrap()
                            .not(),
                    )
                    .expect("Filter minus left hand side problem");
                left_df.drop(&minus_column).unwrap().lazy()
            }
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => {
                //These are handled by the static query.
                input_lf
            }
            GraphPattern::OrderBy { inner, expression } => {
                let mut inner_lf = self.lazy_graph_pattern(
                    columns,
                    input_lf,
                    inner,
                    time_series,
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
            GraphPattern::Project { inner, variables } => {
                let inner_lf = self.lazy_graph_pattern(
                    columns,
                    input_lf,
                    inner,
                    time_series,
                    &context.extension_with(PathEntry::ProjectInner),
                );
                let mut cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
                for ts_identifier_variable_name in get_timeseries_identifier_names(time_series) {
                    cols.push(col(&ts_identifier_variable_name));
                }
                inner_lf.select(cols.as_slice())
            }
            GraphPattern::Distinct { inner } => self
                .lazy_graph_pattern(
                    columns,
                    input_lf,
                    inner,
                    time_series,
                    &context.extension_with(PathEntry::DistinctInner),
                )
                .unique_stable(None, UniqueKeepStrategy::First),
            GraphPattern::Reduced { .. } => {
                todo!()
            }
            GraphPattern::Slice { .. } => {
                todo!()
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let mut found_index = None;
                for i in 0..time_series.len() {
                    let (tsq, _) = time_series.get(i).as_ref().unwrap();
                    if let TimeSeriesQuery::Grouped(g) = &tsq {
                        if context == &g.graph_pattern_context {
                            found_index = Some(i);
                        }
                    }
                }
                if let Some(index) = found_index {
                    let (tsq, df) = time_series.remove(index);
                    join_tsq(columns, input_lf, tsq, df)
                } else {
                    let lf = input_lf.collect().unwrap().lazy(); //Workaround for stack overflow
                    self.lazy_group_without_pushdown(
                        columns,
                        lf,
                        inner,
                        variables,
                        aggregates,
                        time_series,
                        context,
                    )
                }
            }
            GraphPattern::Service { .. } => {
                todo!()
            }
        }
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
