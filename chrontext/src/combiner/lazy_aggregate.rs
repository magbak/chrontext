use crate::constants::NEST;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use polars::prelude::{col, DataFrame, DataType, Expr, GetOutput, IntoSeries, LazyFrame};
use spargebra::algebra::AggregateExpression;
use std::collections::HashSet;
use super::Combiner;

impl Combiner {
    pub fn sparql_aggregate_expression_as_lazy_column_and_expression(
        &mut self,
        variable: &Variable,
        aggregate_expression: &AggregateExpression,
        all_proper_column_names: &Vec<String>,
        columns: &HashSet<String>,
        lf: LazyFrame,
        time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
        context: &Context,
    ) -> (LazyFrame, Expr, Option<Context>) {
        let out_lf;
        let mut out_expr;
        let column_context;
        match aggregate_expression {
            AggregateExpression::Count { expr, distinct } => {
                if let Some(some_expr) = expr {
                    column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                    out_lf = self.lazy_expression(
                        some_expr,
                        lf,
                        columns,
                        time_series,
                        column_context.as_ref().unwrap(),
                    );
                    if *distinct {
                        out_expr = col(column_context.as_ref().unwrap().as_str()).n_unique();
                    } else {
                        out_expr = col(column_context.as_ref().unwrap().as_str()).count();
                    }
                } else {
                    out_lf = lf;
                    column_context = None;

                    let columns_expr = Expr::Columns(all_proper_column_names.clone());
                    if *distinct {
                        out_expr = columns_expr.n_unique();
                    } else {
                        out_expr = columns_expr.unique();
                    }
                }
            }
            AggregateExpression::Sum { expr, distinct } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                out_lf = self.lazy_expression(
                    expr,
                    lf,
                    columns,
                    time_series,
                    column_context.as_ref().unwrap(),
                );

                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .unique()
                        .sum();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str()).sum();
                }
            }
            AggregateExpression::Avg { expr, distinct } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));
                out_lf = self.lazy_expression(
                    expr,
                    lf,
                    columns,
                    time_series,
                    column_context.as_ref().unwrap(),
                );

                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .unique()
                        .mean();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str()).mean();
                }
            }
            AggregateExpression::Min { expr, distinct: _ } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                out_lf = self.lazy_expression(
                    expr,
                    lf,
                    columns,
                    time_series,
                    column_context.as_ref().unwrap(),
                );

                out_expr = col(column_context.as_ref().unwrap().as_str()).min();
            }
            AggregateExpression::Max { expr, distinct: _ } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                out_lf = self.lazy_expression(
                    expr,
                    lf,
                    columns,
                    time_series,
                    column_context.as_ref().unwrap(),
                );

                out_expr = col(column_context.as_ref().unwrap().as_str()).max();
            }
            AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                out_lf = self.lazy_expression(
                    expr,
                    lf,
                    columns,
                    time_series,
                    column_context.as_ref().unwrap(),
                );

                let use_sep = if let Some(sep) = separator {
                    sep.to_string()
                } else {
                    "".to_string()
                };
                if *distinct {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .cast(DataType::Utf8)
                        .list()
                        .apply(
                            move |s| {
                                Ok(s.unique_stable()
                                    .expect("Unique stable error")
                                    .str_concat(use_sep.as_str())
                                    .into_series())
                            },
                            GetOutput::from_type(DataType::Utf8),
                        )
                        .first();
                } else {
                    out_expr = col(column_context.as_ref().unwrap().as_str())
                        .cast(DataType::Utf8)
                        .list()
                        .apply(
                            move |s| Ok(s.str_concat(use_sep.as_str()).into_series()),
                            GetOutput::from_type(DataType::Utf8),
                        )
                        .first();
                }
            }
            AggregateExpression::Sample { expr, .. } => {
                column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                out_lf = self.lazy_expression(
                    expr,
                    lf,
                    columns,
                    time_series,
                    column_context.as_ref().unwrap(),
                );

                out_expr = col(column_context.as_ref().unwrap().as_str()).first();
            }
            AggregateExpression::Custom {
                name,
                expr,
                distinct: _,
            } => {
                let iri = name.as_str();
                if iri == NEST {
                    column_context = Some(context.extension_with(PathEntry::AggregationOperation));

                    out_lf = self.lazy_expression(
                        expr,
                        lf,
                        columns,
                        time_series,
                        column_context.as_ref().unwrap(),
                    );
                    out_expr = col(column_context.as_ref().unwrap().as_str()).list();
                } else {
                    panic!("Custom aggregation not supported")
                }
            }
        }
        out_expr = out_expr.alias(variable.as_str());
        (out_lf, out_expr, column_context)
    }
}