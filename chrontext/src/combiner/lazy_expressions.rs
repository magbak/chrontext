mod exists_helper;

use super::Combiner;
use crate::combiner::lazy_expressions::exists_helper::rewrite_exists_graph_pattern;
use crate::constants::{
    DATETIME_AS_NANOS, DATETIME_AS_SECONDS, NANOS_AS_DATETIME, SECONDS_AS_DATETIME,
};
use crate::query_context::{Context, PathEntry};
use crate::sparql_result_to_polars::{
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::vocab::xsd;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::functions::concat_str;
use polars::lazy::dsl::is_not_null;
use polars::prelude::{
    col, lit, Expr, IntoLazy, LazyFrame, LiteralValue, Operator, Series, TimeUnit,
    UniqueKeepStrategy,
};
use polars_core::prelude::IntoSeries;
use spargebra::algebra::{Expression, Function};
use std::collections::{HashMap, HashSet};
use std::ops::{Div, Mul};
use spargebra::Query;
use crate::combiner::CombinerError;
use crate::combiner::solution_mapping::SolutionMappings;

impl Combiner {
    pub async fn lazy_expression(
        &mut self,
        expr: &Expression,
        solution_mappings: SolutionMappings,
        mut static_query_map: Option<HashMap<Context, Query>>,
        mut prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        let lf = match expr {
            Expression::NamedNode(nn) => {
                let inner_lf = inner_lf.with_column(
                    Expr::Literal(sparql_named_node_to_polars_literal_value(nn))
                        .alias(context.as_str()),
                );
                inner_lf
            }
            Expression::Literal(lit) => {
                let inner_lf = inner_lf.with_column(
                    Expr::Literal(sparql_literal_to_polars_literal_value(lit))
                        .alias(context.as_str()),
                );
                inner_lf
            }
            Expression::Variable(v) => {
                let inner_lf = inner_lf.with_column(col(v.as_str()).alias(context.as_str()));
                inner_lf
            }
            Expression::Or(left, right) => {
                let left_context = context.extension_with(PathEntry::OrLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::OrRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Or,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::And(left, right) => {
                let left_context = context.extension_with(PathEntry::AndLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::AndRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::And,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::Equal(left, right) => {
                let left_context = context.extension_with(PathEntry::EqualLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::EqualRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Eq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::SameTerm(_, _) => {
                todo!("Not implemented")
            }
            Expression::Greater(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::GreaterRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Gt,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::GreaterOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::GreaterOrEqualLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::GreaterOrEqualRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::GtEq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::Less(left, right) => {
                let left_context = context.extension_with(PathEntry::LessLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::LessRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Lt,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::LessOrEqual(left, right) => {
                let left_context = context.extension_with(PathEntry::LessOrEqualLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::LessOrEqualRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::LtEq,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::In(left, right) => {
                let left_context = context.extension_with(PathEntry::InLeft);
                let right_contexts: Vec<Context> = (0..right.len())
                    .map(|i| context.extension_with(PathEntry::InRight(i as u16)))
                    .collect();
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                for i in 0..right.len() {
                    let expr = right.get(i).unwrap();
                    inner_lf = self.lazy_expression(
                        expr,
                        inner_lf,
                        columns,
                        prepared_time_series_queries,
                        right_contexts.get(i).unwrap(),
                    );
                }
                let mut expr = Expr::Literal(LiteralValue::Boolean(false));

                for right_context in &right_contexts {
                    expr = Expr::BinaryExpr {
                        left: Box::new(expr),
                        op: Operator::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Eq,
                            right: Box::new(col(right_context.as_str())),
                        }),
                    }
                }
                inner_lf = inner_lf
                    .with_column(expr.alias(context.as_str()))
                    .drop_columns([left_context.as_str()])
                    .drop_columns(
                        right_contexts
                            .iter()
                            .map(|x| x.as_str())
                            .collect::<Vec<&str>>(),
                    );
                inner_lf
            }
            Expression::Add(left, right) => {
                let left_context = context.extension_with(PathEntry::AddLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::AddRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Plus,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::Subtract(left, right) => {
                let left_context = context.extension_with(PathEntry::SubtractLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::SubtractRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Minus,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::Multiply(left, right) => {
                let left_context = context.extension_with(PathEntry::MultiplyLeft);
                let mut inner_lf = self.lazy_expression(
                    left,
                    inner_lf,
                    columns,
                    prepared_time_series_queries,
                    &context.extension_with(PathEntry::MultiplyLeft),
                );
                let right_context = context.extension_with(PathEntry::MultiplyRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Multiply,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::Divide(left, right) => {
                let left_context = context.extension_with(PathEntry::DivideLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let right_context = context.extension_with(PathEntry::DivideRight);
                inner_lf = self.lazy_expression(right, inner_lf, columns, static_query_map, prepared_time_series_queries, &right_context);

                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(col(left_context.as_str())),
                            op: Operator::Divide,
                            right: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([left_context.as_str(), right_context.as_str()]);
                inner_lf
            }
            Expression::UnaryPlus(inner) => {
                let plus_context = context.extension_with(PathEntry::UnaryPlus);
                let mut inner_lf =
                    self.lazy_expression(inner, inner_lf, columns, static_query_map, prepared_time_series_queries, &plus_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Plus,
                            right: Box::new(col(&plus_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([&plus_context.as_str()]);
                inner_lf
            }
            Expression::UnaryMinus(inner) => {
                let minus_context = context.extension_with(PathEntry::UnaryMinus);
                let mut inner_lf =
                    self.lazy_expression(inner, inner_lf, columns, static_query_map, prepared_time_series_queries, &minus_context);
                inner_lf = inner_lf
                    .with_column(
                        (Expr::BinaryExpr {
                            left: Box::new(Expr::Literal(LiteralValue::Int32(0))),
                            op: Operator::Minus,
                            right: Box::new(col(&minus_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([&minus_context.as_str()]);
                inner_lf
            }
            Expression::Not(inner) => {
                let not_context = context.extension_with(PathEntry::Not);
                let mut inner_lf =
                    self.lazy_expression(inner, inner_lf, columns, static_query_map, prepared_time_series_queries, &not_context);
                inner_lf = inner_lf
                    .with_column(col(&not_context.as_str()).not().alias(context.as_str()))
                    .drop_columns([&not_context.as_str()]);
                inner_lf
            }
            Expression::Exists(inner) => {
                let exists_context = context.extension_with(PathEntry::Exists);
                let mut lf = inner_lf.with_column(
                    Expr::Literal(LiteralValue::Int64(1)).alias(&exists_context.as_str()),
                );
                lf = lf
                    .with_column(col(&exists_context.as_str()).cumsum(false).keep_name());
                let constraints = SolutionMappings {
                    mappings: lf,
                    columns: Default::default(),
                    datatypes: Default::default(),
                };
                let new_inner = rewrite_exists_graph_pattern(inner, &exists_context.as_str());
                let exists_lf = self.lazy_graph_pattern(
                    &new_inner,
                    &mut columns.clone(),
                    df.clone().lazy(),
                    &new_inner,
                    prepared_time_series_queries,
                    &exists_context,
                );
                let exists_df = exists_lf
                    .select([col(&exists_context.as_str())])
                    .unique(None, UniqueKeepStrategy::First)
                    .collect()
                    .expect("Collect lazy exists error");
                let mut ser = Series::from(
                    df.column(&exists_context.as_str())
                        .unwrap()
                        .is_in(exists_df.column(&exists_context.as_str()).unwrap())
                        .unwrap(),
                );
                ser.rename(context.as_str());
                df.with_column(ser).unwrap();
                df = df.drop(&exists_context.as_str()).unwrap();
                df.lazy()
            }
            Expression::Bound(v) => {
                inner_lf.with_column(col(v.as_str()).is_null().alias(context.as_str()))
            }
            Expression::If(left, middle, right) => {
                let left_context = context.extension_with(PathEntry::IfLeft);
                let mut inner_lf =
                    self.lazy_expression(left, inner_lf, columns, static_query_map, prepared_time_series_queries, &left_context);
                let middle_context = context.extension_with(PathEntry::IfMiddle);
                inner_lf = self.lazy_expression(middle, inner_lf, columns, static_query_map, prepared_time_series_queries, &middle_context);
                let right_context = context.extension_with(PathEntry::IfRight);
                inner_lf = self.lazy_expression(
                    right,
                    inner_lf,
                    columns,
                    prepared_time_series_queries,
                    &context.extension_with(PathEntry::IfRight),
                );

                inner_lf = inner_lf
                    .with_column(
                        (Expr::Ternary {
                            predicate: Box::new(col(left_context.as_str())),
                            truthy: Box::new(col(middle_context.as_str())),
                            falsy: Box::new(col(right_context.as_str())),
                        })
                        .alias(context.as_str()),
                    )
                    .drop_columns([
                        left_context.as_str(),
                        middle_context.as_str(),
                        right_context.as_str(),
                    ]);
                inner_lf
            }
            Expression::Coalesce(inner) => {
                let inner_contexts: Vec<Context> = (0..inner.len())
                    .map(|i| context.extension_with(PathEntry::Coalesce(i as u16)))
                    .collect();
                let mut inner_lf = inner_lf;
                for i in 0..inner.len() {
                    inner_lf = self.lazy_expression(
                        inner.get(i).unwrap(),
                        inner_lf,
                        columns,
                        prepared_time_series_queries,
                        inner_contexts.get(i).unwrap(),
                    );
                }

                let coalesced_context = inner_contexts.get(0).unwrap();
                let mut coalesced = col(&coalesced_context.as_str());
                for c in &inner_contexts[1..inner_contexts.len()] {
                    coalesced = Expr::Ternary {
                        predicate: Box::new(is_not_null(coalesced.clone())),
                        truthy: Box::new(coalesced.clone()),
                        falsy: Box::new(col(c.as_str())),
                    }
                }
                inner_lf = inner_lf
                    .with_column(coalesced.alias(context.as_str()))
                    .drop_columns(
                        inner_contexts
                            .iter()
                            .map(|c| c.as_str())
                            .collect::<Vec<&str>>(),
                    );
                inner_lf
            }
            Expression::FunctionCall(func, args) => {
                let args_contexts: Vec<Context> = (0..args.len())
                    .map(|i| context.extension_with(PathEntry::FunctionCall(i as u16)))
                    .collect();
                let mut inner_lf = inner_lf;
                for i in 0..args.len() {
                    inner_lf = self.lazy_expression(
                        args.get(i).unwrap(),
                        inner_lf,
                        columns,
                        prepared_time_series_queries,
                        args_contexts.get(i).unwrap(),
                    )
                    .collect()
                    .unwrap()
                    .lazy(); //TODO: workaround for stack overflow - post bug?
                }
                match func {
                    Function::Year => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .dt()
                                .year()
                                .alias(context.as_str()),
                        );
                    }
                    Function::Month => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .dt()
                                .month()
                                .alias(context.as_str()),
                        );
                    }
                    Function::Day => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .dt()
                                .day()
                                .alias(context.as_str()),
                        );
                    }
                    Function::Hours => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .dt()
                                .hour()
                                .alias(context.as_str()),
                        );
                    }
                    Function::Minutes => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .dt()
                                .minute()
                                .alias(context.as_str()),
                        );
                    }
                    Function::Seconds => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .dt()
                                .second()
                                .alias(context.as_str()),
                        );
                    }
                    Function::Abs => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str()).abs().alias(context.as_str()),
                        );
                    }
                    Function::Ceil => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str()).ceil().alias(context.as_str()),
                        );
                    }
                    Function::Floor => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str()).floor().alias(context.as_str()),
                        );
                    }
                    Function::Concat => {
                        assert!(args.len() > 1);
                        let mut inner_df = inner_lf.collect().unwrap();
                        let series = args_contexts
                            .iter()
                            .map(|c| inner_df.column(c.as_str()).unwrap().clone())
                            .collect::<Vec<Series>>();
                        let mut concat_series =
                            concat_str(series.as_slice(), "").unwrap().into_series();
                        concat_series.rename(context.as_str());
                        inner_df.with_column(concat_series).unwrap();
                        inner_lf = inner_df.lazy();
                    }
                    Function::Round => {
                        assert_eq!(args.len(), 1);
                        let first_context = args_contexts.get(0).unwrap();
                        inner_lf = inner_lf.with_column(
                            col(&first_context.as_str())
                                .round(0)
                                .alias(context.as_str()),
                        );
                    }
                    Function::Custom(nn) => {
                        let iri = nn.as_str();
                        if iri == xsd::INTEGER.as_str() {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            inner_lf = inner_lf.with_column(
                                col(&first_context.as_str())
                                    .cast(DataType::Int64)
                                    .alias(context.as_str()),
                            );
                        } else if iri == xsd::STRING.as_str() {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            inner_lf = inner_lf.with_column(
                                col(&first_context.as_str())
                                    .cast(DataType::Utf8)
                                    .alias(context.as_str()),
                            );
                        } else if iri == DATETIME_AS_NANOS {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            inner_lf = inner_lf.with_column(
                                col(&first_context.as_str())
                                    .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                                    .cast(DataType::UInt64)
                                    .alias(context.as_str()),
                            );
                        } else if iri == DATETIME_AS_SECONDS {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            inner_lf = inner_lf.with_column(
                                col(&first_context.as_str())
                                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                    .cast(DataType::UInt64)
                                    .div(lit(1000))
                                    .alias(context.as_str()),
                            );
                        } else if iri == NANOS_AS_DATETIME {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            inner_lf = inner_lf.with_column(
                                col(&first_context.as_str())
                                    .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                                    .alias(context.as_str()),
                            );
                        } else if iri == SECONDS_AS_DATETIME {
                            assert_eq!(args.len(), 1);
                            let first_context = args_contexts.get(0).unwrap();
                            inner_lf = inner_lf.with_column(
                                col(&first_context.as_str())
                                    .mul(Expr::Literal(LiteralValue::UInt64(1000)))
                                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                    .alias(context.as_str()),
                            );
                        } else {
                            todo!("{:?}", nn)
                        }
                    }
                    _ => {
                        todo!()
                    }
                }
                inner_lf.drop_columns(
                    args_contexts
                        .iter()
                        .map(|x| x.as_str())
                        .collect::<Vec<&str>>(),
                )
            }
        };
        lf
    }
}
