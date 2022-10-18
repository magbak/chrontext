use super::Translator;
use crate::ast::TsQuery;
use crate::costants::{
    DATETIME_AS_NANOS, DATETIME_AS_SECONDS, NANOS_AS_DATETIME, SECONDS_AS_DATETIME,
    TIMESTAMP_VARIABLE_NAME,
};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Variable};
use spargebra::algebra::{AggregateExpression, Expression, Function, GraphPattern};

impl Translator {
    pub fn add_aggregation(
        &mut self,
        mut inner_gp: GraphPattern,
        ts_query: &TsQuery,
        project_paths: &mut Vec<Variable>,
        project_values: &mut Vec<Variable>,
    ) -> GraphPattern {
        if let Some(aggregation) = &ts_query.aggregation {
            let timestamp_variable_expression =
                Expression::Variable(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
            //TODO: Is this safe?
            let duration_nanoseconds = aggregation.duration.as_nanos();
            let use_to_func;
            let use_from_func;
            let use_mag;
            if duration_nanoseconds % 1_000_000_000 == 0 {
                use_to_func = DATETIME_AS_SECONDS;
                use_from_func = SECONDS_AS_DATETIME;
                use_mag = aggregation.duration.as_secs();
            } else {
                use_to_func = DATETIME_AS_NANOS;
                use_from_func = NANOS_AS_DATETIME;
                use_mag = duration_nanoseconds as u64;
            }

            let grouping_col_expression = Expression::Multiply(
                Box::new(Expression::FunctionCall(
                    Function::Floor,
                    vec![Expression::Divide(
                        Box::new(Expression::FunctionCall(
                            Function::Custom(NamedNode::new_unchecked(use_to_func)),
                            vec![timestamp_variable_expression.clone()],
                        )),
                        Box::new(Expression::Literal(Literal::new_typed_literal(
                            use_mag.to_string(),
                            xsd::UNSIGNED_LONG,
                        ))),
                    )],
                )),
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    use_mag.to_string(),
                    xsd::UNSIGNED_LONG,
                ))),
            );
            let timestamp_grouping_variable =
                Variable::new_unchecked(format!("{}_grouping", TIMESTAMP_VARIABLE_NAME));
            inner_gp = GraphPattern::Extend {
                inner: Box::new(inner_gp),
                variable: timestamp_grouping_variable.clone(),
                expression: grouping_col_expression,
            };
            let mut grouping_cols = project_paths.clone();
            grouping_cols.push(timestamp_grouping_variable.clone());

            let mut aggregates = vec![];
            for val_col in project_values.iter() {
                let agg_expr = match aggregation.function_name.as_str() {
                    "mean" | "avg" => AggregateExpression::Avg {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "max" | "maximum" => AggregateExpression::Max {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "min" | "minimum" => AggregateExpression::Min {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "sum" => AggregateExpression::Sum {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "sample" => AggregateExpression::Sample {
                        expr: Box::new(Expression::Variable(val_col.clone())),
                        distinct: false,
                    },
                    "count" => AggregateExpression::Count {
                        expr: Some(Box::new(Expression::Variable(val_col.clone()))),
                        distinct: false,
                    },
                    _ => {
                        panic!("Not found!!!")
                    }
                };
                aggregates.push((val_col.clone(), agg_expr));
            }

            inner_gp = GraphPattern::Group {
                inner: Box::new(inner_gp),
                variables: grouping_cols,
                aggregates,
            };
            inner_gp = GraphPattern::Extend {
                inner: Box::new(inner_gp),
                variable: Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME),
                expression: Expression::FunctionCall(
                    Function::Custom(NamedNode::new_unchecked(use_from_func)),
                    vec![Expression::Variable(timestamp_grouping_variable)],
                ),
            }
        }
        inner_gp
    }
}
