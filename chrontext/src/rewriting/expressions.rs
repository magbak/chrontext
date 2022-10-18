mod and_expression;
mod binary_ordinary_expression;
mod coalesce_expression;
mod exists_expression;
mod function_call_expression;
mod if_expression;
mod in_expression;
mod not_expression;
mod or_expression;
mod unary_ordinary_expression;

use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::Context;
use crate::rewriting::expressions::binary_ordinary_expression::BinaryOrdinaryOperator;
use crate::rewriting::expressions::unary_ordinary_expression::UnaryOrdinaryOperator;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

pub struct ExReturn {
    pub expression: Option<Expression>,
    pub change_type: Option<ChangeType>,
    pub graph_pattern_pushups: Vec<GraphPattern>,
}

impl ExReturn {
    fn new() -> ExReturn {
        ExReturn {
            expression: None,
            change_type: None,
            graph_pattern_pushups: vec![],
        }
    }

    fn with_expression(&mut self, expression: Expression) -> &mut ExReturn {
        self.expression = Some(expression);
        self
    }

    fn with_change_type(&mut self, change_type: ChangeType) -> &mut ExReturn {
        self.change_type = Some(change_type);
        self
    }

    fn with_graph_pattern_pushup(&mut self, graph_pattern: GraphPattern) -> &mut ExReturn {
        self.graph_pattern_pushups.push(graph_pattern);
        self
    }

    fn with_pushups(&mut self, exr: &mut ExReturn) -> &mut ExReturn {
        self.graph_pattern_pushups.extend(
            exr.graph_pattern_pushups
                .drain(0..exr.graph_pattern_pushups.len()),
        );
        self
    }
}

impl StaticQueryRewriter {
    pub fn rewrite_expression(
        &mut self,
        expression: &Expression,
        required_change_direction: &ChangeType,
        variables_in_scope: &HashSet<Variable>,
        context: &Context,
    ) -> ExReturn {
        match expression {
            Expression::NamedNode(nn) => {
                let mut exr = ExReturn::new();
                exr.with_expression(Expression::NamedNode(nn.clone()))
                    .with_change_type(ChangeType::NoChange);
                exr
            }
            Expression::Literal(l) => {
                let mut exr = ExReturn::new();
                exr.with_expression(Expression::Literal(l.clone()))
                    .with_change_type(ChangeType::NoChange);
                exr
            }
            Expression::Variable(v) => {
                if let Some(rewritten_variable) = self.rewrite_variable(v, context) {
                    if variables_in_scope.contains(v) {
                        let mut exr = ExReturn::new();
                        exr.with_expression(Expression::Variable(rewritten_variable))
                            .with_change_type(ChangeType::NoChange);
                        return exr;
                    }
                }
                ExReturn::new()
            }
            Expression::Or(left, right) => self.rewrite_or_expression(
                left,
                right,
                required_change_direction,
                variables_in_scope,
                context,
            ),

            Expression::And(left, right) => self.rewrite_and_expression(
                left,
                right,
                required_change_direction,
                variables_in_scope,
                context,
            ),
            Expression::Equal(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Equal,
                variables_in_scope,
                context,
            ),
            Expression::SameTerm(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::SameTerm,
                variables_in_scope,
                context,
            ),
            Expression::Greater(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Greater,
                variables_in_scope,
                context,
            ),
            Expression::GreaterOrEqual(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::GreaterOrEqual,
                variables_in_scope,
                context,
            ),
            Expression::Less(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Less,
                variables_in_scope,
                context,
            ),
            Expression::LessOrEqual(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::LessOrEqual,
                variables_in_scope,
                context,
            ),
            Expression::In(left, expressions) => self.rewrite_in_expression(
                left,
                expressions,
                required_change_direction,
                variables_in_scope,
                context,
            ),
            Expression::Add(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Add,
                variables_in_scope,
                context,
            ),
            Expression::Subtract(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Subtract,
                variables_in_scope,
                context,
            ),
            Expression::Multiply(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Multiply,
                variables_in_scope,
                context,
            ),
            Expression::Divide(left, right) => self.rewrite_binary_ordinary_expression(
                left,
                right,
                &BinaryOrdinaryOperator::Divide,
                variables_in_scope,
                context,
            ),
            Expression::UnaryPlus(wrapped) => self.rewrite_unary_ordinary_expression(
                wrapped,
                &UnaryOrdinaryOperator::UnaryPlus,
                variables_in_scope,
                context,
            ),
            Expression::UnaryMinus(wrapped) => self.rewrite_unary_ordinary_expression(
                wrapped,
                &UnaryOrdinaryOperator::UnaryMinus,
                variables_in_scope,
                context,
            ),
            Expression::Not(wrapped) => self.rewrite_not_expression(
                wrapped,
                required_change_direction,
                variables_in_scope,
                context,
            ),
            Expression::Exists(wrapped) => self.rewrite_exists_expression(wrapped, context),
            Expression::Bound(v) => {
                let mut exr = ExReturn::new();
                if let Some(v_rewritten) = self.rewrite_variable(v, context) {
                    exr.with_expression(Expression::Bound(v_rewritten))
                        .with_change_type(ChangeType::NoChange);
                }
                exr
            }
            Expression::If(left, mid, right) => {
                self.rewrite_if_expression(left, mid, right, variables_in_scope, context)
            }
            Expression::Coalesce(wrapped) => {
                self.rewrite_coalesce_expression(wrapped, variables_in_scope, context)
            }
            Expression::FunctionCall(fun, args) => {
                self.rewrite_function_call_expression(fun, args, variables_in_scope, context)
            }
        }
    }
}
