use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::expressions::ExReturn;
use oxrdf::Variable;
use spargebra::algebra::{GraphPattern, OrderExpression};
use std::collections::HashSet;

pub struct OEReturn {
    pub order_expression: Option<OrderExpression>,
    pub graph_pattern_pushups: Vec<GraphPattern>,
}

impl OEReturn {
    fn new() -> OEReturn {
        OEReturn {
            order_expression: None,
            graph_pattern_pushups: vec![],
        }
    }

    fn with_order_expression(&mut self, order_expression: OrderExpression) -> &mut OEReturn {
        self.order_expression = Some(order_expression);
        self
    }

    fn with_pushups(&mut self, exr: &mut ExReturn) -> &mut OEReturn {
        self.graph_pattern_pushups.extend(
            exr.graph_pattern_pushups
                .drain(0..exr.graph_pattern_pushups.len()),
        );
        self
    }
}

impl StaticQueryRewriter {
    pub fn rewrite_order_expression(
        &mut self,
        order_expression: &OrderExpression,
        variables_in_scope: &HashSet<Variable>,
        context: &Context,
    ) -> OEReturn {
        let mut oer = OEReturn::new();
        match order_expression {
            OrderExpression::Asc(e) => {
                let mut e_rewrite = self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::OrderingOperation),
                );
                oer.with_pushups(&mut e_rewrite);
                if e_rewrite.expression.is_some() {
                    oer.with_order_expression(OrderExpression::Asc(
                        e_rewrite.expression.take().unwrap(),
                    ));
                }
            }
            OrderExpression::Desc(e) => {
                let mut e_rewrite = self.rewrite_expression(
                    e,
                    &ChangeType::NoChange,
                    variables_in_scope,
                    &context.extension_with(PathEntry::OrderingOperation),
                );
                oer.with_pushups(&mut e_rewrite);
                if e_rewrite.expression.is_some() {
                    oer.with_order_expression(OrderExpression::Desc(
                        e_rewrite.expression.take().unwrap(),
                    ));
                }
            }
        }
        oer
    }
}
