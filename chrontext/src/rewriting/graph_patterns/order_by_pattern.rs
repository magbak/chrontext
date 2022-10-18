use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::order_expression::OEReturn;
use crate::rewriting::pushups::apply_pushups;
use spargebra::algebra::{GraphPattern, OrderExpression};

impl StaticQueryRewriter {
    pub fn rewrite_order_by(
        &mut self,
        inner: &GraphPattern,
        order_expressions: &Vec<OrderExpression>,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::OrderByInner),
        );
        if inner_rewrite.graph_pattern.is_some() {
            let mut order_expressions_rewrite = order_expressions
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    self.rewrite_order_expression(
                        e,
                        &inner_rewrite.variables_in_scope,
                        &context.extension_with(PathEntry::OrderByExpression(i as u16)),
                    )
                })
                .collect::<Vec<OEReturn>>();
            let mut inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            for oer in order_expressions_rewrite.iter_mut() {
                inner_graph_pattern =
                    apply_pushups(inner_graph_pattern, &mut oer.graph_pattern_pushups);
            }
            if order_expressions_rewrite
                .iter()
                .any(|oer| oer.order_expression.is_some())
            {
                inner_rewrite.with_graph_pattern(GraphPattern::OrderBy {
                    inner: Box::new(inner_graph_pattern),
                    expression: order_expressions_rewrite
                        .iter_mut()
                        .filter(|oer| oer.order_expression.is_some())
                        .map(|oer| oer.order_expression.take().unwrap())
                        .collect(),
                });
            } else {
                inner_rewrite.with_graph_pattern(inner_graph_pattern);
            }
            return inner_rewrite;
        }
        GPReturn::none()
    }
}
