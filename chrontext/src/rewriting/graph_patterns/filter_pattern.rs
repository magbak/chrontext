use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,

        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            &context.extension_with(PathEntry::FilterInner),
        );

        if inner_rewrite.graph_pattern.is_some() {
            let mut expression_rewrite = self.rewrite_expression(
                expression,
                &ChangeType::Relaxed,
                &inner_rewrite.variables_in_scope,
                &context.extension_with(PathEntry::FilterExpression),
            );
            if expression_rewrite.expression.is_some() {
                let use_change;
                if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange {
                    use_change = inner_rewrite.change_type.clone();
                } else if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed {
                    if &inner_rewrite.change_type == &ChangeType::Relaxed
                        || &inner_rewrite.change_type == &ChangeType::NoChange
                    {
                        use_change = ChangeType::Relaxed;
                    } else {
                        return GPReturn::none();
                    }
                } else if expression_rewrite.change_type.as_ref().unwrap()
                    == &ChangeType::Constrained
                {
                    if &inner_rewrite.change_type == &ChangeType::Constrained {
                        use_change = ChangeType::Constrained;
                    } else {
                        return GPReturn::none();
                    }
                } else {
                    panic!("Should never happen");
                }
                self.rewritten_filters.insert(
                    context.clone(),
                    expression_rewrite.expression.as_ref().unwrap().clone(),
                );
                let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_rewrite
                    .with_graph_pattern(GraphPattern::Filter {
                        expr: expression_rewrite.expression.take().unwrap(),
                        inner: Box::new(apply_pushups(
                            inner_graph_pattern,
                            &mut expression_rewrite.graph_pattern_pushups,
                        )),
                    })
                    .with_change_type(use_change);
                return inner_rewrite;
            } else {
                let mut inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_graph_pattern = apply_pushups(
                    inner_graph_pattern,
                    &mut expression_rewrite.graph_pattern_pushups,
                );
                inner_rewrite.with_graph_pattern(inner_graph_pattern);
                return inner_rewrite;
            }
        }
        GPReturn::none()
    }
}
