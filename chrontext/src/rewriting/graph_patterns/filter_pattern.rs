use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_filter(
        &mut self,
        expression: &Expression,
        inner: &GraphPattern,
        context: &Context,
    ) -> GPReturn {
        let inner_context = context.extension_with(PathEntry::FilterExpression);
        let mut inner_rewrite =
            self.rewrite_graph_pattern(inner, &context.extension_with(PathEntry::FilterInner));
        let mut expression_rewrite = self.rewrite_expression(
            expression,
            &ChangeType::Relaxed,
            &inner_rewrite.variables_in_scope,
            &inner_context,
        );
        if !expression_rewrite.pushups.is_empty() || inner_rewrite.is_subquery {
            let mut expression_subqueries_vec = vec![];

            for (gp, ctx) in expression_rewrite
                .pushups
                .iter()
                .zip(expression_rewrite.pushup_contexts.iter())
            {
                self.create_add_subquery(gp.clone(), ctx);
                expression_subqueries_vec.push(ctx.clone())
            }
            let ret = GPReturn::subquery(context.clone());
            return ret;
        }

        if expression_rewrite.expression.is_some() {
            let rewritten = inner_rewrite.rewritten
                || expression_rewrite.change_type != Some(ChangeType::NoChange);
            self.rewritten_filters.insert(
                context.clone(),
                expression_rewrite.expression.as_ref().unwrap().clone(),
            );
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite
                .with_graph_pattern(GraphPattern::Filter {
                    expr: expression_rewrite.expression.take().unwrap(),
                    inner: Box::new(inner_graph_pattern),
                })
                .with_rewritten(rewritten);
            return inner_rewrite;
        } else {
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite
                .with_graph_pattern(inner_graph_pattern)
                .with_rewritten(true);
            return inner_rewrite;
        }
    }
}
