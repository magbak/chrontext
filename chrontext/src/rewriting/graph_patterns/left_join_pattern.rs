use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression_opt: &Option<Expression>,
        context: &Context,
    ) -> GPReturn {
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let mut left_rewrite = self.rewrite_graph_pattern(left, &left_context);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let mut right_rewrite = self.rewrite_graph_pattern(right, &right_context);

        if let Some(expression) = expression_opt {
            let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
            let mut expression_rewrite = self.rewrite_expression(
                expression,
                &ChangeType::Relaxed,
                &left_rewrite.variables_in_scope,
                &expression_context,
            );
            if left_rewrite.is_subquery
                || right_rewrite.is_subquery
                || !expression_rewrite.pushups.is_empty()
            {
                if !left_rewrite.is_subquery {
                    self.create_add_subquery(
                        left_rewrite,
                        &left_context,
                        PathEntry::LeftJoinLeftSide,
                    );
                }
                if !right_rewrite.is_subquery {
                    self.create_add_subquery(
                        right_rewrite,
                        &right_context,
                        PathEntry::LeftJoinRightSide,
                    );
                }

                let mut subquery_vec = vec![
                    (PathEntry::LeftJoinLeftSide, left_context),
                    (PathEntry::LeftJoinRightSide, right_context),
                ];
                for (gp, ctx) in expression_rewrite
                    .pushups
                    .iter()
                    .zip(expression_rewrite.pushup_contexts.iter())
                {
                    //TODO: Fix the context so these things are recoverable..
                    self.create_add_subquery(
                        gp.clone(),
                        &expression_context,
                        PathEntry::LeftJoinExpression,
                    );
                    subquery_vec.push((PathEntry::LeftJoinExpression, expression_context.clone()));
                }
                self.subquery_ntuples.push(subquery_vec);
                let mut ret = GPReturn::subquery(context.clone());
                return ret;
            } else {
                left_rewrite.with_scope(&mut right_rewrite);

                let is_rewritten = expression_rewrite.change_type == Some(ChangeType::Relaxed)
                    || left_rewrite.rewritten
                    || right_rewrite.rewritten;
                let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                left_rewrite
                    .with_graph_pattern(GraphPattern::LeftJoin {
                        left: Box::new(left_graph_pattern), // TODO: apply pushups
                        right: Box::new(right_graph_pattern),
                        expression: expression_rewrite.expression.take(),
                    })
                    .with_rewritten(is_rewritten);
                return left_rewrite;
            }
        } else {
            if left_rewrite.is_subquery
                || right_rewrite.is_subquery
            {
                if !left_rewrite.is_subquery {
                    self.create_add_subquery(
                        left_rewrite,
                        &left_context,
                        PathEntry::LeftJoinLeftSide,
                    );
                }
                if !right_rewrite.is_subquery {
                    self.create_add_subquery(
                        right_rewrite,
                        &right_context,
                        PathEntry::LeftJoinRightSide,
                    );
                }
                self.subquery_ntuples.push(vec![
                    (PathEntry::LeftJoinLeftSide, left_context),
                    (PathEntry::LeftJoinRightSide, right_context),
                ]);

                let mut ret = GPReturn::subquery(context.clone());
                return ret;
            } else {
                let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                left_rewrite
                    .with_graph_pattern(GraphPattern::LeftJoin {
                        left: Box::new(left_graph_pattern),
                        right: Box::new(right_graph_pattern),
                        expression: None,
                    })
                    .with_rewritten(left_rewrite.rewritten || right_rewrite.rewritten);
                return left_rewrite;
            }
        }
    }
}
