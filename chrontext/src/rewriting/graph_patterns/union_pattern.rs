use super::StaticQueryRewriter;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;
use crate::rewriting::subqueries::{SubQuery, SubQueryInContext};

impl StaticQueryRewriter {
    pub fn rewrite_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,

        context: &Context,
    ) -> GPReturn {
        let left_context = context.extension_with(PathEntry::UnionLeftSide);
        let mut left_rewrite =
            self.rewrite_graph_pattern(left, &left_context);
        let right_context = context.extension_with(PathEntry::UnionRightSide);
        let mut right_rewrite =
            self.rewrite_graph_pattern(right, &right_context);

        if left_rewrite.is_subquery
            || right_rewrite.is_subquery
        {
            let left_subquery_context;
            if !left_rewrite.is_subquery {
                self.create_add_subquery(left_rewrite, &left_context);
                left_subquery_context = left_context.clone();
            } else {
                left_subquery_context = left_rewrite.subquery_context.unwrap().clone();
            }
            let right_subquery_context;
            if !right_rewrite.is_subquery {
                self.create_add_subquery(right_rewrite, &right_context);
                right_subquery_context = right_context.clone();
            } else {
                right_subquery_context = right_rewrite.subquery_context.unwrap().clone();
            }
            self.subqueries_in_context.push(SubQueryInContext::new(context.clone(), SubQuery::Union(left_subquery_context, right_subquery_context)));
            return GPReturn::subquery(context.clone())
        }

        let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
        let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();

        left_rewrite
            .with_scope(&mut right_rewrite)
            .with_graph_pattern(GraphPattern::Union {
                left: Box::new(left_graph_pattern),
                right: Box::new(right_graph_pattern),
            })
            .with_rewritten(left_rewrite.rewritten || right_rewrite.rewritten);
        return left_rewrite;
    }
}
