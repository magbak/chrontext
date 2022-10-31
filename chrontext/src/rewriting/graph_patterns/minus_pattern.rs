use super::StaticQueryRewriter;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_minus(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,

        context: &Context,
    ) -> GPReturn {
        let left_context = context.extension_with(PathEntry::MinusLeftSide);
        let mut left_rewrite = self.rewrite_graph_pattern(
            left,
            &left_context,
        );
        let right_context = context.extension_with(PathEntry::MinusRightSide);
        let mut right_rewrite = self.rewrite_graph_pattern(
            right,
            &right_context,
        );
        if (left_rewrite.contains_exploded_pattern && right_rewrite.rewritten) || (right_rewrite.contains_exploded_pattern && left_rewrite.rewritten) {
            if !left_rewrite.is_subquery {
                    self.create_add_subquery(
                        left_rewrite,
                        &left_context,
                        PathEntry::MinusLeftSide,
                    );
                }
            if !right_rewrite.is_subquery {
                self.create_add_subquery(
                    right_rewrite,
                    &right_context,
                    PathEntry::MinusRightSide,
                );
            }
            self.subquery_ntuples.push(vec![
                    (PathEntry::LeftJoinLeftSide, left_context),
                    (PathEntry::LeftJoinRightSide, right_context),
                ]);
            let mut ret = GPReturn::subquery(context.clone());
            return ret;
        }

        if left_rewrite.graph_pattern.is_some() {
            if right_rewrite.graph_pattern.is_some() {
                if !left_rewrite.rewritten && !right_rewrite.rewritten
                {
                    let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                    let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                    left_rewrite.with_graph_pattern(GraphPattern::Minus {
                        left: Box::new(left_graph_pattern),
                        right: Box::new(right_graph_pattern),
                    });
                    return left_rewrite;
                } else if left_rewrite.rewritten && !right_rewrite.rewritten
                {
                    let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                    let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                    left_rewrite
                        .with_graph_pattern(GraphPattern::Minus {
                            left: Box::new(left_graph_pattern),
                            right: Box::new(right_graph_pattern),
                        })
                        .with_rewritten(true);
                    return left_rewrite;
                } else if left_rewrite.rewritten && right_rewrite.rewritten
                {
                    todo!("Handle..")
                }
            }
        }
        GPReturn::none()
    }
}
