use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_union(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,

        context: &Context,
    ) -> GPReturn {
        let mut left_rewrite =
            self.rewrite_graph_pattern(left, &context.extension_with(PathEntry::UnionLeftSide));
        let mut right_rewrite =
            self.rewrite_graph_pattern(right, &context.extension_with(PathEntry::UnionRightSide));

        if left_rewrite.graph_pattern.is_some() {
            if right_rewrite.graph_pattern.is_some() {
                let use_change;
                if &left_rewrite.change_type == &ChangeType::NoChange
                    && &right_rewrite.change_type == &ChangeType::NoChange
                {
                    use_change = ChangeType::NoChange;
                } else if &left_rewrite.change_type == &ChangeType::NoChange
                    || &right_rewrite.change_type == &ChangeType::NoChange
                    || &left_rewrite.change_type == &ChangeType::Relaxed
                    || &right_rewrite.change_type == &ChangeType::Relaxed
                {
                    use_change = ChangeType::Relaxed;
                } else {
                    return GPReturn::none();
                }
                let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                left_rewrite
                    .with_scope(&mut right_rewrite)
                    .with_graph_pattern(GraphPattern::Union {
                        left: Box::new(left_graph_pattern),
                        right: Box::new(right_graph_pattern),
                    })
                    .with_change_type(use_change);
                return left_rewrite;
            } else {
                //left is some, right is none
                if &left_rewrite.change_type == &ChangeType::Relaxed
                    || &left_rewrite.change_type == &ChangeType::NoChange
                {
                    return left_rewrite;
                }
            }
        } else if right_rewrite.graph_pattern.is_some() {
            //left is none, right is some
            if &right_rewrite.change_type == &ChangeType::Relaxed
                || &right_rewrite.change_type == &ChangeType::NoChange
            {
                return right_rewrite;
            }
        }
        GPReturn::none()
    }
}
