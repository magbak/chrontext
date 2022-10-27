use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;

use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,

        context: &Context,
    ) -> GPReturn {
        let mut left_rewrite = self.rewrite_graph_pattern(
            left,
            &context.extension_with(PathEntry::JoinLeftSide),
        );
        let mut right_rewrite = self.rewrite_graph_pattern(
            right,
            &context.extension_with(PathEntry::JoinRightSide),
        );

        if left_rewrite.graph_pattern.is_some() {
            if right_rewrite.graph_pattern.is_some() {
                let use_change;
                if &left_rewrite.change_type == &ChangeType::NoChange
                    && &right_rewrite.change_type == &ChangeType::NoChange
                {
                    use_change = ChangeType::NoChange;
                } else if (&left_rewrite.change_type == &ChangeType::NoChange
                    || &left_rewrite.change_type == &ChangeType::Relaxed)
                    && (&right_rewrite.change_type == &ChangeType::NoChange
                        || &right_rewrite.change_type == &ChangeType::Relaxed)
                {
                    use_change = ChangeType::Relaxed;
                } else if (&left_rewrite.change_type == &ChangeType::NoChange
                    || &left_rewrite.change_type == &ChangeType::Constrained)
                    && (&right_rewrite.change_type == &ChangeType::NoChange
                        || &right_rewrite.change_type == &ChangeType::Constrained)
                {
                    use_change = ChangeType::Constrained;
                } else {
                    return GPReturn::none();
                }
                let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();

                left_rewrite
                    .with_scope(&mut right_rewrite)
                    .with_graph_pattern(GraphPattern::Join {
                        left: Box::new(left_graph_pattern),
                        right: Box::new(right_graph_pattern),
                    })
                    .with_change_type(use_change);
                return left_rewrite;
            } else {
                //left some, right none
                if &left_rewrite.change_type == &ChangeType::NoChange
                    || &left_rewrite.change_type == &ChangeType::Relaxed
                {
                    left_rewrite.with_change_type(ChangeType::Relaxed);
                    return left_rewrite;
                }
            }
        } else if right_rewrite.graph_pattern.is_some() {
            //left is none
            if &right_rewrite.change_type == &ChangeType::NoChange
                || &right_rewrite.change_type == &ChangeType::Relaxed
            {
                right_rewrite.with_change_type(ChangeType::Relaxed);
                return right_rewrite;
            }
        }
        GPReturn::none()
    }
}
