use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_reduced(
        &mut self,
        inner: &GraphPattern,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::ReducedInner),
        );
        if inner_rewrite.graph_pattern.is_some() {
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite.with_graph_pattern(GraphPattern::Reduced {
                inner: Box::new(inner_graph_pattern),
            });
            return inner_rewrite;
        }
        GPReturn::none()
    }
}
