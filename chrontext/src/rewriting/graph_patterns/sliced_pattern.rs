use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_slice(
        &mut self,
        inner: &GraphPattern,
        start: &usize,
        length: &Option<usize>,
        required_change_direction: &ChangeType,
        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            required_change_direction,
            &context.extension_with(PathEntry::SliceInner),
        );
        if inner_rewrite.graph_pattern.is_some() {
            let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
            inner_rewrite.with_graph_pattern(GraphPattern::Slice {
                inner: Box::new(inner_graph_pattern),
                start: start.clone(),
                length: length.clone(),
            });
            return inner_rewrite;
        }
        GPReturn::none()
    }
}
