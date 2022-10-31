use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::expressions::ExReturn;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_exists_expression(
        &mut self,
        wrapped: &GraphPattern,
        context: &Context,
    ) -> ExReturn {
        let mut wrapped_rewrite = self.rewrite_graph_pattern(
            wrapped,
            &context.extension_with(PathEntry::Exists),
        );
        let mut exr = ExReturn::new();
        if wrapped_rewrite.graph_pattern.is_some() {
            if !wrapped_rewrite.rewritten {
                exr.with_expression(Expression::Exists(Box::new(
                    wrapped_rewrite.graph_pattern.take().unwrap(),
                )))
                .with_change_type(ChangeType::NoChange);
                return exr;
            } else {
                exr.with_pushup_and_context(wrapped_rewrite, context.clone());
            }
        }
        exr
    }
}
