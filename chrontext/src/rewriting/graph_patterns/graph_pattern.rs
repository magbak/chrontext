use super::StaticQueryRewriter;
use crate::query_context::Context;
use crate::rewriting::graph_patterns::GPReturn;
use spargebra::algebra::GraphPattern;
use spargebra::term::NamedNodePattern;

impl StaticQueryRewriter {
    pub fn rewrite_graph(
        &mut self,
        name: &NamedNodePattern,
        inner: &GraphPattern,
        context: &Context,
    ) -> GPReturn {
        let mut inner_gpr = self.rewrite_graph_pattern(inner, context);
        if inner_gpr.graph_pattern.is_some() {
            let inner_rewrite = inner_gpr.graph_pattern.take().unwrap();
            inner_gpr.with_graph_pattern(GraphPattern::Graph {
                name: name.clone(),
                inner: Box::new(inner_rewrite),
            });
            return inner_gpr;
        }
        GPReturn::none()
    }
}
