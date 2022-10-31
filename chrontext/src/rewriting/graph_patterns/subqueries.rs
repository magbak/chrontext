use spargebra::algebra::GraphPattern;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use super::StaticQueryRewriter;

impl StaticQueryRewriter {
    pub(crate) fn create_add_subquery(&mut self, gpreturn: GPReturn, context: &Context, path_entry:PathEntry) {
        if gpreturn.graph_pattern.is_some() {
            let projection = self.create_projection_graph_pattern(&gpreturn, context, &vec![]);
            self.add_subquery(context, projection, path_entry)
        }
    }

    fn add_subquery(&mut self, context: &Context, gp: GraphPattern, path_entry:PathEntry) {
        self.static_subqueries.insert(context.clone(), gp);
        self.subquery_ntuples.push(vec![(path_entry, context.clone())]);
    }
}