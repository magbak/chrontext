use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use crate::query_context::{Context};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::StaticQueryRewriter;

impl StaticQueryRewriter {
    pub(crate) fn create_add_subquery(&mut self, gpreturn: GPReturn, context: &Context) {
        if gpreturn.graph_pattern.is_some() {
            let mut variables:Vec<Variable> = gpreturn.variables_in_scope.iter().map(|x|x.clone()).collect();
            variables.sort_by_key(|x|x.as_str().to_string());
            let projection = self.create_projection_graph_pattern(&gpreturn, context, &variables);
            self.add_subquery(context, projection)
        }
    }

    fn add_subquery(&mut self, context: &Context, gp: GraphPattern) {
        self.static_subqueries.insert(context.clone(), Query::Select {
            dataset: None,
            pattern: gp,
            base_iri: None
        });
    }
}