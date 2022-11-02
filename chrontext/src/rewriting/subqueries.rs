use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use crate::query_context::{Context};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::StaticQueryRewriter;

#[derive(Debug)]
pub struct SubQueryInContext {
    pub context: Context,
    pub subquery: SubQuery
}

impl SubQueryInContext {
    pub fn new(context:Context, subquery:SubQuery) -> SubQueryInContext{
        SubQueryInContext {
            context,
            subquery
        }
    }
}

#[derive(Debug)]
pub enum SubQuery {
    Filter(Context, Vec<Context>),
    Group(Context),
    Join(Context, Context),
    LeftJoin(Context, Context, Vec<Context>),
    Minus(Context, Context),
    Union(Context, Context),
}

impl StaticQueryRewriter {
    pub(crate) fn create_add_subquery(&mut self, gpreturn: GPReturn, context: &Context) {
        if gpreturn.graph_pattern.is_some() {
            let mut variables:Vec<Variable> = gpreturn.variables_in_scope.iter().map(|x|x.clone()).collect();
            variables.sort_by_key(|x|x.as_str());
            let projection = self.create_projection_graph_pattern(&gpreturn, context, &variables);
            self.add_subquery(context, projection)
        }
    }

    fn add_subquery(&mut self, context: &Context, gp: GraphPattern) {
        self.static_subqueries.insert(context.clone(), gp);
    }
}