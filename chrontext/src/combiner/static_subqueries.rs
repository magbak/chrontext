use super::Combiner;
use crate::query_context::Context;
use oxrdf::{NamedNode, Variable};
use polars_core::frame::DataFrame;
use spargebra::Query;
use std::collections::HashMap;
use crate::combiner::solution_mapping::SolutionMappings;

impl Combiner {
    pub fn execute_static_query(
        &self,
        query: &Query,
        constraints: &Option<SolutionMappings>,
    ) -> (DataFrame, HashMap<Variable, NamedNode>) {
        todo!()
    }
}

pub(crate) fn split_static_queries(
    static_queries: &mut HashMap<Context, Query>,
    context: &Context,
) -> HashMap<Context, Query> {
    let mut split_keys = vec![];
    for k in &static_queries.keys() {
        if k.path.iter().zip(context.path()).map(|(x, y)| x == y).all() {
            split_keys.push(k.clone())
        }
    }
    let mut new_map = HashMap::new();
    for k in split_keys {
        new_map.insert(k, static_queries.remove(&k).unwrap())
    }
    new_map
}
