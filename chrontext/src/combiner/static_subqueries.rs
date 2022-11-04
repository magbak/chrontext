use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::query_context::Context;
use crate::static_sparql::execute_sparql_query;
use spargebra::Query;
use std::collections::HashMap;
use crate::combiner::CombinerError;
use crate::sparql_result_to_polars::create_static_query_result_df;

impl Combiner {
    pub fn execute_static_query(
        &self,
        query: &Query,
        solution_mappings: Option<SolutionMappings>,
    ) -> Result<SolutionMappings, CombinerError> {
        let solutions = execute_sparql_query(&self.endpoint, query).await?;
        let mut datatypes = HashMap::new();
        create_static_query_result_df(query, solutions);
        if solut

        let columns = static_result_df
            .get_column_names()
            .iter()
            .map(|x| x.to_string())
            .collect();
        todo!()
    }
}

pub(crate) fn split_static_queries(
    static_queries: &mut HashMap<Context, Query>,
    context: &Context,
) -> HashMap<Context, Query> {
    let mut split_keys = vec![];
    for k in static_queries.keys() {
        if k.path.iter().zip(&context.path).all(|(x, y)| x == y) {
            split_keys.push(k.clone())
        }
    }
    let mut new_map = HashMap::new();
    for k in split_keys {
        new_map.insert(k, static_queries.remove(&k).unwrap());
    }
    new_map
}

pub(crate) fn split_static_queries_opt(
    static_queries: &mut Option<HashMap<Context, Query>>,
    context: &Context,
) -> Option<HashMap<Context, Query>> {
    if let Some(static_queries) = static_queries {
        let mut split_keys = vec![];
        for k in static_queries.keys() {
            if k.path.iter().zip(&context.path).all(|(x, y)| x == y) {
                split_keys.push(k.clone())
            }
        }
        let mut new_map = HashMap::new();
        for k in split_keys {
            new_map.insert(k, static_queries.remove(&k).unwrap());
        }
        Some(new_map)
    } else {
        None
    }
}
