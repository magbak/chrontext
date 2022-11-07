use super::Combiner;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::time_series_queries::complete_basic_time_series_queries;
use crate::combiner::CombinerError;
use crate::query_context::Context;
use crate::sparql_result_to_polars::create_static_query_dataframe;
use crate::static_sparql::execute_sparql_query;
use polars::prelude::{col, Expr, IntoLazy};
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use polars_core::prelude::JoinType;

impl Combiner {
    pub async fn execute_static_query(
        &mut self,
        query: &Query,
        solution_mappings: Option<SolutionMappings>,
    ) -> Result<SolutionMappings, CombinerError> {
        let solutions = execute_sparql_query(&self.endpoint, query)
            .await
            .map_err(|x| CombinerError::StaticQueryExecutionError(x))?;
        complete_basic_time_series_queries(
            &solutions,
            &mut self.prepper.basic_time_series_queries,
        )?;
        let (df, mut datatypes) = create_static_query_dataframe(query, solutions);
        let mut columns:HashSet<String> = df
            .get_column_names()
            .iter()
            .map(|x| x.to_string())
            .collect();
        if columns.is_empty() {
            return Ok(solution_mappings.unwrap())
        }
        let mut lf = df.lazy();
        if let Some(SolutionMappings { mappings: input_lf, columns: input_columns, datatypes: input_datatypes }) = solution_mappings {
            let on:Vec<&String> = columns.intersection(&input_columns).collect();
            let on_cols:Vec<Expr> = on.iter().map(|x|col(x)).collect();
            let join_type = if on_cols.is_empty() {
                JoinType::Cross
            } else {
                JoinType::Inner
            };
            lf = lf.join(input_lf, on_cols.as_slice(), on_cols.as_slice(), join_type);

            columns.extend(input_columns);
            datatypes.extend(input_datatypes);
        }
        Ok(SolutionMappings::new(lf, columns, datatypes))
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
        let q = static_queries.remove(&k).unwrap();
        new_map.insert(k, q);
    }
    new_map
}

pub(crate) fn split_static_queries_opt(
    static_queries: &mut Option<HashMap<Context, Query>>,
    context: &Context,
) -> Option<HashMap<Context, Query>> {
    if let Some(static_queries) = static_queries {
        Some(split_static_queries(static_queries, context))
    } else {
        None
    }
}
