use crate::query_context::{Context, PathEntry};
use crate::static_sparql::execute_sparql_query;
use oxrdf::Variable;
use polars_core::prelude::Series;
use sparesults::QuerySolution;
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use spargebra::Query;
use std::collections::{HashMap, HashSet};

pub(crate) struct StaticExecutor {
    query_results: HashMap<Context, Vec<QuerySolution>>,
    pub(crate) query_map: HashMap<Context, Query>,
    endpoint: String,
}

impl StaticExecutor {
    pub(crate) async fn execute_static_queries(&mut self) {
        let empty_context = Context::new();
        let mut contexts: Vec<&Context> = self.query_map.keys().collect();
        contexts = contexts
            .into_iter()
            .filter(|x| x != &&empty_context)
            .collect();
        contexts.sort_by_key(|x| x.path.iter().map(|x| x.to_string()).join("."));
        for ctx in contexts {
            let query = self.query_map.get(ctx).unwrap();
            let constrained_query = self.constrain_query(ctx, query);
            let solutions = execute_sparql_query(&self.endpoint, &constrained_query).await?;
            self.query_results.insert(ctx.clone(), solutions);
        }
    }

    fn constrain_query(&self, context: &Context, query: &Query) -> Query {
        let projected_variable_set = get_variable_set(query);
        let mut solutions = Some(vec![]);
        for (other, other_solns) in &self.query_results {
            if let Some((this_after_meet, other_after_meet)) =
                find_context_after_meet(context, other)
            {

                let other_variables = get_variable_set(&self.query_map.get(other).unwrap());
                let mut common_variables: Vec<&Variable> = projected_variable_set
                    .intersection(&other_variables)
                    .collect();
                //Dette er feil.. vi må eksludere mer, det er soln. mapping..
                if !common_variables.is_empty() && other.in_scope(&context, false) {
                    //Todo is this correct?
                    if solutions.is_none() {
                        solutions = Some(other_solns.iter().map(|x| x.clone()).collect());
                    }
                }
            }
        }
        let mut variables_sorted = values_df.get_column_names();
        let mut bindings = vec![];
        for c in values_df.columns(variables_sorted.as_slice()).unwrap() {
            bindings.push(series_to_ground_term_vec(c))
        }
        let values_pattern = GraphPattern::Values {
            variables: variables_sorted
                .iter()
                .map(|x| Variable::new_unchecked(x))
                .collect(),
            bindings: vec![],
        };
        let mut constrained_query = query.clone();
    }
}

fn series_to_ground_term_vec(series: &Series) -> Vec<GroundTerm> {}

fn get_variable_set(query: &Query) -> HashSet<&Variable> {
    if let GraphPattern::Project { inner, variables } = query.pattern {
        return variables.iter().collect();
    } else {
        panic!("Non project graph pattern in query")
    }
}
