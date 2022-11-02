mod aggregate_expression;
mod expressions;
mod graph_patterns;
mod order_expression;
mod project_static;
pub(crate) mod subqueries;

use crate::constraints::{Constraint, VariableConstraints};
use crate::query_context::{Context};
use crate::rewriting::expressions::ExReturn;
use crate::timeseries_query::BasicTimeSeriesQuery;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::Variable;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use crate::rewriting::subqueries::SubQueryInContext;

#[derive(Debug)]
pub struct StaticQueryRewriter {
    variable_counter: u16,
    additional_projections: HashSet<Variable>,
    variable_constraints: VariableConstraints,
    basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
    static_subqueries: HashMap<Context, GraphPattern>,
    subqueries_in_context: Vec<SubQueryInContext>,
    pub rewritten_filters: HashMap<Context, Expression>,
}

impl StaticQueryRewriter {
    pub fn new(variable_constraints: &VariableConstraints) -> StaticQueryRewriter {
        StaticQueryRewriter {
            variable_counter: 0,
            additional_projections: Default::default(),
            variable_constraints: variable_constraints.clone(),
            basic_time_series_queries: vec![],
            static_subqueries: HashMap::new(),
            subqueries_in_context: vec![],
            rewritten_filters: HashMap::new(),
        }
    }

    pub fn rewrite_query(&mut self, query: Query) -> Option<(Query, Vec<BasicTimeSeriesQuery>)> {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = &query
        {
            let mut pattern_rewrite = self.rewrite_graph_pattern(pattern, &Context::new());
            if pattern_rewrite.graph_pattern.is_some() {
                return Some((
                    Query::Select {
                        dataset: dataset.clone(),
                        pattern: pattern_rewrite.graph_pattern.take().unwrap(),
                        base_iri: base_iri.clone(),
                    },
                    self.basic_time_series_queries
                        .drain(0..self.basic_time_series_queries.len())
                        .collect(),
                ));
            } else {
                None
            }
        } else {
            panic!("Only support for Select");
        }
    }

    fn project_all_static_variables(&mut self, rewrites: Vec<&ExReturn>, context: &Context) {
        for r in rewrites {
            if let Some(expr) = &r.expression {
                self.project_all_static_variables_in_expression(expr, context);
            }
        }
    }

    fn rewrite_variable(&self, v: &Variable, context: &Context) -> Option<Variable> {
        if let Some(ctr) = self.variable_constraints.get_constraint(v, context) {
            if !(ctr == &Constraint::ExternalDataPoint
                || ctr == &Constraint::ExternalDataValue
                || ctr == &Constraint::ExternalTimestamp
                || ctr == &Constraint::ExternallyDerived)
            {
                Some(v.clone())
            } else {
                None
            }
        } else {
            Some(v.clone())
        }
    }
}
