mod aggregate_expression;
mod expressions;
mod graph_patterns;
mod order_expression;
mod project_static;
mod subqueries;

use crate::constraints::{Constraint, VariableConstraints};
use crate::query_context::Context;
use crate::rewriting::expressions::ExReturn;
use crate::timeseries_query::BasicTimeSeriesQuery;
use spargebra::algebra::{Expression};
use spargebra::term::Variable;
use spargebra::Query;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct StaticQueryRewriter {
    variable_counter: u16,
    additional_projections: HashSet<Variable>,
    variable_constraints: VariableConstraints,
    basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
    static_subqueries: HashMap<Context, Query>,
    rewritten_filters: HashMap<Context, Expression>,
}

impl StaticQueryRewriter {
    pub fn new(variable_constraints: &VariableConstraints) -> StaticQueryRewriter {
        StaticQueryRewriter {
            variable_counter: 0,
            additional_projections: Default::default(),
            variable_constraints: variable_constraints.clone(),
            basic_time_series_queries: vec![],
            static_subqueries: HashMap::new(),
            rewritten_filters: HashMap::new(),
        }
    }

    pub fn rewrite_query(
        mut self,
        query: Query,
    ) -> (HashMap<Context, Query>, Vec<BasicTimeSeriesQuery>, HashMap<Context, Expression>) {
        if let Query::Select {
            dataset,
            pattern,
            base_iri,
        } = query
        {
            let pattern_rewrite = self.rewrite_graph_pattern(&pattern, &Context::new());
            if let Some(p) = pattern_rewrite.graph_pattern {
                self.static_subqueries.insert(Context::new(), Query::Select {
                    dataset,
                    pattern: p,
                    base_iri
                });
            }
            (self.static_subqueries, self.basic_time_series_queries, self.rewritten_filters)
        } else {
            panic!("Only support for select query")
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
