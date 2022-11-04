use std::collections::HashMap;
use oxrdf::Variable;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use spargebra::algebra::GraphPattern;
use spargebra::term::TriplePattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns:: ConstrainingSolutionMapping;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result< ConstrainingSolutionMapping, CombinerError> {
        let inner_lf = self.lazy_graph_pattern(
            columns,
            input_lf,
            inner,
            &context.extension_with(PathEntry::ProjectInner),
        );
        let mut cols: Vec<Expr> = variables.iter().map(|c| col(c.as_str())).collect();
        for ts_identifier_variable_name in get_timeseries_identifier_names(time_series) {
            cols.push(col(&ts_identifier_variable_name));
        }
        inner_lf.select(cols.as_slice())
    }
}
