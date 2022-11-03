mod bgp_pattern;
mod distinct_pattern;
mod extend_pattern;
pub(crate) mod filter_expression_rewrites;
mod filter_pattern;
mod graph_pattern;
mod group_pattern;
mod join_pattern;
mod left_join_pattern;
mod minus_pattern;
mod order_by_pattern;
mod path_pattern;
mod project_pattern;
mod reduced_pattern;
mod service_pattern;
mod sliced_pattern;
mod union_pattern;
mod values_pattern;

use std::collections::HashMap;
use super::TimeSeriesQueryPrepper;
use crate::query_context::Context;
use crate::timeseries_query::TimeSeriesQuery;
use spargebra::algebra::GraphPattern;

#[derive(Debug)]
pub struct GPPrepReturn {
    pub fail_groupby_complex_query: bool,
    pub time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
}

impl GPPrepReturn {
    fn new(time_series_queries: Vec<TimeSeriesQuery>) -> GPPrepReturn {
        GPPrepReturn {
            fail_groupby_complex_query: false,
            time_series_queries,
        }
    }

    pub fn fail_groupby_complex_query() -> GPPrepReturn {
        GPPrepReturn {
            fail_groupby_complex_query: true,
            time_series_queries: vec![],
        }
    }

    pub fn drained_time_series_queries(&mut self) -> Vec<TimeSeriesQuery> {
        self.time_series_queries
            .drain(0..self.time_series_queries.len())
            .collect()
    }

    pub fn with_time_series_queries_from(&mut self, other: &mut GPPrepReturn) {
        self.time_series_queries
            .extend(other.drained_time_series_queries())
    }
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        match graph_pattern {
            GraphPattern::Bgp { patterns: _ } => {
                self.prepare_bgp(try_groupby_complex_query, context)
            }
            GraphPattern::Path {
                subject,
                path,
                object,
            } => self.prepare_path(subject, path, object),
            GraphPattern::Join { left, right } => {
                self.prepare_join(left, right, try_groupby_complex_query, context)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.prepare_left_join(left, right, expression, try_groupby_complex_query, context)
            }
            GraphPattern::Filter { expr, inner } => {
                self.prepare_filter(expr, inner, try_groupby_complex_query, context)
            }
            GraphPattern::Union { left, right } => {
                self.prepare_union(left, right, try_groupby_complex_query, context)
            }
            GraphPattern::Graph { inner, .. } => {
                self.prepare_graph(inner, try_groupby_complex_query, context)
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.prepare_extend(
                inner,
                variable,
                expression,
                try_groupby_complex_query,
                context,
            ),
            GraphPattern::Minus { left, right } => {
                self.prepare_minus(left, right, try_groupby_complex_query, context)
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => self.prepare_values(variables, bindings),
            GraphPattern::OrderBy { inner, expression } => {
                self.prepare_order_by(inner, expression, try_groupby_complex_query, context)
            }
            GraphPattern::Project { inner, variables } => {
                self.prepare_project(inner, variables, try_groupby_complex_query, context)
            }
            GraphPattern::Distinct { inner } => {
                self.prepare_distinct(inner, try_groupby_complex_query, context)
            }
            GraphPattern::Reduced { inner } => {
                self.prepare_reduced(inner, try_groupby_complex_query, context)
            }
            GraphPattern::Slice { inner, .. } => {
                self.prepare_slice(inner, try_groupby_complex_query, context)
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.prepare_group(
                inner,
                variables,
                aggregates,
                try_groupby_complex_query,
                context,
            ),
            GraphPattern::Service { .. } => self.prepare_service(),
        }
    }
}
