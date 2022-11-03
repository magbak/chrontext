mod bgp;
mod join;
mod group;
mod left_join;
mod union;
mod project;
mod extend;
mod minus;
mod distinct;
mod filter;
mod order_by;

use std::collections::{HashMap, HashSet};
use std::ops::Not;
use log::debug;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LiteralValue};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use spargebra::algebra::GraphPattern;
use crate::combiner::{CombinerError, get_timeseries_identifier_names};
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::join_timeseries::join_tsq;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::combiner::lazy_order::lazy_order_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use super::Combiner;

pub(crate) struct LazyGraphPatternReturn {
    pub lf: Option<LazyFrame>,
    pub columns: Option<HashSet<String>>
}

impl LazyGraphPatternReturn {
    pub fn new(lf:LazyFrame, columns:HashSet<String>) -> LazyGraphPatternReturn {
        LazyGraphPatternReturn{ lf: Some(lf), columns: Some(columns) }
    }

    pub fn empty() -> LazyGraphPatternReturn {
        LazyGraphPatternReturn{ lf: None, columns: None }
    }
}

impl Combiner {
    pub(crate) fn lazy_graph_pattern(
        &mut self,
        columns: &mut HashSet<String>,
        graph_pattern: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {

        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                self.lazy_bgp(patterns, constraints, prepared_time_series_queries,  context)
            }
            GraphPattern::Path { .. } => {
                Ok(LazyGraphPatternReturn::empty())
            }
            GraphPattern::Join { left, right } => {
                self.lazy_join(left, right, constraints,  prepared_time_series_queries, context)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.lazy_left_join(left, right, expression, constraints, prepared_time_series_queries, context)
            }
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter(inner, expr, constraints, prepared_time_series_queriescontext)
            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(left, right, constraints, prepared_time_series_queries, context)
            }
            GraphPattern::Graph { name: _, inner } => {
                todo!()
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                self.lazy_extend(inner, variable, expression, constraints, prepared_time_series_queries, context)
            }
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(left, right, constraints, prepared_time_series_queries, context)
            }
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => {
                //These are handled by the static query.
                input_lf
            }
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(inner, expression, constraints, prepared_time_series_queries, context)
            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(inner, variables, constraints, prepared_time_series_queries, context)
            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(inner, input_lf, prepared_time_series_queries, context)
            }
            GraphPattern::Reduced { .. } => {
                todo!()
            }
            GraphPattern::Slice { .. } => {
                todo!()
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                self.lazy_group(inner, variables, aggregates, input_lf, prepared_time_series_queries, context)
            }
            GraphPattern::Service { .. } => {
                Ok(LazyGraphPatternReturn::empty())
            }
        }
    }
}