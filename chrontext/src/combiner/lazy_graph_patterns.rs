mod bgp;
mod join;
mod group;
mod left_join;
mod union;
mod project;
mod extend;
mod minus;
mod distinct;
mod path;
mod filter;
mod order_by;

use std::collections::HashSet;
use std::ops::Not;
use log::debug;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LiteralValue};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use spargebra::algebra::GraphPattern;
use crate::combiner::get_timeseries_identifier_names;
use crate::combiner::join_timeseries::join_tsq;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::combiner::lazy_order::lazy_order_expression;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use super::Combiner;

impl Combiner {
    pub(crate) fn lazy_graph_pattern(
        &mut self,
        columns: &mut HashSet<String>,
        input_lf: Option<LazyFrame>,
        graph_pattern: &GraphPattern,
        context: &Context,
    ) -> LazyFrame {

        match graph_pattern {
            GraphPattern::Bgp { patterns } => {
                self.lazy_bgp(patterns, input_lf, context)
            }
            GraphPattern::Path { .. } => {
                self.lazy_path()
            }
            GraphPattern::Join { left, right } => {
                self.lazy_join(left, right)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.lazy_left_join(left, right, expression, input_lf, context)
            }
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter()
            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(left, right, input_lf, context)
            }
            GraphPattern::Graph { name: _, inner } => {
                todo!()
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                self.lazy_extend(inner, variable, expression, input_lf, context)
            }
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(left, right, input_lf, context)
            }
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => {
                //These are handled by the static query.
                input_lf
            }
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(inner, expression)
            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(inner, variables, input_lf, context)
            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(inner, input_lf, context)
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
                self.lazy_group(inner, variables, aggregates, input_lf, context)
            }
            GraphPattern::Service { .. } => {
                todo!()
            }
        }
    }
}