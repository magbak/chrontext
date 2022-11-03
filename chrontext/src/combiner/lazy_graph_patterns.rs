mod bgp;
mod distinct;
mod extend;
mod filter;
mod group;
mod join;
mod left_join;
mod minus;
mod order_by;
mod project;
mod union;

use super::Combiner;
use crate::combiner::constraining_solution_mapping::{
    update_constraints, ConstrainingSolutionMapping,
};
use crate::combiner::join_timeseries::join_tsq;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::combiner::lazy_order::lazy_order_expression;
use crate::combiner::{get_timeseries_identifier_names, CombinerError};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::lf_wrap::WrapLF;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LiteralValue};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use spargebra::algebra::GraphPattern;
use std::collections::{HashMap, HashSet};
use std::ops::Not;

pub(crate) struct LazyGraphPatternReturn {
    pub lf: Option<LazyFrame>,
    pub columns: Option<HashSet<String>>,
}

impl LazyGraphPatternReturn {
    pub fn new(lf: LazyFrame, columns: HashSet<String>) -> LazyGraphPatternReturn {
        LazyGraphPatternReturn {
            lf: Some(lf),
            columns: Some(columns),
        }
    }

    pub fn empty() -> LazyGraphPatternReturn {
        LazyGraphPatternReturn {
            lf: None,
            columns: None,
        }
    }
}

impl Combiner {
    pub(crate) fn lazy_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let mut updated_constraints = constraints;
        let mut new_prepared_time_series_queries = prepared_time_series_queries;
        
        if let Some(query) = self.static_query_map.get(context) {
            let (static_result_df, datatypes) = self.execute_static_query(query, &constraints);
            let mut wrap_lf = WrapLF::new(static_result_df.lazy());
            let GPPrepReturn {
                time_series_queries,
                ..
            } = self.prepper.prepare_graph_pattern(graph_pattern, false, &context);
            new_prepared_time_series_queries = time_series_queries;
            updated_constraints = Some(update_constraints(
                &mut updated_constraints,
                wrap_lf.lf.collect().unwrap(),
                datatypes,
            ))
        }

        if let Some(tsqs) = &mut new_prepared_time_series_queries {
            if let Some(tsq) = tsqs.remove(context) {
                let (lf, columns) = self.execute_attach_time_series_query(&tsq, &updated_constraints.unwrap())?;
                if tsqs.is_empty() {
                    return Ok(LazyGraphPatternReturn::new(lf, colums))
                }
                else {
                    updated_constraints = Some(update_constraints(&mut updated_constraints, lf.collect().unwrap(), HashMap::new()))
                }
            }
        }

        match graph_pattern {
            GraphPattern::Bgp { .. } => {
                self.lazy_bgp(updated_constraints, new_prepared_time_series_queries, context)
            }
            GraphPattern::Path { .. } => Ok(LazyGraphPatternReturn::empty()),
            GraphPattern::Join { left, right } => self.lazy_join(
                left,
                right,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.lazy_left_join(
                left,
                right,
                expression,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Filter { expr, inner } => self.lazy_filter(inner, expr, updated_constraints, prepared_time_series_queriescontext, &context),
            GraphPattern::Union { left, right } => self.lazy_union(
                left,
                right,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Graph { name: _, inner } => {
                todo!()
            }
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => self.lazy_extend(
                inner,
                variable,
                expression,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Minus { left, right } => self.lazy_minus(
                left,
                right,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => {
                //These are handled by the static query.
                input_lf
            }
            GraphPattern::OrderBy { inner, expression } => self.lazy_order_by(
                inner,
                expression,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Project { inner, variables } => self.lazy_project(
                inner,
                variables,
                updated_constraints,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(inner, input_lf, new_prepared_time_series_queries, context)
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
            } => self.lazy_group(
                inner,
                variables,
                aggregates,
                input_lf,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Service { .. } => Ok(LazyGraphPatternReturn::empty()),
        }
    }
}
