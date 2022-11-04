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
use crate::combiner::join_timeseries::join_tsq;
use crate::combiner::{get_timeseries_identifier_names, CombinerError};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::lf_wrap::WrapLF;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use log::debug;
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LiteralValue};
use polars_core::frame::{DataFrame, UniqueKeepStrategy};
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use crate::combiner::solution_mapping::{SolutionMappings, update_constraints};

impl Combiner {
    pub(crate) async fn lazy_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        let mut updated_solution_mappings = solution_mappings;
        let mut new_prepared_time_series_queries = prepared_time_series_queries;

        if let Some(query) = static_query_map.remove(context) {
            let (static_result_df, datatypes) = self.execute_static_query(&query, &constraints);
            let columns = static_result_df
                .get_column_names()
                .iter()
                .map(|x| x.to_string())
                .collect();
            let mut wrap_lf = WrapLF::new(static_result_df.lazy());
            let GPPrepReturn {
                time_series_queries,
                ..
            } = self
                .prepper
                .prepare_graph_pattern(graph_pattern, false, &context);
            new_prepared_time_series_queries = time_series_queries;
            updated_solution_mappings = Some(update_constraints(
                &mut updated_solution_mappings,
                wrap_lf.lf,
                columns,
                datatypes,
            ))
        }

        if let Some(tsqs) = &mut new_prepared_time_series_queries {
            if let Some(tsq) = tsqs.remove(context) {
                let solution_mapping =
                    self.execute_attach_time_series_query(&tsq, &updated_solution_mappings.unwrap()).await?;
                updated_solution_mappings = Some(SolutionMappings::new(lf, colums));
            }
        }

        if static_query_map.is_empty()
            && (prepared_time_series_queries.is_none()
                || (new_prepared_time_series_queries.is_some()
                    && new_prepared_time_series_queries.unwrap().is_empty()))
        {
            return updated_solution_mappings.unwrap
        }

        match graph_pattern {
            GraphPattern::Bgp { .. } => {panic!("This situation should never occur")},
            GraphPattern::Path { .. } => {panic!("This situation should never occur")},
            GraphPattern::Join { left, right } => self.lazy_join(
                left,
                right,
                updated_solution_mappings,
                static_query_map,
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
                updated_solution_mappings,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Filter { expr, inner } => self.lazy_filter(
                inner,
                expr,
                updated_solution_mappings,
                prepared_time_series_queriescontext,
                &context,
            ),
            GraphPattern::Union { left, right } => self.lazy_union(
                left,
                right,
                updated_solution_mappings,
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
                updated_solution_mappings,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Minus { left, right } => self.lazy_minus(
                left,
                right,
                updated_solution_mappings,
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
                updated_solution_mappings,
                new_prepared_time_series_queries,
                context,
            ),
            GraphPattern::Project { inner, variables } => self.lazy_project(
                inner,
                variables,
                updated_solution_mappings,
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
            GraphPattern::Service { .. } => Ok(SolutionMappings::empty()),
        }
    }
}
