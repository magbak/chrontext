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
use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::CombinerError;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use async_recursion::async_recursion;
use log::debug;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::collections::HashMap;

impl Combiner {
    #[async_recursion]
    pub(crate) async fn lazy_graph_pattern(
        &mut self,
        graph_pattern: &GraphPattern,
        solution_mappings: Option<SolutionMappings>,
        mut static_query_map: HashMap<Context, Query>,
        prepared_time_series_queries: Option<HashMap<Context, Vec<TimeSeriesQuery>>>,
        context: &Context,
    ) -> Result<SolutionMappings, CombinerError> {
        println!("Context: {:?}", context);
        println!("Static query map: {:?}", static_query_map);
        println!("Prpeared tsqs {:?}", prepared_time_series_queries);

        let mut updated_solution_mappings = solution_mappings;
        let mut new_prepared_time_series_queries = prepared_time_series_queries;

        //We have to eagerly evaluate static queries contained in the group by pattern since otherwise we are unable to push down the group by into the time series database.
        let mut found_group_by_pushdown = false;
        let mut static_query_opt = static_query_map.remove(&context);
        if static_query_opt.is_none() {
            let groupby_inner_context = context.extension_with(PathEntry::GroupInner);
            static_query_opt = static_query_map.remove(&groupby_inner_context);
            if static_query_opt.is_some() {
                found_group_by_pushdown = true;
            }
        }

        if let Some(query) = static_query_opt {
            let mut new_solution_mappings = self
                .execute_static_query(&query, updated_solution_mappings)
                .await?;
            let GPPrepReturn {
                time_series_queries,
                ..
            } = self.prepper.prepare_graph_pattern(
                graph_pattern,
                false,
                &mut new_solution_mappings,
                &context,
            );
            println!(
                "New solution mappings {}",
                new_solution_mappings.mappings.clone().collect().unwrap()
            );
            updated_solution_mappings = Some(new_solution_mappings);
            new_prepared_time_series_queries = Some(time_series_queries);
        }

        if let Some(tsqs_map) = &mut new_prepared_time_series_queries {
            println!("TSQS: {:?}, context: {:?}", tsqs_map, context);
            if let Some(tsqs) = tsqs_map.remove(context) {
                for tsq in tsqs {
                    debug!("Attached time series query at context: {:?}", context);
                    let new_solution_mappings = self
                        .execute_attach_time_series_query(&tsq, updated_solution_mappings.unwrap())
                        .await?;
                    updated_solution_mappings = Some(new_solution_mappings);
                }
            }
        }

        if found_group_by_pushdown
            && (new_prepared_time_series_queries.is_none()
                || (new_prepared_time_series_queries.is_some()
                    && new_prepared_time_series_queries.as_ref().unwrap().is_empty()))
        {
            return Ok(updated_solution_mappings.unwrap());
        }

        if static_query_map.is_empty()
            && updated_solution_mappings.is_none()
            && (new_prepared_time_series_queries.is_none()
                || (new_prepared_time_series_queries.is_some()
                    && new_prepared_time_series_queries
                        .as_ref()
                        .unwrap()
                        .is_empty()))
        {
            return Ok(updated_solution_mappings.unwrap());
        }

        match graph_pattern {
            GraphPattern::Bgp { .. } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::Path { .. } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::Join { left, right } => {
                self.lazy_join(
                    left,
                    right,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.lazy_left_join(
                    left,
                    right,
                    expression,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Filter { expr, inner } => {
                self.lazy_filter(
                    inner,
                    expr,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    &context,
                )
                .await
            }
            GraphPattern::Union { left, right } => {
                self.lazy_union(
                    left,
                    right,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Graph { name: _, inner: _ } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => {
                self.lazy_extend(
                    inner,
                    variable,
                    expression,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Minus { left, right } => {
                self.lazy_minus(
                    left,
                    right,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => Ok(updated_solution_mappings.unwrap()),
            GraphPattern::OrderBy { inner, expression } => {
                self.lazy_order_by(
                    inner,
                    expression,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Project { inner, variables } => {
                self.lazy_project(
                    inner,
                    variables,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Distinct { inner } => {
                self.lazy_distinct(
                    inner,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
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
                self.lazy_group(
                    inner,
                    variables,
                    aggregates,
                    updated_solution_mappings,
                    static_query_map,
                    new_prepared_time_series_queries,
                    context,
                )
                .await
            }
            GraphPattern::Service { .. } => Ok(updated_solution_mappings.unwrap()),
        }
    }
}
