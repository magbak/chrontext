mod join_timeseries;
pub(crate) mod lazy_aggregate;
pub(crate) mod lazy_expressions;
pub(crate) mod lazy_graph_patterns;
mod lazy_order;
pub(crate) mod static_subqueries;
mod constraining_solution_mapping;
pub(crate) mod time_series_queries;

use crate::combiner::lazy_aggregate::sparql_aggregate_expression_as_lazy_column_and_expression;
use crate::query_context::{Context, PathEntry};

use crate::preparing::TimeSeriesQueryPrepper;
use crate::pushdown_setting::PushdownSetting;
use crate::rewriting::subqueries::SubQueryInContext;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use oxrdf::Variable;
use polars::frame::DataFrame;
use polars::prelude::{col, Expr, IntoLazy, LazyFrame, UniqueKeepStrategy};
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::error::Error;

#[derive(Debug)]
pub enum CombinerError {
    TimeSeriesQueryError(Box<dyn Error>)
}

pub struct Combiner {
    counter: u16,
    endpoint: String,
    time_series_database: Box<dyn TimeSeriesQueryable>,
    static_query_map: HashMap<Context, Query>,
    static_subqueries_in_context: Vec<SubQueryInContext>,
    prepper: TimeSeriesQueryPrepper,
}

impl Combiner {
    pub fn new(
        endpoint: String,
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        static_query_map: HashMap<Context, Query>,
        rewritten_filters: HashMap<Context, Expression>,
        static_subqueries_in_context: Vec<SubQueryInContext>,
    ) -> Combiner {
        let prepper = TimeSeriesQueryPrepper::new(
            pushdown_settings,
            basic_time_series_queries,
            rewritten_filters,
        );
        Combiner {
            counter: 0,
            endpoint,
            time_series_database,
            static_query_map,
            static_subqueries_in_context,
            prepper,
        }
    }

    pub async fn combine_static_and_time_series_results(&mut self, query: &Query) -> LazyFrame {
        let project_variables;
        let inner_graph_pattern;
        let mut distinct = false;
        let mut context = Context::new();
        if let Query::Select {
            dataset: _,
            pattern,
            base_iri: _,
        } = query
        {
            if let GraphPattern::Project { inner, variables } = pattern {
                project_variables = variables.clone();
                inner_graph_pattern = inner;
                context = context.extension_with(PathEntry::ProjectInner);
            } else if let GraphPattern::Distinct { inner } = pattern {
                context = context.extension_with(PathEntry::DistinctInner);
                if let GraphPattern::Project { inner, variables } = inner.as_ref() {
                    distinct = true;
                    project_variables = variables.clone();
                    inner_graph_pattern = inner;
                    context = context.extension_with(PathEntry::ProjectInner);
                } else {
                    panic!("Wrong!");
                }
            } else {
                panic!("Also wrong!");
            }
        } else {
            panic!("Wrong!!!");
        }
        let mut columns = static_result_df
            .get_column_names()
            .iter()
            .map(|c| c.to_string())
            .collect();

        let mut lf = static_result_df.lazy();
        lf = self.lazy_graph_pattern(&mut columns, lf, inner_graph_pattern, time_series, &context);
        let projections = project_variables
            .iter()
            .map(|c| col(c.as_str()))
            .collect::<Vec<Expr>>();
        lf = lf.select(projections.as_slice());
        if distinct {
            lf = lf.unique_stable(None, UniqueKeepStrategy::First);
        }
        lf
    }
}

fn get_timeseries_identifier_names(
    time_series: &mut Vec<(TimeSeriesQuery, DataFrame)>,
) -> Vec<String> {
    time_series.iter().fold(vec![], |mut coll, (tsq, _)| {
        coll.extend(
            tsq.get_identifier_variables()
                .iter()
                .map(|x| x.as_str().to_string()),
        );
        coll
    })
}
