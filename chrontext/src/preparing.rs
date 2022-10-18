mod expressions;
mod graph_patterns;
mod synchronization;

use crate::pushdown_setting::PushdownSetting;
use crate::query_context::Context;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use polars_core::frame::DataFrame;
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct TimeSeriesQueryPrepper {
    pushdown_settings: HashSet<PushdownSetting>,
    allow_compound_timeseries_queries: bool,
    basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
    pub static_result_df: DataFrame,
    grouping_counter: u16,
    rewritten_filters: HashMap<Context, Expression>,
}

impl TimeSeriesQueryPrepper {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        allow_compound_timeseries_queries: bool,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        static_result_df: DataFrame,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> TimeSeriesQueryPrepper {
        TimeSeriesQueryPrepper {
            allow_compound_timeseries_queries,
            pushdown_settings,
            basic_time_series_queries,
            static_result_df,
            grouping_counter: 0,
            rewritten_filters,
        }
    }

    pub fn prepare(&mut self, query: &Query) -> Vec<TimeSeriesQuery> {
        if let Query::Select { pattern, .. } = query {
            let mut pattern_prepared = self.prepare_graph_pattern(pattern, false, &Context::new());
            pattern_prepared.drained_time_series_queries()
        } else {
            panic!("Only support for Select");
        }
    }
}
