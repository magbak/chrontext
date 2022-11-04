mod expressions;
pub(crate) mod graph_patterns;
mod synchronization;
pub mod lf_wrap;

use crate::pushdown_setting::PushdownSetting;
use crate::query_context::Context;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use spargebra::algebra::Expression;
use spargebra::Query;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct TimeSeriesQueryPrepper {
    pushdown_settings: HashSet<PushdownSetting>,
    basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
    grouping_counter: u16,
    rewritten_filters: HashMap<Context, Expression>,
}

impl TimeSeriesQueryPrepper {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        basic_time_series_queries: Vec<BasicTimeSeriesQuery>,
        rewritten_filters: HashMap<Context, Expression>,
    ) -> TimeSeriesQueryPrepper {
        TimeSeriesQueryPrepper {
            pushdown_settings,
            basic_time_series_queries,
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
