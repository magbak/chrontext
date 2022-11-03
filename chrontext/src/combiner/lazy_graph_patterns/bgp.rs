use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::LazyFrame;
use spargebra::term::TriplePattern;
use std::collections::HashMap;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;

impl Combiner {
    pub(crate) fn lazy_bgp(
        &mut self,
        patterns: &Vec<TriplePattern>,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        if let Some(query) = self.static_query_map.get(context) {

        }
        let bgp_context = context.extension_with(PathEntry::BGP);
        //No action, handled statically
        let mut output_lf = input_lf;
        for p in patterns {
            output_lf =
                self.lazy_triple_pattern(columns, output_lf.unwrap(), p, time_series, &bgp_context);
        }
        output_lf
    }
}
