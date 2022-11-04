use std::collections::HashMap;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_distinct(
        &mut self,
        inner: &GraphPattern,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result< ConstrainingSolutionMapping, CombinerError> {
        let  ConstrainingSolutionMapping{ solution_mapping, columns, datatypes } = self.lazy_graph_pattern(
            inner,
            constraints,
            prepared_time_series_queries,
            &context.extension_with(PathEntry::DistinctInner),
        )?;
        Ok( ConstrainingSolutionMapping::new(lf.unique_stable(None, UniqueKeepStrategy::First), columns.unwrap()))
    }
}
