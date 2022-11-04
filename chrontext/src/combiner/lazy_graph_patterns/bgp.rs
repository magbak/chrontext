use super::Combiner;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{IntoLazy, LazyFrame};
use spargebra::term::TriplePattern;
use std::collections::HashMap;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::{ConstrainingSolutionMapping, update_constraints};
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::lf_wrap::WrapLF;

impl Combiner {
    pub(crate) fn lazy_bgp(
        &mut self,
        constraints: Option<ConstrainingSolutionMapping>,
    ) -> Result< ConstrainingSolutionMapping, CombinerError> {
        if let Some(ConstrainingSolutionMapping{ solution_mapping, .. }) = constraints {
            let columns = solution_mapping.get_column_names().iter().map(|x|x.clone()).collect();
            return Ok( ConstrainingSolutionMapping::new(solution_mapping.lazy(), columns))
        } else {
            panic!("Should never happen")
        }
    }
}
