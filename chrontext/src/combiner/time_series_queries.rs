use std::collections::HashSet;
use super::Combiner;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::combiner::CombinerError;
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{col, Expr, IntoLazy, LazyFrame};
use polars_core::prelude::JoinType;

impl Combiner {
    pub fn execute_attach_time_series_query(
        &mut self,
        tsq: &TimeSeriesQuery,
        constraints: &ConstrainingSolutionMapping,
    ) -> Result<(LazyFrame, HashSet<String>), CombinerError> {
        let ts_df = self
            .time_series_database
            .execute(tsq)
            .await
            .map_err(|x| CombinerError::TimeSeriesQueryError(x))?;
        let ts_lf = ts_df.lazy();
        let ConstrainingSolutionMapping {
            solution_mapping, ..
        } = constraints;
        let on: Vec<Expr>;
        if let Some(colname) = tsq.get_groupby_column() {
            on = vec![col(colname)]
        } else {
            on = tsq
                .get_identifier_variables()
                .iter()
                .map(|x| col(x.as_str()))
                .collect();
        }
        let mut lf =
            solution_mapping
                .lazy()
                .join(ts_lf, on.as_slice(), on.as_slice(), JoinType::Inner);
        return Ok((lf, columns));
    }
}
