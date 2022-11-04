use std::collections::{HashMap};
use super::Combiner;
use crate::combiner::CombinerError;
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{col, Expr, IntoLazy};
use polars_core::prelude::JoinType;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::query_context::Context;

impl Combiner {
    pub async fn execute_attach_time_series_query(
        &mut self,
        tsq: &TimeSeriesQuery,
        constraints: &SolutionMappings,
    ) -> Result<SolutionMappings, CombinerError> {
        let ts_df = self
            .time_series_database
            .execute(tsq)
            .await
            .map_err(|x| CombinerError::TimeSeriesQueryError(x))?;
        let ts_lf = ts_df.lazy();
        let SolutionMappings {
            mappings: solution_mapping, ..
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

pub(crate) fn split_time_series_queries(time_series_queries: &mut Option<HashMap<Context, TimeSeriesQuery>>, context:&Context) -> Option<HashMap<Context, TimeSeriesQuery>> {
    if let Some(tsqs) = time_series_queries {
        let mut split_keys = vec![];
        for k in &tsqs.keys() {
            if k.path.iter().zip(context.path()).map(|(x, y)| x == y).all() {
                split_keys.push(k.clone())
            }
        }
        let mut new_map = HashMap::new();
        for k in split_keys {
            new_map.insert(k, tsqs.remove(&k).unwrap());
        }
        Some(new_map)
    } else {
        None
    }
}

