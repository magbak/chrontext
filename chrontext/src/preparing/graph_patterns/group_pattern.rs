use crate::query_context::{Context, PathEntry};
use log::debug;
use std::collections::HashSet;

use super::TimeSeriesQueryPrepper;
use crate::constants::GROUPING_COL;
use crate::find_query_variables::find_all_used_variables_in_aggregate_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::pushdown_setting::PushdownSetting;
use crate::timeseries_query::{GroupedTimeSeriesQuery, TimeSeriesQuery};
use oxrdf::Variable;
use polars::prelude::LazyFrame;
use polars_core::frame::DataFrame;
use polars_core::prelude::{JoinType, UniqueKeepStrategy};
use polars_core::series::Series;
use spargebra::algebra::{AggregateExpression, GraphPattern};
use crate::preparing::lf_wrap::WrapLF;

impl TimeSeriesQueryPrepper {
    pub fn prepare_group(
        &mut self,
        graph_pattern: &GraphPattern,
        by: &Vec<Variable>,
        aggregations: &Vec<(Variable, AggregateExpression)>,
        try_groupby_complex_query: bool,
        wrap_lf: &mut WrapLF,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            return GPPrepReturn::fail_groupby_complex_query();
        }
        let inner_context = &context.extension_with(PathEntry::GroupInner);
        let mut try_graph_pattern_prepare =
            self.prepare_graph_pattern(graph_pattern, true, &inner_context);
        if !try_graph_pattern_prepare.fail_groupby_complex_query
            && self.pushdown_settings.contains(&PushdownSetting::GroupBy)
        {
            let mut time_series_queries = try_graph_pattern_prepare.drained_time_series_queries();

            if time_series_queries.len() == 1 {
                let mut tsq = time_series_queries.remove(0);
                let in_scope = check_aggregations_are_in_scope(&tsq, inner_context, aggregations);

                if in_scope {
                    let grouping_col = self.add_grouping_col(by);
                    tsq = add_basic_groupby_mapping_values(
                        tsq,
                        static_result_df,
                        &grouping_col,
                    );
                    let tsfuncs = tsq.get_timeseries_functions(context);
                    let mut keep_by = vec![Variable::new_unchecked(&grouping_col)];
                    for v in by {
                        for (v2, _) in &tsfuncs {
                            if v2.as_str() == v.as_str() {
                                keep_by.push(v.clone())
                            }
                        }
                    }
                    //TODO: For OPC UA we must ensure that mapping df is 1:1 with identities, or alternatively group on these

                    tsq = TimeSeriesQuery::Grouped(GroupedTimeSeriesQuery {
                        tsq: Box::new(tsq),
                        graph_pattern_context: context.clone(),
                        by: keep_by,
                        aggregations: aggregations.clone(),
                    });
                    return GPPrepReturn::new(vec![tsq]);
                }
            }
        }

        self.prepare_graph_pattern(
            graph_pattern,
            false,
            &context.extension_with(PathEntry::GroupInner),
        )
    }

    fn add_grouping_col(&mut self, by: &Vec<Variable>) -> String {
        let grouping_col = format!("{}_{}", GROUPING_COL, self.grouping_counter);
        self.grouping_counter += 1;
        let by_names: Vec<String> = by
            .iter()
            .filter(|x| {
                self.static_result_df
                    .get_column_names()
                    .contains(&x.as_str())
            })
            .map(|x| x.as_str().to_string())
            .collect();
        let mut df = self
            .static_result_df
            .select(by_names.as_slice())
            .unwrap()
            .unique(Some(by_names.as_slice()), UniqueKeepStrategy::First)
            .unwrap();
        let mut series = Series::from_iter(0..(df.height() as i64));
        series.rename(&grouping_col);
        df.with_column(series).unwrap();
        self.static_result_df = self
            .static_result_df
            .join(
                &df,
                by_names.as_slice(),
                by_names.as_slice(),
                JoinType::Inner,
                None,
            )
            .unwrap();
        grouping_col
    }
}

fn check_aggregations_are_in_scope(
    tsq: &TimeSeriesQuery,
    context: &Context,
    aggregations: &Vec<(Variable, AggregateExpression)>,
) -> bool {
    for (_, ae) in aggregations {
        let mut used_vars = HashSet::new();
        find_all_used_variables_in_aggregate_expression(ae, &mut used_vars);
        for v in &used_vars {
            if tsq.has_equivalent_timestamp_variable(v, context) {
                continue;
            } else if tsq.has_equivalent_value_variable(v, context) {
                continue;
            } else {
                debug!("Variable {:?} in aggregate expression not in scope", v);
                return false;
            }
        }
    }
    true
}

fn add_basic_groupby_mapping_values(
    tsq: TimeSeriesQuery,
    static_result_df: &DataFrame,
    grouping_col: &str,
) -> TimeSeriesQuery {
    match tsq {
        TimeSeriesQuery::Basic(b) => {
            let by_vec = vec![
                grouping_col,
                b.identifier_variable.as_ref().unwrap().as_str(),
            ];
            let df = static_result_df.select(by_vec).unwrap();
            TimeSeriesQuery::GroupedBasic(b, df, grouping_col.to_string())
        }
        TimeSeriesQuery::Filtered(tsq, f) => TimeSeriesQuery::Filtered(
            Box::new(add_basic_groupby_mapping_values(
                *tsq,
                static_result_df,
                grouping_col,
            )),
            f,
        ),
        TimeSeriesQuery::InnerSynchronized(inners, syncs) => {
            let mut tsq_added = vec![];
            for tsq in inners {
                tsq_added.push(Box::new(add_basic_groupby_mapping_values(
                    *tsq,
                    static_result_df,
                    grouping_col,
                )))
            }
            TimeSeriesQuery::InnerSynchronized(tsq_added, syncs)
        }
        TimeSeriesQuery::ExpressionAs(tsq, v, e) => TimeSeriesQuery::ExpressionAs(
            Box::new(add_basic_groupby_mapping_values(
                *tsq,
                static_result_df,
                grouping_col,
            )),
            v,
            e,
        ),
        TimeSeriesQuery::Grouped(_) => {
            panic!("Should never happen")
        }
        TimeSeriesQuery::GroupedBasic(_, _, _) => {
            panic!("Should never happen")
        }
    }
}
