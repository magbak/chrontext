use super::TimeSeriesQueryPrepper;
use crate::find_query_variables::find_all_used_variables_in_expression;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::query_context::{Context, PathEntry};
use crate::timeseries_query::TimeSeriesQuery;
use oxrdf::Variable;
use spargebra::algebra::{Expression, GraphPattern};
use std::collections::HashSet;

impl TimeSeriesQueryPrepper {
    pub(crate) fn prepare_extend(
        &mut self,
        inner: &GraphPattern,
        var: &Variable,
        expr: &Expression,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        let mut inner_prepare = self.prepare_graph_pattern(
            inner,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::ExtendInner),
        );
        if try_groupby_complex_query {
            let mut expression_vars = HashSet::new();
            find_all_used_variables_in_expression(expr, &mut expression_vars);
            let mut found_i = None;
            for (i, tsq) in inner_prepare.time_series_queries.iter().enumerate() {
                let mut found_all = true;
                let mut found_some = false;
                for expression_var in &expression_vars {
                    if tsq.has_equivalent_value_variable(expression_var, context) {
                        found_some = true;
                    } else if tsq.has_equivalent_timestamp_variable(expression_var, context) {
                        found_some = true;
                    } else {
                        found_all = false;
                        break;
                    }
                }
                if found_all && found_some {
                    found_i = Some(i);
                }
            }
            if let Some(i) = found_i {
                let inner_tsq = inner_prepare.time_series_queries.remove(i);
                let new_tsq =
                    TimeSeriesQuery::ExpressionAs(Box::new(inner_tsq), var.clone(), expr.clone());
                inner_prepare.time_series_queries.push(new_tsq);
                inner_prepare
            } else {
                GPPrepReturn::fail_groupby_complex_query()
            }
        } else {
            inner_prepare
        }
    }
}
