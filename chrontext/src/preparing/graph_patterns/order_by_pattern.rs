use super::TimeSeriesQueryPrepper;
use crate::preparing::graph_patterns::GPPrepReturn;

use crate::query_context::{Context, PathEntry};
use log::debug;
use spargebra::algebra::{GraphPattern, OrderExpression};

impl TimeSeriesQueryPrepper {
    pub fn prepare_order_by(
        &mut self,
        inner: &GraphPattern,
        _order_expressions: &Vec<OrderExpression>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> GPPrepReturn {
        if try_groupby_complex_query {
            debug!("Encountered graph inside order by, not supported for complex groupby pushdown");
            return GPPrepReturn::fail_groupby_complex_query();
        } else {
            let inner_prepare = self.prepare_graph_pattern(
                inner,
                try_groupby_complex_query,
                &context.extension_with(PathEntry::OrderByInner),
            );
            inner_prepare
        }
    }
}
