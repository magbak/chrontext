use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;

impl TimeSeriesQueryPrepper {
    pub fn prepare_in_expression(
        &mut self,
        left: &Expression,
        expressions: &Vec<Expression>,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            &context.extension_with(PathEntry::InLeft),
        );
        let mut prepared: Vec<EXPrepReturn> = expressions
            .iter()
            .map(|x| self.prepare_expression(x, try_groupby_complex_query, context))
            .collect();
        if left_prepare.fail_groupby_complex_query
            || prepared.iter().any(|x| x.fail_groupby_complex_query)
        {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        for p in &mut prepared {
            left_prepare.with_time_series_queries_from(p)
        }
        left_prepare
    }
}
