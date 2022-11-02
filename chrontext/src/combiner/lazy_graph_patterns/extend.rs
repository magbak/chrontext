use oxrdf::Variable;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::TriplePattern;

impl Combiner {
    pub(crate) fn lazy_extend(
        &mut self,
        inner: &GraphPattern,
        variable: &Variable,
        expression: &Expression,
        input_lf: Option<LazyFrame>,
        context: &Context,
    ) -> LazyFrame {
        let inner_context = context.extension_with(PathEntry::ExtendInner);
        let mut inner_lf =
            self.lazy_graph_pattern(columns, input_lf, inner, &inner_context);
        if !columns.contains(variable.as_str()) {
            inner_lf = lazy_expression(expression, inner_lf, columns, time_series, &inner_context)
                .rename([inner_context.as_str()], &[variable.as_str()]);
            columns.insert(variable.as_str().to_string());
        }
        inner_lf
    }
}
