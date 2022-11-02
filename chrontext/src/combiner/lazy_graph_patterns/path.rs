use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use spargebra::algebra::GraphPattern;

impl Combiner {
    pub(crate) fn lazy_path(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        input_lf: Option<LazyFrame>,
        context: &Context,
    ) -> LazyFrame {
        //No action, handled statically
                input_lf
    }
}
