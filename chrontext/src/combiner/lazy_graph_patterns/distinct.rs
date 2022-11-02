use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::LazyFrame;
use polars_core::frame::UniqueKeepStrategy;
use spargebra::algebra::GraphPattern;

impl Combiner {
    pub(crate) fn lazy_distinct(
        &mut self,
        inner: &GraphPattern,
        input_lf: Option<LazyFrame>,
        context: &Context,
    ) -> LazyFrame {
        self.lazy_graph_pattern(
            columns,
            input_lf,
            inner,
            &context.extension_with(PathEntry::DistinctInner),
        )
        .unique_stable(None, UniqueKeepStrategy::First)
    }
}
