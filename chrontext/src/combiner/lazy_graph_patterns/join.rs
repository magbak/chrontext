use polars::prelude::LazyFrame;
use spargebra::algebra::GraphPattern;
use crate::query_context::{Context, PathEntry};
use super::Combiner;

impl Combiner {
    pub(crate) fn lazy_join(&mut self, left: &GraphPattern,
        right: &GraphPattern, input_lf:Option<LazyFrame>, context:&Context) -> LazyFrame {
        let left_lf = self.lazy_graph_pattern(
            columns,
            input_lf,
            left,
            &context.extension_with(PathEntry::JoinLeftSide),
        );
        let right_lf = self.lazy_graph_pattern(
            columns,
            left_lf,
            right,
            &context.extension_with(PathEntry::JoinRightSide),
        );
        right_lf
    }
}