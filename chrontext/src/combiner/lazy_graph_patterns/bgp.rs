use polars::prelude::LazyFrame;
use spargebra::term::TriplePattern;
use crate::query_context::{Context, PathEntry};
use super::Combiner;

impl Combiner {
    pub(crate) fn lazy_bgp(&mut self, patterns: &Vec<TriplePattern>, input_lf:Option<LazyFrame>, context:&Context) -> LazyFrame {
        let bgp_context = context.extension_with(PathEntry::BGP);
        //No action, handled statically
        let mut output_lf = input_lf;
        for p in patterns {
            output_lf =
                self.lazy_triple_pattern(columns, output_lf.unwrap(), p, time_series, &bgp_context);
        }
        output_lf
    }
}