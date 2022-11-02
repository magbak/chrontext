use crate::combiner::join_timeseries::join_tsq;
use crate::constants::HAS_VALUE;
use crate::query_context::Context;
use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{DataFrame, LazyFrame};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use std::collections::HashSet;
use super::Combiner;

impl Combiner {
    pub(crate) fn lazy_triple_pattern(
        columns: &mut HashSet<String>,
        input_lf: LazyFrame,
        triple_pattern: &TriplePattern,
        context: &Context,
    ) -> LazyFrame {
        let mut found_index = None;
        if let NamedNodePattern::NamedNode(pn) = &triple_pattern.predicate {
            if pn.as_str() == HAS_VALUE {
                if let TermPattern::Variable(obj_var) = &triple_pattern.object {
                    if !columns.contains(obj_var.as_str()) {
                        for i in 0..time_series.len() {
                            let (tsq, _) = time_series.get(i).unwrap();
                            if tsq.has_equivalent_value_variable(obj_var, context) {
                                found_index = Some(i);
                                break;
                            }
                        }
                    }
                }
            }
        }

        if let Some(i) = found_index {
            let (tsq, df) = time_series.remove(i);
            return join_tsq(columns, input_lf, tsq, df);
        }
        input_lf
    }
}
