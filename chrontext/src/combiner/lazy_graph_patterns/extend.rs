use std::collections::HashMap;
use oxrdf::Variable;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::{IntoLazy, LazyFrame};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::TriplePattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::combiner::lazy_expressions::lazy_expression;
use crate::combiner::lazy_graph_patterns::LazyGraphPatternReturn;
use crate::preparing::graph_patterns::GPPrepReturn;
use crate::preparing::lf_wrap::WrapLF;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_extend(
        &mut self,
        inner: &GraphPattern,
        variable: &Variable,
        expression: &Expression,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result<LazyGraphPatternReturn, CombinerError> {
        let inner_context = context.extension_with(PathEntry::ExtendInner);

        let mut inner_lf =
            self.lazy_graph_pattern(columns, input_lf, inner, &inner_context);
        if !columns.contains(variable.as_str()) {
            inner_lf = lazy_expression(expression, constraints, columns, time_series, &inner_context)
                .rename([inner_context.as_str()], &[variable.as_str()]);
            columns.insert(variable.as_str().to_string());
        }
        inner_lf
    }
}
