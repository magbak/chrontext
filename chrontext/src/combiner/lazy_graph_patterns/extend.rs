use std::collections::HashMap;
use oxrdf::Variable;
use super::Combiner;
use crate::query_context::{Context, PathEntry};
use polars::prelude::{IntoLazy, LazyFrame};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::TriplePattern;
use crate::combiner::CombinerError;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
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
    ) -> Result< ConstrainingSolutionMapping, CombinerError> {
        let inner_context = context.extension_with(PathEntry::ExtendInner);

        let  ConstrainingSolutionMapping{ mut solution_mapping, mut columns, datatypes } =
            self.lazy_graph_pattern(inner, constraints, prepared_time_series_queries,  &inner_context)?;
        if !columns.unwrap().contains(variable.as_str()) {
            let out_lf = self.lazy_expression(expression, lf.unwrap(), &columns.unwrap(), time_series, &inner_context)
                .rename([inner_context.as_str()], &[variable.as_str()]);
            columns.unwrap().insert(variable.as_str().to_string());
            return Ok( ConstrainingSolutionMapping::new(out_lf, columns.unwrap()))
        } else {
            return Ok( ConstrainingSolutionMapping::new(lf.unwrap(), columns.unwrap()))
        }
    }
}
