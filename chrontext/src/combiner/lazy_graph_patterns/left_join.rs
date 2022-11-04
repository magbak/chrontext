use std::collections::HashMap;
use super::Combiner;
use crate::combiner::{CombinerError, get_timeseries_identifier_names};
use crate::combiner::lazy_expressions::lazy_expression;
use crate::query_context::{Context, PathEntry};
use polars::prelude::{col, concat, Expr, IntoLazy, LazyFrame, LiteralValue};
use spargebra::algebra::{Expression, GraphPattern};
use std::ops::Not;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use crate::timeseries_query::TimeSeriesQuery;

impl Combiner {
    pub(crate) fn lazy_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression: &Option<Expression>,
        constraints: Option<ConstrainingSolutionMapping>,
        prepared_time_series_queries: Option<HashMap<Context, TimeSeriesQuery>>,
        context: &Context,
    ) -> Result< ConstrainingSolutionMapping, CombinerError> {
        let left_join_distinct_column = context.as_str();
        let left_context = context.extension_with(PathEntry::LeftJoinLeftSide);
        let right_context = context.extension_with(PathEntry::LeftJoinRightSide);
        let mut left_df = self
            .lazy_graph_pattern(
                left,
                constraints,
                prepared_time_series_queries,
                &left_context,
            )?
            .with_column(Expr::Literal(LiteralValue::Int64(1)).alias(&left_join_distinct_column))
            .with_column(col(&left_join_distinct_column).cumsum(false).keep_name())
            .collect()
            .expect("Left join collect left problem");

        let ts_identifiers = get_timeseries_identifier_names(time_series);
        let mut right_lf = self.lazy_graph_pattern(
            right,
            left_df.clone().lazy(),
            prepared_time_series_queries
            &right_context,
        );

        if let Some(expr) = expression {
            let expression_context = context.extension_with(PathEntry::LeftJoinExpression);
            right_lf = lazy_expression(expr, right_lf, columns, time_series, &expression_context);
            right_lf = right_lf
                .filter(col(&expression_context.as_str()))
                .drop_columns([&expression_context.as_str()]);
        }

        let right_df = right_lf.collect().expect("Collect right problem");

        for id in ts_identifiers {
            if !columns.contains(&id) {
                left_df = left_df.drop(&id).expect("Drop problem");
            }
        }
        left_df = left_df
            .filter(
                &left_df
                    .column(&left_join_distinct_column)
                    .expect("Did not find left helper")
                    .is_in(
                        right_df
                            .column(&left_join_distinct_column)
                            .expect("Did not find right helper"),
                    )
                    .expect("Is in problem")
                    .not(),
            )
            .expect("Filter problem");

        for c in right_df.get_column_names_owned().iter() {
            if !left_df.get_column_names().contains(&c.as_str()) {
                left_df = left_df
                    .lazy()
                    .with_column(Expr::Literal(LiteralValue::Null).alias(c))
                    .collect()
                    .expect("Not ok");
                left_df
                    .with_column(
                        left_df
                            .column(c)
                            .expect("Col c prob")
                            .cast(right_df.column(c).unwrap().dtype())
                            .expect("Cast error"),
                    )
                    .expect("TODO: panic message");
            }
        }

        let mut output_lf =
            concat(vec![left_df.lazy(), right_df.lazy()], true, true).expect("Concat error");
        output_lf = output_lf.drop_columns(&[&left_join_distinct_column]);
        output_lf = output_lf
            .collect()
            .expect("Left join collect problem")
            .lazy();
        output_lf
    }
}
