use super::TimeSeriesQueryPrepper;
use crate::preparing::expressions::EXPrepReturn;
use crate::query_context::{Context, PathEntry};
use spargebra::algebra::Expression;

pub enum BinaryOrdinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    LessOrEqual,
    Less,
    Greater,
    GreaterOrEqual,
    SameTerm,
    Equal,
}

impl TimeSeriesQueryPrepper {
    pub fn prepare_binary_ordinary_expression(
        &mut self,
        left: &Expression,
        right: &Expression,
        operation: &BinaryOrdinaryOperator,
        try_groupby_complex_query: bool,
        context: &Context,
    ) -> EXPrepReturn {
        let (left_path_entry, right_path_entry) = match { operation } {
            BinaryOrdinaryOperator::Add => (PathEntry::AddLeft, PathEntry::AddRight),
            BinaryOrdinaryOperator::Subtract => (PathEntry::SubtractLeft, PathEntry::SubtractRight),
            BinaryOrdinaryOperator::Multiply => (PathEntry::MultiplyLeft, PathEntry::MultiplyRight),
            BinaryOrdinaryOperator::Divide => (PathEntry::DivideLeft, PathEntry::DivideRight),
            BinaryOrdinaryOperator::LessOrEqual => {
                (PathEntry::LessOrEqualLeft, PathEntry::LessOrEqualRight)
            }
            BinaryOrdinaryOperator::Less => (PathEntry::LessLeft, PathEntry::LessRight),
            BinaryOrdinaryOperator::Greater => (PathEntry::GreaterLeft, PathEntry::GreaterRight),
            BinaryOrdinaryOperator::GreaterOrEqual => (
                PathEntry::GreaterOrEqualLeft,
                PathEntry::GreaterOrEqualRight,
            ),
            BinaryOrdinaryOperator::SameTerm => (PathEntry::SameTermLeft, PathEntry::SameTermRight),
            BinaryOrdinaryOperator::Equal => (PathEntry::EqualLeft, PathEntry::EqualRight),
        };

        let mut left_prepare = self.prepare_expression(
            left,
            try_groupby_complex_query,
            &context.extension_with(left_path_entry),
        );
        let mut right_prepare = self.prepare_expression(
            right,
            try_groupby_complex_query,
            &context.extension_with(right_path_entry),
        );
        if left_prepare.fail_groupby_complex_query || right_prepare.fail_groupby_complex_query {
            return EXPrepReturn::fail_groupby_complex_query();
        }
        left_prepare.with_time_series_queries_from(&mut right_prepare);
        left_prepare
    }
}
