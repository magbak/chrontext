use super::Translator;
use crate::ast::TsQuery;
use crate::costants::{TIMESTAMP_VARIABLE_NAME, XSD_DATETIME_FORMAT};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, Variable};
use spargebra::algebra::Expression;

impl Translator {
    pub fn add_timestamp_conditions(&mut self, ts_query: &TsQuery) {
        let mut timestamp_conditions = vec![];
        if let Some(from_ts) = ts_query.from_datetime {
            let gteq = Expression::GreaterOrEqual(
                Box::new(Expression::Variable(Variable::new_unchecked(
                    TIMESTAMP_VARIABLE_NAME,
                ))),
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    format!("{}", from_ts.format(XSD_DATETIME_FORMAT)),
                    xsd::DATE_TIME,
                ))),
            );
            timestamp_conditions.push(gteq);
        }

        if let Some(to_ts) = ts_query.to_datetime {
            let gteq = Expression::LessOrEqual(
                Box::new(Expression::Variable(Variable::new_unchecked(
                    TIMESTAMP_VARIABLE_NAME,
                ))),
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    format!("{}", to_ts.format(XSD_DATETIME_FORMAT)),
                    xsd::DATE_TIME,
                ))),
            );
            timestamp_conditions.push(gteq);
        }
        self.conditions.append(&mut timestamp_conditions);
    }
}
