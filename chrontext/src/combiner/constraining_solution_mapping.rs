use std::collections::{HashMap, HashSet};
use oxrdf::{NamedNode, Variable};
use polars::prelude::LazyFrame;
use polars_core::datatypes::AnyValue;
use polars_core::prelude::{DataType, JoinType, NamedFrom};
use polars_core::series::Series;

pub struct ConstrainingSolutionMapping {
    pub solution_mapping: LazyFrame,
    pub columns: HashSet<String>,
    pub datatypes: HashMap<Variable, NamedNode>
}

pub fn update_constraints(constraints:&mut Option<ConstrainingSolutionMapping>, lf:LazyFrame, columns:HashSet<String>, datatypes:HashMap<Variable, NamedNode>) -> ConstrainingSolutionMapping {
    if let Some(constraints) = constraints {
        let mut join_on = vec![];
        for c in &constraints.columns {
            if columns.contains(&c) {
                join_on.push(c.clone());
            }
        }
        let new_lf = constraints.solution_mapping.join(lf, on.as_slice(), on.as_slice(), JoinType::Inner).unwrap();
        let new_columns = constraints.columns.union(&columns).collect();
        ConstrainingSolutionMapping {solution_mapping:new_df, columns:new_columns, datatypes:new_datatypes}
    } else {
        ConstrainingSolutionMapping {solution_mapping:lf, columns, datatypes}
    }
}
