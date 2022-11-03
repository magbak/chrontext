use std::collections::HashMap;
use oxrdf::{NamedNode, Variable};
use polars::prelude::DataFrame;
use polars_core::datatypes::AnyValue;
use polars_core::prelude::{DataType, JoinType, NamedFrom};
use polars_core::series::Series;

pub struct ConstrainingSolutionMapping {
    pub solution_mapping: DataFrame,
    pub datatypes: HashMap<Variable, NamedNode>
}

pub fn update_constraints(constraints:&mut Option<ConstrainingSolutionMapping>, mut df:DataFrame, datatypes:HashMap<Variable, NamedNode>) -> ConstrainingSolutionMapping {
    if let Some(constraints) = constraints {
        let mut join_on = vec![];
        let df_cols = df.get_column_names();
        for c in constraints.solution_mapping.get_column_names() {
            if df_cols.contains(&c) {
                join_on.push(c);
            }
        }
        let new_df;
        if join_on.is_empty() {
            join_on.push("dummy_column");
            let mapping_dummy_col = Series::new_empty("dummy_column", &DataType::Boolean).extend_constant(AnyValue::Boolean(true), constraints.solution_mapping.height()).unwrap();
            let df_dummy_col = Series::new_empty("dummy_column", &DataType::Boolean).extend_constant(AnyValue::Boolean(true), df.height()).unwrap();
            new_df = constraints.solution_mapping.with_column(mapping_dummy_col).unwrap().join(df.with_column(df_dummy_col).unwrap(), join_on.as_slice(), join_on.as_slice(), JoinType::Inner, None).unwrap().drop("dummy_column").unwrap();
        } else {
            new_df = constraints.solution_mapping.join(&df, on.as_slice(), on.as_slice(), JoinType::Inner, None).unwrap();
        }
        ConstrainingSolutionMapping {solution_mapping:new_df, datatypes:new_datatypes}
    } else {
        ConstrainingSolutionMapping {solution_mapping:df, datatypes}
    }
}
