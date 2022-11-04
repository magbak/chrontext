use std::collections::{HashMap, HashSet};
use oxrdf::{NamedNode, Variable};
use polars::prelude::LazyFrame;
use polars_core::datatypes::AnyValue;
use polars_core::prelude::{DataType, JoinType, NamedFrom};
use polars_core::series::Series;

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub columns: HashSet<String>,
    pub datatypes: HashMap<Variable, NamedNode>
}

impl SolutionMappings {
    pub fn new(mappings: LazyFrame, columns:HashSet<String>, datatypes: HashMap<Variable, NamedNode>) -> SolutionMappings {
        SolutionMappings {
            mappings,
            columns,
            datatypes
        }
    }
}

pub fn update_solution_mappings(solution_mappings:&mut Option<SolutionMappings>, lf:LazyFrame, columns:HashSet<String>, datatypes:HashMap<Variable, NamedNode>) -> SolutionMappings {
    if let Some(constraints) = solution_mappings {
        let mut join_on = vec![];
        for c in &constraints.columns {
            if columns.contains(c) {
                join_on.push(c.clone());
            }
        }
        let new_lf = constraints.mappings.join(lf, join_on.as_slice(), join_on.as_slice(), JoinType::Inner);
        let new_columns = constraints.columns.union(&columns).collect();
        SolutionMappings { mappings:new_lf, columns:new_columns, datatypes:new_datatypes}
    } else {
        SolutionMappings { mappings:lf, columns, datatypes}
    }
}
