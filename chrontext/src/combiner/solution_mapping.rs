use std::collections::{HashMap, HashSet};
use oxrdf::{NamedNode, Variable};
use polars::prelude::LazyFrame;
use polars_core::prelude::{JoinType, NamedFrom};
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