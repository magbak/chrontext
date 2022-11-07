use std::collections::{HashMap, HashSet};
use oxrdf::{NamedNode, Variable};
use polars::prelude::LazyFrame;

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