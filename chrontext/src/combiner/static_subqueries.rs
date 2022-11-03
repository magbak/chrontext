use std::collections::HashMap;
use oxrdf::{NamedNode, Variable};
use polars_core::frame::DataFrame;
use spargebra::Query;
use crate::combiner::constraining_solution_mapping::ConstrainingSolutionMapping;
use super::Combiner;

impl Combiner {
    pub fn execute_static_query(&self, query:&Query, constraints:&Option<ConstrainingSolutionMapping>) -> (DataFrame, HashMap<Variable, NamedNode>) {
        todo!()
    }

}