use crate::query_context::{Context};
use crate::static_sparql::execute_sparql_query;
use oxrdf::{Literal, NamedNode, Variable};
use spargebra::algebra::GraphPattern;
use spargebra::term::GroundTerm;
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use oxrdf::vocab::xsd;
use polars_core::datatypes::{AnyValue, DataType};
use polars_core::frame::DataFrame;
use polars_core::prelude::{JoinType, Series};
use polars_core::utils::concat_df;
use crate::rewriting::subqueries::{SubQuery, SubQueryInContext};
use crate::sparql_result_to_polars::create_static_query_result_df;

pub(crate) struct StaticExecutor{
    pub(crate) query_results: HashMap<Context, (DataFrame, HashMap<String,NamedNode>)>,
    pub(crate) query_map: HashMap<Context, Query>,
    pub(crate) subqueries_in_context: Vec<SubQueryInContext>,
    endpoint: String,
}

impl StaticExecutor {
    pub(crate) async fn execute_static_queries(&mut self) {
        for sqc in &self.subqueries_in_context {
            match &sqc.subquery {
                SubQuery::Filter(inner_context, expression_contexts) => {
                    let (inner_results, inner_datatypes) = self.get_query_results(inner_context).await;
                    for expr_ctx in expression_contexts {
                        self.constrain_with_results(inner_results, expr_ctx);
                        self.get_query_results(expr_ctx).await;
                    }
                    self.query_results.insert(sqc.context.clone(), inner_results.clone());
                }
                SubQuery::Group(inner_context) => {
                    let inner_results = self.get_query_results(inner_context).await;
                    self.query_results.insert(sqc.context.clone(), inner_results.clone());
                }
                SubQuery::Join(left_context, right_context) => {
                    let left_results = self.get_query_results(left_context).await;
                    self.constrain_with_results(left_results, right_context);
                    let right_results = self.get_query_results(left_context).await;
                    let joined_results = join_results(left_results, right_results);
                    self.query_results.insert(sqc.context.clone(), joined_results);
                }
                SubQuery::LeftJoin(left_context,right_context, expression_contexts) => {
                    let left_results = self.get_query_results(left_context).await;
                    self.constrain_with_results(left_results, right_context);
                    let right_results = self.get_query_results(left_context).await;
                    for expr_ctx in expression_contexts {
                        self.constrain_with_results(left_results, expr_ctx);
                        self.get_query_results(expr_ctx).await;
                    }
                    self.query_results.insert(sqc.context.clone(), left_results);
                }
                SubQuery::Minus(left_context, right_context) => {
                    let left_results = self.get_query_results(left_context).await;
                    self.constrain_with_results( left_results, right_context);
                    self.get_query_results(left_context).await;
                    self.query_results.insert(sqc.context.clone(), left_results.clone());
                }
                SubQuery::Union(left_context, right_context) => {
                    let left_results = self.get_query_results(left_context).await;
                    let right_results = self.get_query_results(left_context).await;
                    let joined_results = union_results(left_results, right_results);
                    self.query_results.insert(sqc.context.clone(), joined_results);
                }
            }
        }
    }

    async fn get_query_results(&mut self, context:&Context) -> &(DataFrame, HashMap<String, NamedNode>) {
        if !self.query_results.contains_key(context) {
            let query = self.query_map.get(context).unwrap();
            let solutions = execute_sparql_query(&self.endpoint, &query).await?;
            let (solution_df, datatype_map) = create_static_query_result_df(&query, solutions);
            self.query_results.insert(context.clone(), (solution_df, datatype_map));
        }
        return self.query_results.get(context).unwrap()
    }

    fn constrain_with_results(&mut self, constraining_results: &DataFrame, constraining_datatypes:HashMap<String, NamedNode>,  constrained_context: &Context) {
        let column_names = constraining_results.get_column_names();
        let query_to_constrain = self.query_map.get_mut(constrained_context).unwrap();
        let projected_variable_vec: Vec<&Variable> = get_variable_set(&query_to_constrain).into_iter().collect();

        let mut variables = vec![];
        let mut bindings = vec![];
        for v in projected_variable_vec {
            if column_names.contains(&v.as_str()) {
                variables.push(v.clone());
                bindings.push(create_variable_bindings(constraining_results.column(v.as_str()).unwrap(), constraining_datatypes.get(v.as_str().unwrap())))
            }
        }

        let values_pattern = GraphPattern::Values {
            variables,
            bindings
        };



    }

}

fn create_variable_bindings(ser: &Series, datatype:&NamedNode) -> Vec<Option<GroundTerm>> {
    let mut bindings = vec![];
    for any in ser.iter() {
        bindings.push(match any {
            AnyValue::Null => {None}
            AnyValue::Boolean(b) => {GroundTerm::Literal(Literal::from(b))}
            AnyValue::Utf8(u) => {GroundTerm::Literal(Literal::new_typed_literal(u, datatype))}
            AnyValue::UInt8(u) => {todo!()}
            AnyValue::UInt16(u) => {todo!()}
            AnyValue::UInt32(u) => {todo!()}
            AnyValue::UInt64(u) => {todo!()}
            AnyValue::Int8(i) => {todo!()}
            AnyValue::Int16(i) => {todo!()}
            AnyValue::Int32(i) => {todo!()}
            AnyValue::Int64(i) => {todo!()}
            AnyValue::Float32(f) => {todo!()}
            AnyValue::Float64(f) => {todo!()}
            AnyValue::Date(d) => {todo!()}
            AnyValue::Datetime(d, u, z) => {todo!()}
            AnyValue::Duration(d, u) => {todo!()}
            AnyValue::Time(t) => {todo!()}
            AnyValue::Categorical(c, r) => {todo!()}
            AnyValue::List(_) => {todo!()}
            AnyValue::Object(_) => {todo!()}
            AnyValue::Struct(_, _) => {todo!()}
            AnyValue::StructOwned(_) => {todo!()}
            AnyValue::Utf8Owned(_) => {todo!()}
        });
    }
    bindings
}

fn union_results(left: &DataFrame, right: &DataFrame) -> DataFrame {
    concat_df([left, right]).unwrap()
}

fn join_results(left: &DataFrame, right: &DataFrame) -> DataFrame {
    let mut on = vec![];
    let left_cols = left.get_column_names();
    let right_cols = right.get_column_names();
    for l in left_cols {
        if right_cols.contains(&l) {
            on.push(l)
        }
    }
    left.join(right, on.as_slice(), on.as_slice(), JoinType::Inner).unwrap()

}

fn get_variable_set(query: &Query) -> HashSet<&Variable> {
    if let GraphPattern::Project { inner, variables } = query {
        return variables.iter().collect();
    } else {
        panic!("Non project graph pattern in query")
    }
}
