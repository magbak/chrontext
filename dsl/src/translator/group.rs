use super::Translator;
use crate::ast::TsQuery;
use crate::costants::{NEST, TIMESTAMP_VARIABLE_NAME};
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern};
use std::collections::HashSet;

impl Translator {
    pub fn add_group(
        &self,
        mut inner_gp: GraphPattern,
        ts_query: &TsQuery,
        project_paths: &mut Vec<Variable>,
        project_values: &mut Vec<Variable>,
    ) -> GraphPattern {
        fn nest_column_aggregation(variable: Variable) -> AggregateExpression {
            AggregateExpression::Custom {
                name: NamedNode::new_unchecked(NEST),
                expr: Box::new(Expression::Variable(variable)),
                distinct: false,
            }
        }

        fn sample_column_aggregation(variable: Variable) -> AggregateExpression {
            AggregateExpression::Sample {
                expr: Box::new(Expression::Variable(variable)),
                distinct: false,
            }
        }
        if let Some(group) = &ts_query.group {
            assert!(!group.var_names.is_empty());
            let mut group_by = vec![];
            let mut new_projections = vec![];
            let mut grouping_values = HashSet::new();
            let mut grouping_paths = HashSet::new();

            for var_name in &group.var_names {
                let var = self
                    .glue_variables
                    .iter()
                    .find(|var| var.as_str() == var_name)
                    .unwrap();

                //These are the groupings that are NOT terminal, and so not projected by default
                for vp in &self.group_path_name_expressions {
                    if &vp.variable == var {
                        inner_gp = GraphPattern::Extend {
                            inner: Box::new(inner_gp),
                            variable: vp.path_variable.clone(),
                            expression: vp.path_to_variable_expression.clone(),
                        };
                        new_projections.push(vp.path_variable.clone());
                    }
                }
                group_by.push(self.variable_has_path_name.get(var).unwrap().clone());

                //Assuming that only one object may have a timeseries
                for vp in self.path_name_expressions.iter().chain(
                    self.optional_path_name_expressions
                        .iter()
                        .filter(|x| x.is_some())
                        .map(|x| x.as_ref().unwrap()),
                ) {
                    if &vp.variable == var {
                        grouping_paths.insert(&vp.path_variable);
                        if project_paths.contains(&vp.path_variable) {
                            project_paths.retain(|v| v != &vp.path_variable);
                            project_paths
                                .insert(grouping_paths.len() - 1, vp.path_variable.clone());
                        }

                        if let Some(value) = self.variable_has_value.get(var) {
                            grouping_values.insert(value);
                            project_values.retain(|v| v != value);
                            project_values.insert(grouping_values.len() - 1, value.clone());
                        }
                    }
                }
            }

            let mut aggregates = vec![];
            for pp in project_paths.iter() {
                if !grouping_paths.contains(pp) {
                    aggregates.push((pp.clone(), nest_column_aggregation(pp.clone())));
                } else {
                    aggregates.push((pp.clone(), sample_column_aggregation(pp.clone())));
                }
            }
            for pv in project_values.iter() {
                if !grouping_values.contains(pv) {
                    aggregates.push((pv.clone(), nest_column_aggregation(pv.clone())));
                } else {
                    aggregates.push((pv.clone(), sample_column_aggregation(pv.clone())));
                }
            }

            if project_values.len() > 0 {
                group_by.push(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
            }
            inner_gp = GraphPattern::Group {
                inner: Box::new(inner_gp),
                variables: group_by.clone(),
                aggregates,
            };
            for (i, np) in new_projections.into_iter().enumerate() {
                project_paths.insert(i, np);
            }
        }
        inner_gp
    }
}
