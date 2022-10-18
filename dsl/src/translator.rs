mod aggregation;
mod graph_pattern;
mod group;
mod path;
mod timestamp_conditions;
mod triples_template;

use crate::ast::TsQuery;
use crate::connective_mapping::ConnectiveMapping;
use crate::costants::{
    HAS_DATA_POINT, HAS_TIMESERIES, HAS_TIMESTAMP, HAS_VALUE, TIMESTAMP_VARIABLE_NAME,
};
use oxrdf::{NamedNode, Variable};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::{HashMap, HashSet};
use std::iter::zip;

#[derive(Debug)]
pub struct VariablePathExpression {
    pub(crate) variable: Variable,
    pub(crate) path_variable: Variable,
    pub(crate) path_to_variable_expression: Expression,
}

pub struct Translator {
    variables: Vec<Variable>,
    triples: Vec<TriplePattern>,
    conditions: Vec<Expression>,
    path_name_expressions: Vec<VariablePathExpression>,
    group_path_name_expressions: Vec<VariablePathExpression>,
    optional_triples: Vec<Vec<TriplePattern>>,
    optional_conditions: Vec<Option<Expression>>,
    optional_path_name_expressions: Vec<Option<VariablePathExpression>>,
    variable_has_path_name: HashMap<Variable, Variable>,
    variable_has_value: HashMap<Variable, Variable>,
    counter: u16,
    name_template: Vec<TriplePattern>,
    type_name_template: Vec<TriplePattern>,
    has_outgoing: HashSet<Variable>,
    is_lhs_terminal: HashSet<Variable>,
    connective_mapping: ConnectiveMapping,
    group_by: Vec<String>,
    glue_variables: Vec<Variable>,
}

impl Translator {
    pub fn new(
        name_template: Vec<TriplePattern>,
        type_name_template: Vec<TriplePattern>,
        connective_mapping: ConnectiveMapping,
    ) -> Translator {
        Translator {
            variables: vec![],
            triples: vec![],
            conditions: vec![],
            path_name_expressions: vec![],
            group_path_name_expressions: vec![],
            optional_triples: vec![],
            optional_conditions: vec![],
            optional_path_name_expressions: vec![],
            variable_has_path_name: Default::default(),
            variable_has_value: Default::default(),
            counter: 0,
            name_template,
            type_name_template,
            has_outgoing: Default::default(),
            is_lhs_terminal: Default::default(),
            connective_mapping,
            group_by: vec![],
            glue_variables: vec![],
        }
    }

    pub fn translate(&mut self, ts_query: &TsQuery) -> Query {
        if let Some(group) = &ts_query.group {
            self.group_by.extend(group.var_names.iter().cloned());
        }
        self.translate_graph_pattern(&ts_query.graph_pattern);
        let mut inner_gp = GraphPattern::Bgp {
            patterns: self.triples.drain(0..self.triples.len()).collect(),
        };
        let mut project_paths = vec![];
        let mut project_values = vec![];
        inner_gp = self.add_optional_parts(inner_gp, &mut project_paths, &mut project_values);
        self.add_timestamp_conditions(ts_query);
        inner_gp = self.add_conditions(inner_gp);
        inner_gp = self.add_paths_and_values(inner_gp, &mut project_paths, &mut project_values);
        inner_gp =
            self.add_aggregation(inner_gp, ts_query, &mut project_paths, &mut project_values);
        inner_gp = self.add_group(inner_gp, ts_query, &mut project_paths, &mut project_values);

        let mut all_projections = project_paths;
        let has_value = !project_values.is_empty();
        all_projections.append(&mut project_values);
        if has_value {
            all_projections.push(Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME));
        }

        let project = GraphPattern::Project {
            inner: Box::new(inner_gp),
            variables: all_projections,
        };

        Query::Select {
            dataset: None,
            pattern: project,
            base_iri: None,
        }
    }

    fn add_optional_parts(
        &mut self,
        mut inner_gp: GraphPattern,
        project_paths: &mut Vec<Variable>,
        project_values: &mut Vec<Variable>,
    ) -> GraphPattern {
        let optional_triples_drain = self.optional_triples.drain(0..self.optional_triples.len());
        let optional_path_name_expressions_drain = &self.optional_path_name_expressions;
        let optional_conditions_drain = self
            .optional_conditions
            .drain(0..self.optional_conditions.len());

        for (optional_pattern, (path_name_expression_opt, conditions_opt)) in zip(
            optional_triples_drain,
            zip(
                optional_path_name_expressions_drain,
                optional_conditions_drain,
            ),
        ) {
            let mut optional_gp = GraphPattern::Bgp {
                patterns: optional_pattern,
            };

            if let Some(condition) = conditions_opt {
                optional_gp = GraphPattern::Filter {
                    expr: condition,
                    inner: Box::new(optional_gp),
                }
            }

            if let Some(variable_path_expression) = path_name_expression_opt {
                if !self
                    .has_outgoing
                    .contains(&variable_path_expression.variable)
                {
                    optional_gp = GraphPattern::Extend {
                        inner: Box::new(optional_gp),
                        variable: variable_path_expression.path_variable.clone(),
                        expression: variable_path_expression.path_to_variable_expression.clone(),
                    };
                    project_paths.push(variable_path_expression.path_variable.clone());
                    let value_var = self
                        .variable_has_value
                        .get(&variable_path_expression.variable)
                        .expect("Optional variable path has value");
                    project_values.push(value_var.clone());
                }
            }

            inner_gp = GraphPattern::LeftJoin {
                left: Box::new(inner_gp),
                right: Box::new(optional_gp),
                expression: None,
            };
        }
        inner_gp
    }

    fn add_conditions(&mut self, mut inner_gp: GraphPattern) -> GraphPattern {
        if !self.conditions.is_empty() {
            let mut conjuction = self.conditions.remove(0);
            for c in self.conditions.drain(0..self.conditions.len()) {
                conjuction = Expression::And(Box::new(conjuction), Box::new(c));
            }
            inner_gp = GraphPattern::Filter {
                expr: conjuction,
                inner: Box::new(inner_gp),
            };
        }
        inner_gp
    }

    fn add_paths_and_values(
        &mut self,
        mut inner_gp: GraphPattern,
        project_paths: &mut Vec<Variable>,
        project_values: &mut Vec<Variable>,
    ) -> GraphPattern {
        for variable_path_expression in &self.path_name_expressions {
            if !self
                .has_outgoing
                .contains(&variable_path_expression.variable)
            {
                inner_gp = GraphPattern::Extend {
                    inner: Box::new(inner_gp),
                    variable: variable_path_expression.path_variable.clone(),
                    expression: variable_path_expression.path_to_variable_expression.clone(),
                };
                project_paths.push(variable_path_expression.path_variable.clone());
                let value_variable = self
                    .variable_has_value
                    .get(&variable_path_expression.variable)
                    .expect("Value variable associated with path");
                project_values.push(value_variable.clone());
            }
        }
        inner_gp
    }

    fn add_triple_pattern(&mut self, triple_pattern: TriplePattern, optional_index: Option<usize>) {
        if let Some(i) = optional_index {
            self.optional_triples
                .get_mut(i)
                .unwrap()
                .push(triple_pattern);
        } else {
            self.triples.push(triple_pattern);
        }
    }

    fn add_value_and_timeseries_variable(
        &mut self,
        optional_index: Option<usize>,
        end_variable: &Variable,
    ) -> Variable {
        let timeseries_variable =
            Variable::new_unchecked(format!("{}_timeseries", end_variable.as_str()));
        let has_timeseries_triple = TriplePattern {
            subject: TermPattern::Variable(end_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESERIES)),
            object: TermPattern::Variable(timeseries_variable.clone()),
        };
        let datapoint_variable =
            Variable::new_unchecked(format!("{}_datapoint", timeseries_variable.as_str()));
        let has_datapoint_triple = TriplePattern {
            subject: TermPattern::Variable(timeseries_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_DATA_POINT)),
            object: TermPattern::Variable(datapoint_variable.clone()),
        };

        let value_variable =
            Variable::new_unchecked(format!("{}_value", datapoint_variable.as_str()));
        let has_value_triple = TriplePattern {
            subject: TermPattern::Variable(datapoint_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_VALUE)),
            object: TermPattern::Variable(value_variable.clone()),
        };
        let timestamp_variable = Variable::new_unchecked(TIMESTAMP_VARIABLE_NAME);
        let has_timestamp_triple = TriplePattern {
            subject: TermPattern::Variable(datapoint_variable.clone()),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(HAS_TIMESTAMP)),
            object: TermPattern::Variable(timestamp_variable),
        };
        if let Some(i) = optional_index {
            let opt_triples = self.optional_triples.get_mut(i).unwrap();
            opt_triples.push(has_timeseries_triple);
            opt_triples.push(has_datapoint_triple);
            opt_triples.push(has_value_triple);
            opt_triples.push(has_timestamp_triple);
        } else {
            self.triples.push(has_timeseries_triple);
            self.triples.push(has_datapoint_triple);
            self.triples.push(has_value_triple);
            self.triples.push(has_timestamp_triple);
        }
        self.variable_has_value
            .insert(end_variable.clone(), value_variable.clone());
        value_variable
    }
}
