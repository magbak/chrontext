use super::Translator;
use crate::ast::{
    BooleanOperator, GraphPathPattern, LiteralData, PathElementOrConnective, PathOrLiteralData,
};
use crate::costants::LIKE_FUNCTION;
use crate::translator::triples_template::TemplateType;
use crate::translator::VariablePathExpression;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Variable};
use spargebra::algebra::{Expression, Function};

pub enum VariableOrLiteral {
    Variable(Variable),
    Literal(Literal),
}

impl Translator {
    pub fn translate_graph_pattern(&mut self, gp: &GraphPathPattern) {
        let mut optional_counter = 0;
        for cp in &gp.conditioned_paths {
            let mut optional_index = None;
            if cp.lhs_path.optional {
                optional_index = Some(optional_counter);
            }
            let mut translated_lhs_variable_path = vec![];
            self.translate_path(
                &mut vec![],
                &mut translated_lhs_variable_path,
                optional_index,
                cp.lhs_path.path.iter().collect(),
            );
            let translated_lhs_value_variable = self.add_value_and_timeseries_variable(
                optional_index,
                translated_lhs_variable_path.last().unwrap(),
            );

            self.is_lhs_terminal
                .insert(translated_lhs_value_variable.clone());
            let connectives_path = cp
                .lhs_path
                .path
                .iter()
                .map(|p| match p {
                    PathElementOrConnective::PathElement(_) => None,
                    PathElementOrConnective::Connective(c) => Some(c.to_string()),
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect();
            self.create_name_path_variable(
                optional_index,
                translated_lhs_variable_path,
                connectives_path,
            );
            if let Some(op) = &cp.boolean_operator {
                if let Some(rhs_path_or_literal) = &cp.rhs_path_or_literal {
                    let translated_rhs_variable_or_literal = self.translate_path_or_literal(
                        &mut vec![],
                        optional_index,
                        rhs_path_or_literal,
                    );
                    let translated_rhs_value_variable_or_literal =
                        match translated_rhs_variable_or_literal {
                            VariableOrLiteral::Variable(rhs_end) => VariableOrLiteral::Variable(
                                self.add_value_and_timeseries_variable(optional_index, &rhs_end),
                            ),
                            VariableOrLiteral::Literal(l) => VariableOrLiteral::Literal(l),
                        };
                    self.add_condition(
                        optional_index,
                        &translated_lhs_value_variable,
                        op,
                        translated_rhs_value_variable_or_literal,
                    );
                }
            }
            if cp.lhs_path.optional {
                optional_counter += 1
            }
        }
    }

    pub fn translate_path_or_literal(
        &mut self,
        path_identifier: &mut Vec<String>,
        optional_index: Option<usize>,
        path_or_literal: &PathOrLiteralData,
    ) -> VariableOrLiteral {
        match path_or_literal {
            PathOrLiteralData::Path(p) => {
                assert!(!p.optional);
                //optional from lhs of condition always dominates, we do not expect p.optional to be set.
                let mut translated_path = vec![];
                self.translate_path(
                    path_identifier,
                    &mut translated_path,
                    optional_index,
                    p.path.iter().collect(),
                );
                VariableOrLiteral::Variable(translated_path.last().unwrap().clone())
            }
            PathOrLiteralData::Literal(l) => {
                let literal = match l {
                    LiteralData::Real(r) => Literal::new_typed_literal(r.to_string(), xsd::DOUBLE),
                    LiteralData::Integer(i) => {
                        Literal::new_typed_literal(i.to_string(), xsd::INTEGER)
                    }
                    LiteralData::String(s) => {
                        Literal::new_typed_literal(s.to_string(), xsd::STRING)
                    }
                    LiteralData::Boolean(b) => {
                        Literal::new_typed_literal(b.to_string(), xsd::BOOLEAN)
                    }
                };
                VariableOrLiteral::Literal(literal)
            }
        }
    }

    pub fn create_name_path_variable(
        &mut self,
        optional_index: Option<usize>,
        variables_path: Vec<Variable>,
        connectives_path: Vec<String>,
    ) {
        let mut variable_names_path = vec![];
        let mut glue_names_path = vec![];
        for (i, v) in variables_path.iter().enumerate() {
            let vname = Variable::new_unchecked(format!("{}_name_on_path", v.as_str()));
            variable_names_path.push(vname.clone());
            let triples =
                self.fill_triples_template(TemplateType::NameTemplate, None, Some(&vname), v);
            for t in triples {
                self.add_triple_pattern(t, optional_index);
            }
            if !self.variable_has_path_name.contains_key(v)
                && self.group_by.contains(&v.as_str().to_string())
                && i < variables_path.len() - 1
            {
                glue_names_path.push((v.clone(), variables_path.clone()));
            }
        }
        fn build_concat_path_expression(
            variable_names_path: Vec<Variable>,
            connectives_path: Vec<&String>,
        ) -> Expression {
            assert_eq!(variable_names_path.len(), connectives_path.len() + 1);
            let mut args_vec = vec![];
            for (vp, cc) in variable_names_path.iter().zip(connectives_path) {
                args_vec.push(Expression::Variable(vp.clone()));
                args_vec.push(Expression::Literal(Literal::new_typed_literal(
                    cc.clone(),
                    xsd::STRING,
                )));
            }
            //We must push the final variable since the connectives path has one less element.
            args_vec.push(Expression::Variable(
                variable_names_path.last().unwrap().clone(),
            ));

            Expression::FunctionCall(Function::Concat, args_vec)
        }

        let path_string_expression =
            build_concat_path_expression(variable_names_path, connectives_path.iter().collect());

        let last_variable = variables_path.last().unwrap().clone();
        let path_variable =
            Variable::new_unchecked(format!("{}_path_name", last_variable.as_str()));
        self.variable_has_path_name
            .insert(last_variable.clone(), path_variable.clone());
        let expr = VariablePathExpression {
            variable: last_variable.clone(),
            path_variable: path_variable.clone(),
            path_to_variable_expression: path_string_expression,
        };
        if let Some(_) = optional_index {
            self.optional_path_name_expressions.push(Some(expr));
        } else {
            self.path_name_expressions.push(expr);
            self.optional_path_name_expressions.push(None);
        }

        //This logic is only for grouping on nonterminals
        for (g, path) in glue_names_path {
            let path_len = path.len();
            let path_string_expression = build_concat_path_expression(
                path,
                connectives_path[0..(path_len - 1)].iter().collect(),
            );
            let path_variable = Variable::new_unchecked(format!("{}_path_name", g.as_str()));
            let expr = VariablePathExpression {
                variable: g.clone(),
                path_variable: path_variable.clone(),
                path_to_variable_expression: path_string_expression,
            };
            self.variable_has_path_name
                .insert(g.clone(), path_variable.clone());
            self.group_path_name_expressions.push(expr);
        }
    }

    pub fn add_condition(
        &mut self,
        optional_index: Option<usize>,
        lhs_variable: &Variable,
        op: &BooleanOperator,
        rhs_variable_or_literal: VariableOrLiteral,
    ) {
        let lhs_expression = Expression::Variable(lhs_variable.clone());
        let rhs_expression = match rhs_variable_or_literal {
            VariableOrLiteral::Variable(v) => Expression::Variable(v),
            VariableOrLiteral::Literal(l) => Expression::Literal(l),
        };
        let mapped_expression = match op {
            BooleanOperator::NEQ => Expression::Not(Box::new(Expression::Equal(
                Box::new(lhs_expression),
                Box::new(rhs_expression),
            ))),
            BooleanOperator::EQ => {
                Expression::Equal(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::LTEQ => {
                Expression::LessOrEqual(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::GTEQ => {
                Expression::GreaterOrEqual(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::LT => {
                Expression::Less(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::GT => {
                Expression::Greater(Box::new(lhs_expression), Box::new(rhs_expression))
            }
            BooleanOperator::LIKE => Expression::FunctionCall(
                Function::Custom(NamedNode::new_unchecked(LIKE_FUNCTION)),
                vec![rhs_expression],
            ),
        };

        if let Some(_) = optional_index {
            self.optional_conditions.push(Some(mapped_expression));
        } else {
            self.optional_conditions.push(None);
            self.conditions.push(mapped_expression);
        }
    }
}
