use super::Translator;
use crate::ast::{Connective, ElementConstraint, PathElement, PathElementOrConnective};
use crate::translator::triples_template::TemplateType;
use oxrdf::{NamedNode, Variable};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

impl Translator {
    pub fn translate_path(
        &mut self,
        path_identifier: &mut Vec<String>,
        variable_path_so_far: &mut Vec<Variable>,
        optional_index: Option<usize>,
        path_elements: Vec<&PathElementOrConnective>,
    ) {
        let start_index;
        let first_variable;
        if !variable_path_so_far.is_empty() {
            let first = variable_path_so_far.last().unwrap();
            assert!(path_elements.len() >= 2);
            start_index = 0;
            first_variable = first.clone();
        } else {
            assert!(path_elements.len() >= 3);
            if let PathElementOrConnective::PathElement(pe) = path_elements.get(0).unwrap() {
                first_variable = self
                    .add_path_element(path_identifier, optional_index, pe)
                    .clone();
                start_index = 1;
                variable_path_so_far.push(first_variable.clone());
            } else {
                panic!("Found unexpected connective");
            }
        }

        let first_elem = *path_elements.get(start_index).unwrap();
        let second_elem = *path_elements.get(start_index + 1).unwrap();
        if let PathElementOrConnective::Connective(c) = first_elem {
            if let PathElementOrConnective::PathElement(pe) = second_elem {
                let connective_named_node = self.translate_connective_named_node(c);
                let last_variable = self
                    .add_path_element(path_identifier, optional_index, pe)
                    .clone();
                variable_path_so_far.push(last_variable.clone());
                self.has_outgoing.insert(first_variable.clone());
                let triple_pattern = TriplePattern {
                    subject: TermPattern::Variable(first_variable.clone()),
                    predicate: NamedNodePattern::NamedNode(connective_named_node),
                    object: TermPattern::Variable(last_variable),
                };
                self.add_triple_pattern(triple_pattern, optional_index);
                path_identifier.push(format!("__{}__", c.to_variable_name_part()));
                if path_elements.len() > start_index + 2 {
                    self.translate_path(
                        path_identifier,
                        variable_path_so_far,
                        optional_index,
                        path_elements[start_index + 2..path_elements.len()].to_vec(),
                    )
                } else {
                    //Finished
                }
            } else {
                panic!("Bad path sequence")
            }
        } else {
            panic!("Bad path sequence")
        }
    }

    pub fn add_path_element(
        &mut self,
        path_identifier: &mut Vec<String>,
        optional_index: Option<usize>,
        path_element: &PathElement,
    ) -> Variable {
        let variable;
        if let Some(glue) = &path_element.glue {
            path_identifier.clear();
            path_identifier.push(path_element.glue.as_ref().unwrap().id.clone());

            if let Some(glue_var) = self.glue_variables.iter().find(|x| x.as_str() == &glue.id) {
                variable = glue_var.clone();
            } else {
                variable = self.create_and_add_variable(&path_identifier.join(""));
                self.glue_variables.push(variable.clone())
            }
        } else if let Some(element) = &path_element.element {
            match element {
                ElementConstraint::Name(n) => {
                    path_identifier.push(format!("_{}_", n));
                }
                ElementConstraint::TypeName(tn) => {
                    path_identifier.push(tn.to_string());
                }
                ElementConstraint::TypeNameAndName(tn, n) => {
                    path_identifier.push(tn.to_string());
                    path_identifier.push(format!("_{}_", n));
                }
            }
            variable = self.create_and_add_variable(&path_identifier.join(""));
        } else {
            panic!("Either element or glue must be set")
        }

        if let Some(element) = &path_element.element {
            self.add_element_constraint_to_variable(optional_index, element, &variable);
        }
        variable
    }

    pub fn create_and_add_variable(&mut self, variable_name: &str) -> Variable {
        let variable = Variable::new(variable_name)
            .expect(&format!("Invalid variable name: {}", variable_name));

        self.variables.push(variable);
        self.variables
            .get(self.variables.len() - 1)
            .unwrap()
            .clone()
    }

    pub fn add_element_constraint_to_variable(
        &mut self,
        optional_index: Option<usize>,
        ec: &ElementConstraint,
        variable: &Variable,
    ) {
        match ec {
            ElementConstraint::Name(n) => {
                let name_triples =
                    self.fill_triples_template(TemplateType::NameTemplate, Some(n), None, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
            }
            ElementConstraint::TypeName(tn) => {
                let type_name_triples = self.fill_triples_template(
                    TemplateType::TypeTemplate,
                    Some(tn),
                    None,
                    variable,
                );
                for type_name_triple in type_name_triples {
                    self.add_triple_pattern(type_name_triple, optional_index);
                }
            }
            ElementConstraint::TypeNameAndName(tn, n) => {
                let name_triples =
                    self.fill_triples_template(TemplateType::NameTemplate, Some(n), None, variable);
                for name_triple in name_triples {
                    self.add_triple_pattern(name_triple, optional_index);
                }
                let type_name_triples = self.fill_triples_template(
                    TemplateType::TypeTemplate,
                    Some(tn),
                    None,
                    variable,
                );
                for type_name_triple in type_name_triples {
                    self.add_triple_pattern(type_name_triple, optional_index);
                }
            }
        }
    }

    pub fn translate_connective_named_node(&self, connective: &Connective) -> NamedNode {
        let connective_string = connective.to_string();
        let iri = self
            .connective_mapping
            .map
            .get(&connective_string)
            .expect(&format!("Connective {} not defined", &connective_string));
        NamedNode::new(iri).expect("Invalid iri")
    }
}
