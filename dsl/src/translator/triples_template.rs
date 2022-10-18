use super::Translator;
use crate::costants::{REPLACE_STR_LITERAL, REPLACE_VARIABLE_NAME};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, Variable};
use spargebra::term::{TermPattern, TriplePattern};
use std::collections::HashMap;

pub enum TemplateType {
    TypeTemplate,
    NameTemplate,
}

impl Translator {
    pub fn fill_triples_template(
        &mut self,
        template_type: TemplateType,
        replace_str: Option<&str>,
        replace_str_variable: Option<&Variable>,
        replace_variable: &Variable,
    ) -> Vec<TriplePattern> {
        let template = match template_type {
            TemplateType::TypeTemplate => &self.type_name_template,
            TemplateType::NameTemplate => &self.name_template,
        };
        let mut map = HashMap::new();
        let mut triples = vec![];
        for t in template {
            let subject_term_pattern;
            if let TermPattern::Variable(subject_variable) = &t.subject {
                if !map.contains_key(subject_variable) {
                    let use_subject_variable;
                    if REPLACE_VARIABLE_NAME == subject_variable.as_str() {
                        use_subject_variable = replace_variable.clone();
                    } else {
                        use_subject_variable = Variable::new_unchecked(format!(
                            "{}_{}",
                            subject_variable.as_str().to_string(),
                            self.counter
                        ));
                        self.counter += 1;
                    }
                    subject_term_pattern = TermPattern::Variable(use_subject_variable);
                    map.insert(subject_variable, subject_term_pattern.clone());
                } else {
                    subject_term_pattern = map.get(subject_variable).unwrap().clone();
                }
            } else {
                subject_term_pattern = t.subject.clone();
            }
            let object_term_pattern;
            if let TermPattern::Variable(object_variable) = &t.object {
                if !map.contains_key(object_variable) {
                    let use_object_variable;
                    if REPLACE_VARIABLE_NAME == object_variable.as_str() {
                        use_object_variable = replace_variable.clone();
                    } else {
                        use_object_variable = Variable::new_unchecked(format!(
                            "{}_{}",
                            object_variable.as_str().to_string(),
                            self.counter
                        ));
                        self.counter += 1;
                    }
                    object_term_pattern = TermPattern::Variable(use_object_variable);
                    map.insert(object_variable, object_term_pattern.clone());
                } else {
                    object_term_pattern = map.get(object_variable).unwrap().clone();
                }
            } else if let TermPattern::Literal(lit) = &t.object {
                if lit.datatype() == xsd::STRING && lit.value() == REPLACE_STR_LITERAL {
                    if let Some(replace_str) = replace_str {
                        object_term_pattern = TermPattern::Literal(Literal::new_typed_literal(
                            replace_str,
                            xsd::STRING,
                        ));
                    } else if let Some(replace_str_variable) = replace_str_variable {
                        object_term_pattern = TermPattern::Variable(replace_str_variable.clone())
                    } else {
                        panic!("Should never happen");
                    }
                } else {
                    object_term_pattern = TermPattern::Literal(lit.clone());
                }
            } else {
                object_term_pattern = t.object.clone();
            }
            triples.push(TriplePattern {
                subject: subject_term_pattern,
                predicate: t.predicate.clone(),
                object: object_term_pattern,
            })
        }
        triples
    }
}
