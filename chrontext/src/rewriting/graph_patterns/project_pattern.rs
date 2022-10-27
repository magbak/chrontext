use super::StaticQueryRewriter;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;

impl StaticQueryRewriter {
    pub fn rewrite_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,

        context: &Context,
    ) -> GPReturn {
        let mut inner_rewrite = self.rewrite_graph_pattern(
            inner,
            &context.extension_with(PathEntry::ProjectInner),
        );
        if inner_rewrite.graph_pattern.is_some() {
            let mut variables_rewrite = variables
                .iter()
                .map(|v| self.rewrite_variable(v, context))
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect::<Vec<Variable>>();
            let mut datatype_keys_sorted = inner_rewrite
                .datatypes_in_scope
                .keys()
                .collect::<Vec<&Variable>>();
            datatype_keys_sorted.sort_by_key(|v| v.to_string());
            for k in datatype_keys_sorted {
                let vs = inner_rewrite.datatypes_in_scope.get(k).unwrap();
                let mut vars = vs.iter().collect::<Vec<&Variable>>();
                //Sort to make rewrites deterministic
                vars.sort_by_key(|v| v.to_string());
                for v in vars {
                    variables_rewrite.push(v.clone());
                }
            }
            let mut id_keys_sorted = inner_rewrite
                .external_ids_in_scope
                .keys()
                .collect::<Vec<&Variable>>();
            id_keys_sorted.sort_by_key(|v| v.to_string());
            for k in id_keys_sorted {
                let vs = inner_rewrite.external_ids_in_scope.get(k).unwrap();
                let mut vars = vs.iter().collect::<Vec<&Variable>>();
                //Sort to make rewrites deterministic
                vars.sort_by_key(|v| v.to_string());
                for v in vars {
                    variables_rewrite.push(v.clone());
                }
            }
            let mut additional_projections_sorted = self
                .additional_projections
                .iter()
                .collect::<Vec<&Variable>>();
            additional_projections_sorted.sort_by_key(|x| x.to_string());
            for v in additional_projections_sorted {
                if !variables_rewrite.contains(v) {
                    variables_rewrite.push(v.clone());
                }
            }
            //Todo: redusere scope??
            if variables_rewrite.len() > 0 {
                let inner_graph_pattern = inner_rewrite.graph_pattern.take().unwrap();
                inner_rewrite.with_graph_pattern(GraphPattern::Project {
                    inner: Box::new(inner_graph_pattern),
                    variables: variables_rewrite,
                });
                return inner_rewrite;
            }
        }
        GPReturn::none()
    }
}
