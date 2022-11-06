use std::collections::{HashMap, HashSet};
use oxrdf::Term;
use oxrdf::vocab::xsd;
use super::Combiner;
use crate::combiner::CombinerError;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use polars::prelude::{col, Expr, IntoLazy};
use polars_core::prelude::JoinType;
use sparesults::QuerySolution;
use crate::combiner::solution_mapping::SolutionMappings;
use crate::query_context::Context;

impl Combiner {
    pub async fn execute_attach_time_series_query(
        &mut self,
        tsq: &TimeSeriesQuery,
        mut solution_mappings: SolutionMappings,
    ) -> Result<SolutionMappings, CombinerError> {
        let ts_df = self
            .time_series_database
            .execute(tsq)
            .await
            .map_err(|x| CombinerError::TimeSeriesQueryError(x))?;
        tsq.validate(&ts_df).map_err(|x|CombinerError::TimeSeriesValidationError(x))?;
        //Todo derive datatypes
        let ts_lf = ts_df.lazy();
        let on: Vec<Expr>;
        if let Some(colname) = tsq.get_groupby_column() {
            on = vec![col(colname)]
        } else {
            on = tsq
                .get_identifier_variables()
                .iter()
                .map(|x| col(x.as_str()))
                .collect();
        }

        solution_mappings.mappings =
            solution_mappings.mappings
                .join(ts_lf, on.as_slice(), on.as_slice(), JoinType::Inner);
        return Ok(solution_mappings);
    }
}

pub(crate) fn split_time_series_queries(time_series_queries: &mut Option<HashMap<Context, Vec<TimeSeriesQuery>>>, context:&Context) -> Option<HashMap<Context, Vec<TimeSeriesQuery>>> {
    if let Some(tsqs) = time_series_queries {
        let mut split_keys = vec![];
        for k in tsqs.keys() {
            if k.path.iter().zip(&context.path).all(|(x, y)| x == y) {
                split_keys.push(k.clone())
            }
        }
        let mut new_map = HashMap::new();
        for k in split_keys {
            let tsq = tsqs.remove(&k).unwrap();
            new_map.insert(k,tsq);
        }
        Some(new_map)
    } else {
        None
    }
}


pub(crate) fn complete_basic_time_series_queries(
    static_query_solutions: &Vec<QuerySolution>,
    basic_time_series_queries: &mut Vec<BasicTimeSeriesQuery>,
) -> Result<(), CombinerError> {
    for basic_query in basic_time_series_queries {
        let mut ids = HashSet::new();
        for sqs in static_query_solutions {
            if let Some(Term::Literal(lit)) =
                sqs.get(basic_query.identifier_variable.as_ref().unwrap())
            {
                if lit.datatype() == xsd::STRING {
                    ids.insert(lit.value().to_string());
                } else {
                    todo!()
                }
            }
        }

        if let Some(datatype_var) = &basic_query.datatype_variable {
            for sqs in static_query_solutions {
                if let Some(Term::NamedNode(nn)) = sqs.get(datatype_var) {
                    if basic_query.datatype.is_none() {
                        basic_query.datatype = Some(nn.clone());
                    } else if let Some(dt) = &basic_query.datatype {
                        if dt.as_str() != nn.as_str() {
                            return Err(CombinerError::InconsistentDatatype(
                                nn.as_str().to_string(),
                                dt.as_str().to_string(),
                                basic_query
                                    .timeseries_variable
                                    .as_ref()
                                    .unwrap()
                                    .variable
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
        let mut ids_vec: Vec<String> = ids.into_iter().collect();
        ids_vec.sort();
        basic_query.ids = Some(ids_vec);
    }
    Ok(())
}
