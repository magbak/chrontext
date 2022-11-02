use crate::combiner::Combiner;
use crate::preparing::TimeSeriesQueryPrepper;
use crate::preprocessing::Preprocessor;
use crate::pushdown_setting::PushdownSetting;
use crate::rewriting::StaticQueryRewriter;
use crate::sparql_result_to_polars::create_static_query_result_df;
use crate::splitter::parse_sparql_select_query;
use crate::static_sparql::execute_sparql_query;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{BasicTimeSeriesQuery, TimeSeriesQuery};
use log::debug;
use oxrdf::vocab::xsd;
use oxrdf::Term;
use polars::frame::DataFrame;
use sparesults::QuerySolution;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum OrchestrationError {
    InconsistentDatatype(String, String, String),
}

impl Display for OrchestrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrchestrationError::InconsistentDatatype(s1, s2, s3) => {
                write!(
                    f,
                    "Inconsistent datatypes {} and {} for variable {}",
                    s1, s2, s3
                )
            }
        }
    }
}

impl Error for OrchestrationError {}

pub struct Engine {
    pushdown_settings: HashSet<PushdownSetting>,
    time_series_database: Box<dyn TimeSeriesQueryable>,
}

impl Engine {
    pub fn new(
        pushdown_settings: HashSet<PushdownSetting>,
        time_series_database: Box<dyn TimeSeriesQueryable>,
        endpoint: String,
    ) -> Engine {
        Engine {
            pushdown_settings,
            time_series_database,
        }
    }

    pub async fn execute_hybrid_query(
        &mut self,
        query: &str,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let parsed_query = parse_sparql_select_query(query)?;
        debug!("Parsed query: {:?}", &parsed_query);
        let mut preprocessor = Preprocessor::new();
        let (preprocessed_query, variable_constraints) = preprocessor.preprocess(&parsed_query);
        debug!("Constraints: {:?}", variable_constraints);
        let mut rewriter = StaticQueryRewriter::new(&variable_constraints);
        let (static_rewrite, mut basic_time_series_queries) =
            rewriter.rewrite_query(preprocessed_query).unwrap();
        debug!("Produced static rewrite: {}", static_rewrite);
        debug!(
            "Produced basic time series queries: {:?}",
            basic_time_series_queries
        );
        let static_query_solutions = execute_sparql_query(endpoint, &static_rewrite).await?;
        complete_basic_time_series_queries(
            &static_query_solutions,
            &mut basic_time_series_queries,
        )?;
        let static_result_df =
            create_static_query_result_df(&static_rewrite, static_query_solutions);
        let StaticQueryRewriter {
            rewritten_filters, ..
        } = rewriter;
        let mut prepper = TimeSeriesQueryPrepper::new(
            self.pushdown_settings.clone(),
            self.time_series_database
                .allow_compound_timeseries_queries(),
            basic_time_series_queries,
            static_result_df,
            rewritten_filters,
        );
        let time_series_queries = prepper.prepare(&parsed_query);
        let TimeSeriesQueryPrepper {
            static_result_df, ..
        } = prepper;
        debug!("Static result dataframe: {}", static_result_df);
        if static_result_df.height() == 0 {
            todo!("Empty static df not supported yet")
        } else {
            let mut time_series = self
                .execute_time_series_queries(time_series_queries)
                .await?;
            debug!("Time series: {:?}", time_series);
            let mut combiner = Combiner::new();
            let lazy_frame = combiner.combine_static_and_time_series_results(
                &parsed_query,
                static_result_df,
                &mut time_series,
            );
            Ok(lazy_frame.collect()?)
        }
    }

    async fn execute_time_series_queries(
        &mut self,
        time_series_queries: Vec<TimeSeriesQuery>,
    ) -> Result<Vec<(TimeSeriesQuery, DataFrame)>, Box<dyn Error>> {
        let mut out = vec![];
        for tsq in time_series_queries {
            let df_res = self.time_series_database.execute(&tsq).await;
            match df_res {
                Ok(df) => {
                    match tsq.validate(&df) {
                        Ok(_) => {}
                        Err(err) => return Err(Box::new(err)),
                    }
                    out.push((tsq, df))
                }
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }
}

pub(crate) fn complete_basic_time_series_queries(
    static_query_solutions: &Vec<QuerySolution>,
    basic_time_series_queries: &mut Vec<BasicTimeSeriesQuery>,
) -> Result<(), OrchestrationError> {
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
                            return Err(OrchestrationError::InconsistentDatatype(
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
