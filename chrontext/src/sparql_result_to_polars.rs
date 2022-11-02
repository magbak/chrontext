use std::collections::HashMap;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode, Term};
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
use polars::prelude::{DataFrame, LiteralValue, NamedFrom, Series, TimeUnit};
use sparesults::QuerySolution;
use spargebra::algebra::GraphPattern;
use spargebra::Query;
use std::str::FromStr;

pub(crate) fn create_static_query_result_df(
    static_query: &Query,
    static_query_solutions: Vec<QuerySolution>,
) -> (DataFrame, HashMap<String, NamedNode>) {
    let column_variables;
    if let Query::Select {
        dataset: _,
        pattern,
        base_iri: _,
    } = static_query
    {
        if let GraphPattern::Project { variables, .. } = pattern {
            column_variables = variables.clone();
        } else if let GraphPattern::Distinct { inner } = pattern {
            if let GraphPattern::Project { variables, .. } = inner.as_ref() {
                column_variables = variables.clone();
            } else {
                panic!("");
            }
        } else {
            panic!("");
        }
    } else {
        panic!("");
    }

    let mut series_vec = vec![];
    for c in &column_variables {
        let literal_values = static_query_solutions
            .iter()
            .map(|x| {
                if let Some(term) = x.get(c) {
                    sparql_term_to_polars_literal_value(term)
                } else {
                    LiteralValue::Null
                }
            })
            .collect();
        let series = polars_literal_values_to_series(literal_values, c.as_str());
        series_vec.push(series);
    }
    let df = DataFrame::new(series_vec).expect("Create df problem");
    df
}

pub(crate) fn sparql_term_to_polars_literal_value(term: &Term) -> polars::prelude::LiteralValue {
    match term {
        Term::NamedNode(named_node) => sparql_named_node_to_polars_literal_value(named_node),
        Term::Literal(lit) => sparql_literal_to_polars_literal_value(lit),
        _ => {
            panic!("Not supported")
        }
    }
}

pub(crate) fn sparql_named_node_to_polars_literal_value(named_node: &NamedNode) -> LiteralValue {
    LiteralValue::Utf8(named_node.as_str().to_string())
}

pub(crate) fn sparql_literal_to_polars_literal_value(lit: &Literal) -> LiteralValue {
    let datatype = lit.datatype();
    let value = lit.value();
    let literal_value = if datatype == xsd::STRING {
        LiteralValue::Utf8(value.to_string())
    } else if datatype == xsd::UNSIGNED_INT {
        let u = u32::from_str(value).expect("Integer parsing error");
        LiteralValue::UInt32(u)
    } else if datatype == xsd::UNSIGNED_LONG {
        let u = u64::from_str(value).expect("Integer parsing error");
        LiteralValue::UInt64(u)
    } else if datatype == xsd::INTEGER {
        let i = i64::from_str(value).expect("Integer parsing error");
        LiteralValue::Int64(i)
    } else if datatype == xsd::LONG {
        let i = i64::from_str(value).expect("Integer parsing error");
        LiteralValue::Int64(i)
    } else if datatype == xsd::INT {
        let i = i32::from_str(value).expect("Integer parsing error");
        LiteralValue::Int32(i)
    } else if datatype == xsd::DOUBLE {
        let d = f64::from_str(value).expect("Integer parsing error");
        LiteralValue::Float64(d)
    } else if datatype == xsd::FLOAT {
        let f = f32::from_str(value).expect("Integer parsing error");
        LiteralValue::Float32(f)
    } else if datatype == xsd::BOOLEAN {
        let b = bool::from_str(value).expect("Boolean parsing error");
        LiteralValue::Boolean(b)
    } else if datatype == xsd::DATE_TIME {
        let dt_without_tz = value.parse::<NaiveDateTime>();
        if let Ok(dt) = dt_without_tz {
            LiteralValue::DateTime(dt, TimeUnit::Nanoseconds)
        } else {
            let dt_without_tz = value.parse::<DateTime<Utc>>();
            if let Ok(dt) = dt_without_tz {
                LiteralValue::DateTime(dt.naive_utc(), TimeUnit::Nanoseconds)
            } else {
                panic!("Could not parse datetime: {}", value);
            }
        }
    } else if datatype == xsd::DECIMAL {
        let d = f64::from_str(value).expect("Decimal parsing error");
        LiteralValue::Float64(d)
    } else {
        todo!("Not implemented!")
    };
    literal_value
}

fn polars_literal_values_to_series(literal_values: Vec<LiteralValue>, name: &str) -> Series {
    let first_non_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null != x)
        .cloned();
    let first_null_opt = literal_values
        .iter()
        .find(|x| &&LiteralValue::Null == x)
        .cloned();
    if let (Some(first_non_null), None) = (&first_non_null_opt, &first_null_opt) {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Boolean(b) = x {
                            b
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<bool>>(),
            ),
            LiteralValue::Utf8(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Utf8(u) = x {
                            u
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<String>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt32(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<u32>>(),
            ),
            LiteralValue::UInt64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt64(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<u64>>(),
            ),
            LiteralValue::Int32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int32(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i32>>(),
            ),
            LiteralValue::Int64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int64(i) = x {
                            i
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<i64>>(),
            ),
            LiteralValue::Float32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float32(f) = x {
                            f
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<f32>>(),
            ),
            LiteralValue::Float64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float64(f) = x {
                            Some(f)
                        } else {
                            panic!("Not possible")
                        }
                    })
                    .collect::<Vec<Option<f64>>>(),
            ),
            LiteralValue::Range { .. } => {
                todo!()
            }
            LiteralValue::DateTime(_, t) =>
            //TODO: Assert time unit lik??
            {
                let s = Series::new(
                    name,
                    literal_values
                        .into_iter()
                        .map(|x| {
                            if let LiteralValue::DateTime(n, t_prime) = x {
                                assert_eq!(t, &t_prime);
                                n
                            } else {
                                panic!("Not possible")
                            }
                        })
                        .collect::<Vec<NaiveDateTime>>(),
                );
                s
            }
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    } else if let (Some(first_non_null), Some(_)) = (&first_non_null_opt, &first_null_opt) {
        match first_non_null {
            LiteralValue::Boolean(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Boolean(b) = x {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<bool>>>(),
            ),
            LiteralValue::Utf8(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Utf8(u) = x {
                            Some(u)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<String>>>(),
            ),
            LiteralValue::UInt32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt32(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u32>>>(),
            ),
            LiteralValue::UInt64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::UInt64(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<u64>>>(),
            ),
            LiteralValue::Int32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int32(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i32>>>(),
            ),
            LiteralValue::Int64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Int64(i) = x {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<i64>>>(),
            ),
            LiteralValue::Float32(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float32(f) = x {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<f32>>>(),
            ),
            LiteralValue::Float64(_) => Series::new(
                name,
                literal_values
                    .into_iter()
                    .map(|x| {
                        if let LiteralValue::Float64(f) = x {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<f64>>>(),
            ),
            LiteralValue::Range { .. } => {
                todo!()
            }
            LiteralValue::DateTime(_, t) =>
            //TODO: Assert time unit lik??
            {
                Series::new(
                    name,
                    literal_values
                        .into_iter()
                        .map(|x| {
                            if let LiteralValue::DateTime(n, t_prime) = x {
                                assert_eq!(t, &t_prime);
                                Some(n)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Option<NaiveDateTime>>>(),
                )
            }
            LiteralValue::Duration(_, _) => {
                todo!()
            }
            LiteralValue::Series(_) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    } else {
        Series::new(
            name,
            literal_values
                .iter()
                .map(|_| None)
                .collect::<Vec<Option<bool>>>(),
        )
    }
}
