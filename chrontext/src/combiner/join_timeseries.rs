use crate::timeseries_query::TimeSeriesQuery;
use polars::prelude::{col, JoinType};
use polars::prelude::{DataFrame, IntoLazy, LazyFrame};
use std::collections::HashSet;

pub fn join_tsq(
    columns: &mut HashSet<String>,
    input_lf: LazyFrame,
    tsq: TimeSeriesQuery,
    df: DataFrame,
) -> LazyFrame {
    let mut join_on = vec![];
    for c in df.get_column_names() {
        if columns.contains(c) {
            join_on.push(col(c));
        } else {
            columns.insert(c.to_string());
        }
    }

    let to_drop: Vec<&str>;
    if let TimeSeriesQuery::Grouped(_) = &tsq {
        let groupby_col = tsq.get_groupby_column();
        to_drop = vec![groupby_col.unwrap().as_str()]
    } else {
        let id_vars = tsq.get_identifier_variables();
        for id_var in &id_vars {
            assert!(columns.contains(id_var.as_str()));
        }
        to_drop = id_vars.iter().map(|x| x.as_str()).collect();
    }
    let mut output_lf = input_lf.join(
        df.lazy(),
        join_on.as_slice(),
        join_on.as_slice(),
        JoinType::Inner,
    );

    output_lf = output_lf.drop_columns(to_drop.as_slice());
    for var_name in to_drop {
        columns.remove(var_name);
    }
    output_lf
}
