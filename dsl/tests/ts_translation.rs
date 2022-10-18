use dsl::ast::{Connective, ConnectiveType};
use dsl::connective_mapping::ConnectiveMapping;
use dsl::costants::{REPLACE_STR_LITERAL, REPLACE_VARIABLE_NAME};
use dsl::parser::ts_query;
use dsl::translator::Translator;
use log::debug;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{Literal, NamedNode, Variable};
use rstest::*;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;
use std::collections::HashMap;

#[fixture]
fn use_logger() {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize")
        }
    }
}

#[fixture]
fn type_name_template() -> Vec<TriplePattern> {
    let type_variable = Variable::new_unchecked("type_var");
    let type_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::from(rdf::TYPE)),
        object: TermPattern::Variable(type_variable.clone()),
    };
    let type_name_triple = TriplePattern {
        subject: TermPattern::Variable(type_variable),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
            "http://example.org/types#hasName",
        )),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING)),
    };
    vec![type_triple, type_name_triple]
}

#[fixture]
fn name_template() -> Vec<TriplePattern> {
    let name_triple = TriplePattern {
        subject: TermPattern::Variable(Variable::new_unchecked(REPLACE_VARIABLE_NAME)),
        predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
            "http://example.org/types#hasName",
        )),
        object: TermPattern::Literal(Literal::new_typed_literal(REPLACE_STR_LITERAL, xsd::STRING)),
    };
    vec![name_triple]
}

#[fixture]
fn connective_mapping() -> ConnectiveMapping {
    let map = HashMap::from([
        (
            Connective::new(ConnectiveType::Period, 1).to_string(),
            "http://example.org/types#hasOnePeriodRelation".to_string(),
        ),
        (
            Connective::new(ConnectiveType::Period, 2).to_string(),
            "http://example.org/types#hasTwoPeriodRelation".to_string(),
        ),
        (
            Connective::new(ConnectiveType::Dash, 1).to_string(),
            "http://example.org/types#hasOneDashRelation".to_string(),
        ),
    ]);

    ConnectiveMapping { map }
}

#[fixture]
fn translator(
    name_template: Vec<TriplePattern>,
    type_name_template: Vec<TriplePattern>,
    connective_mapping: ConnectiveMapping,
) -> Translator {
    Translator::new(name_template, type_name_template, connective_mapping)
}

#[rstest]
fn test_easy_translation(mut translator: Translator) {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
"#;
    let (_, tsq) = ts_query(q).expect("No problemo");
    let mut actual = translator.translate(&tsq);
    actual = Query::parse(&actual.to_string(), None).expect("Parse myself");
    //println!("{}", actual);
    //println!("{:?}", actual);
    let expected_query_str = r#"
  PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
  PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value ?valve_PosPct___Period___mag__timeseries_datapoint_value ?timestamp WHERE {
  ?ABC rdf:type ?type_var_0.
  ?type_var_0 <http://example.org/types#hasName> "ABC".
  ?valve <http://example.org/types#hasName> "HLV".
  ?ABC <http://example.org/types#hasOneDashRelation> ?valve.
  ?valve__Dash___Mvm_ <http://example.org/types#hasName> "Mvm".
  ?valve <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm_.
  ?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> "stVal".
  ?valve__Dash___Mvm_ <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm___Period___stVal_.
  ?valve__Dash___Mvm___Period___stVal_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve__Dash___Mvm___Period___stVal__timeseries.
  ?valve__Dash___Mvm___Period___stVal__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint.
  ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value.
  ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp.
  ?ABC <http://example.org/types#hasName> ?ABC_name_on_path.
  ?valve <http://example.org/types#hasName> ?valve_name_on_path.
  ?valve__Dash___Mvm_ <http://example.org/types#hasName> ?valve__Dash___Mvm__name_on_path.
  ?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> ?valve__Dash___Mvm___Period___stVal__name_on_path.
  ?valve_PosPct_ <http://example.org/types#hasName> "PosPct".
  ?valve <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct_.
  ?valve_PosPct___Period___mag_ <http://example.org/types#hasName> "mag".
  ?valve_PosPct_ <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct___Period___mag_.
  ?valve_PosPct___Period___mag_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve_PosPct___Period___mag__timeseries.
  ?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve_PosPct___Period___mag__timeseries_datapoint.
  ?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve_PosPct___Period___mag__timeseries_datapoint_value.
  ?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp.
  ?valve <http://example.org/types#hasName> ?valve_name_on_path.
  ?valve_PosPct_ <http://example.org/types#hasName> ?valve_PosPct__name_on_path.
  ?valve_PosPct___Period___mag_ <http://example.org/types#hasName> ?valve_PosPct___Period___mag__name_on_path.
  BIND(CONCAT(?ABC_name_on_path, "-", ?valve_name_on_path, ".", ?valve__Dash___Mvm__name_on_path, ".", ?valve__Dash___Mvm___Period___stVal__name_on_path) AS ?valve__Dash___Mvm___Period___stVal__path_name)
  BIND(CONCAT(?valve_name_on_path, ".", ?valve_PosPct__name_on_path, ".", ?valve_PosPct___Period___mag__name_on_path) AS ?valve_PosPct___Period___mag__path_name)
  FILTER((?timestamp >= "2021-11-30T23:00:01+00:00"^^xsd:dateTime) && (?timestamp <= "2021-12-01T23:00:01+00:00"^^xsd:dateTime))
}"#;
    let expected_query = Query::parse(expected_query_str, None).expect("Parse expected error");
    assert_eq!(expected_query, actual);
}

#[rstest]
fn test_aggregation_translation(mut translator: Translator) {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    aggregate mean 10min
"#;
    let (_, tsq) = ts_query(q).expect("No problemo");
    let actual = translator.translate(&tsq);
    //println!("{}", actual);
    let actual_str = actual.to_string();
    //TODO: Workaround for generated variable names in query.. perhaps rename them..
    let expected_query_str = r#"SELECT ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value ?valve_PosPct___Period___mag__timeseries_datapoint_value ?timestamp WHERE { {SELECT (AVG(?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) AS ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) (AVG(?valve_PosPct___Period___mag__timeseries_datapoint_value) AS ?valve_PosPct___Period___mag__timeseries_datapoint_value) ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?timestamp_grouping WHERE { ?ABC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type_var_0 .?type_var_0 <http://example.org/types#hasName> "ABC" .?valve <http://example.org/types#hasName> "HLV" .?ABC <http://example.org/types#hasOneDashRelation> ?valve .?valve__Dash___Mvm_ <http://example.org/types#hasName> "Mvm" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm_ .?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> "stVal" .?valve__Dash___Mvm_ <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm___Period___stVal_ .?valve__Dash___Mvm___Period___stVal_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve__Dash___Mvm___Period___stVal__timeseries .?valve__Dash___Mvm___Period___stVal__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint .?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value .?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?ABC <http://example.org/types#hasName> ?ABC_name_on_path .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve__Dash___Mvm_ <http://example.org/types#hasName> ?valve__Dash___Mvm__name_on_path .?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> ?valve__Dash___Mvm___Period___stVal__name_on_path .?valve_PosPct_ <http://example.org/types#hasName> "PosPct" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct_ .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> "mag" .?valve_PosPct_ <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct___Period___mag_ .?valve_PosPct___Period___mag_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve_PosPct___Period___mag__timeseries .?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve_PosPct___Period___mag__timeseries_datapoint .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve_PosPct___Period___mag__timeseries_datapoint_value .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve_PosPct_ <http://example.org/types#hasName> ?valve_PosPct__name_on_path .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> ?valve_PosPct___Period___mag__name_on_path . FILTER(((?timestamp >= "2021-11-30T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>) && (?timestamp <= "2021-12-01T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>))) BIND(CONCAT(?ABC_name_on_path, "-", ?valve_name_on_path, ".", ?valve__Dash___Mvm__name_on_path, ".", ?valve__Dash___Mvm___Period___stVal__name_on_path) AS ?valve__Dash___Mvm___Period___stVal__path_name) BIND(CONCAT(?valve_name_on_path, ".", ?valve_PosPct__name_on_path, ".", ?valve_PosPct___Period___mag__name_on_path) AS ?valve_PosPct___Period___mag__path_name) BIND(FLOOR(<https://github.com/magbak/chrontext#DateTimeAsSeconds>(?timestamp) / "600"^^<http://www.w3.org/2001/XMLSchema#unsignedLong>) * "600"^^<http://www.w3.org/2001/XMLSchema#unsignedLong> AS ?timestamp_grouping) } GROUP BY ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?timestamp_grouping} BIND(<https://github.com/magbak/chrontext#SecondsAsDateTime>(?timestamp_grouping) AS ?timestamp) }"#.to_string();
    assert_eq!(expected_query_str, actual_str);
}

#[rstest]
fn test_aggregation_and_group_translation(mut translator: Translator) {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    group valve
    aggregate mean 10min
"#;
    let (_, tsq) = ts_query(q).expect("No problemo");
    let actual = translator.translate(&tsq);
    //println!("{}", actual);
    let actual_str = actual.to_string();
    //TODO: Workaround for generated variable names in query.. perhaps rename them..
    let expected_query_str = r#"SELECT ?valve_path_name ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value ?valve_PosPct___Period___mag__timeseries_datapoint_value ?timestamp WHERE { {SELECT (<https://github.com/magbak/chrontext#nestAggregation>(?valve__Dash___Mvm___Period___stVal__path_name) AS ?valve__Dash___Mvm___Period___stVal__path_name) (<https://github.com/magbak/chrontext#nestAggregation>(?valve_PosPct___Period___mag__path_name) AS ?valve_PosPct___Period___mag__path_name) (<https://github.com/magbak/chrontext#nestAggregation>(?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) AS ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) (<https://github.com/magbak/chrontext#nestAggregation>(?valve_PosPct___Period___mag__timeseries_datapoint_value) AS ?valve_PosPct___Period___mag__timeseries_datapoint_value) ?valve_path_name ?timestamp WHERE { {SELECT (AVG(?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) AS ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) (AVG(?valve_PosPct___Period___mag__timeseries_datapoint_value) AS ?valve_PosPct___Period___mag__timeseries_datapoint_value) ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?timestamp_grouping WHERE { ?ABC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type_var_0 .?type_var_0 <http://example.org/types#hasName> "ABC" .?valve <http://example.org/types#hasName> "HLV" .?ABC <http://example.org/types#hasOneDashRelation> ?valve .?valve__Dash___Mvm_ <http://example.org/types#hasName> "Mvm" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm_ .?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> "stVal" .?valve__Dash___Mvm_ <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm___Period___stVal_ .?valve__Dash___Mvm___Period___stVal_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve__Dash___Mvm___Period___stVal__timeseries .?valve__Dash___Mvm___Period___stVal__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint .?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value .?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?ABC <http://example.org/types#hasName> ?ABC_name_on_path .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve__Dash___Mvm_ <http://example.org/types#hasName> ?valve__Dash___Mvm__name_on_path .?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> ?valve__Dash___Mvm___Period___stVal__name_on_path .?valve_PosPct_ <http://example.org/types#hasName> "PosPct" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct_ .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> "mag" .?valve_PosPct_ <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct___Period___mag_ .?valve_PosPct___Period___mag_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve_PosPct___Period___mag__timeseries .?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve_PosPct___Period___mag__timeseries_datapoint .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve_PosPct___Period___mag__timeseries_datapoint_value .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve_PosPct_ <http://example.org/types#hasName> ?valve_PosPct__name_on_path .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> ?valve_PosPct___Period___mag__name_on_path . FILTER(((?timestamp >= "2021-11-30T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>) && (?timestamp <= "2021-12-01T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>))) BIND(CONCAT(?ABC_name_on_path, "-", ?valve_name_on_path, ".", ?valve__Dash___Mvm__name_on_path, ".", ?valve__Dash___Mvm___Period___stVal__name_on_path) AS ?valve__Dash___Mvm___Period___stVal__path_name) BIND(CONCAT(?valve_name_on_path, ".", ?valve_PosPct__name_on_path, ".", ?valve_PosPct___Period___mag__name_on_path) AS ?valve_PosPct___Period___mag__path_name) BIND(FLOOR(<https://github.com/magbak/chrontext#DateTimeAsSeconds>(?timestamp) / "600"^^<http://www.w3.org/2001/XMLSchema#unsignedLong>) * "600"^^<http://www.w3.org/2001/XMLSchema#unsignedLong> AS ?timestamp_grouping) } GROUP BY ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?timestamp_grouping} BIND(<https://github.com/magbak/chrontext#SecondsAsDateTime>(?timestamp_grouping) AS ?timestamp) BIND(CONCAT(?ABC, "-", ?valve, ".", ?valve__Dash___Mvm_, ".", ?valve__Dash___Mvm___Period___stVal_) AS ?valve_path_name) } GROUP BY ?valve_path_name ?timestamp} }"#.to_string();
    assert_eq!(expected_query_str, actual_str);
}

#[rstest]
fn test_only_group_translation(mut translator: Translator) {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    group valve
"#;
    let (_, tsq) = ts_query(q).expect("No problemo");
    let actual = translator.translate(&tsq);
    //println!("{}", actual);
    let expected_query_str = r#"SELECT ?valve_path_name ?valve__Dash___Mvm___Period___stVal__path_name ?valve_PosPct___Period___mag__path_name ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value ?valve_PosPct___Period___mag__timeseries_datapoint_value ?timestamp WHERE { {SELECT (<https://github.com/magbak/chrontext#nestAggregation>(?valve__Dash___Mvm___Period___stVal__path_name) AS ?valve__Dash___Mvm___Period___stVal__path_name) (<https://github.com/magbak/chrontext#nestAggregation>(?valve_PosPct___Period___mag__path_name) AS ?valve_PosPct___Period___mag__path_name) (<https://github.com/magbak/chrontext#nestAggregation>(?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) AS ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value) (<https://github.com/magbak/chrontext#nestAggregation>(?valve_PosPct___Period___mag__timeseries_datapoint_value) AS ?valve_PosPct___Period___mag__timeseries_datapoint_value) ?valve_path_name ?timestamp WHERE { ?ABC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type_var_0 .?type_var_0 <http://example.org/types#hasName> "ABC" .?valve <http://example.org/types#hasName> "HLV" .?ABC <http://example.org/types#hasOneDashRelation> ?valve .?valve__Dash___Mvm_ <http://example.org/types#hasName> "Mvm" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm_ .?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> "stVal" .?valve__Dash___Mvm_ <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm___Period___stVal_ .?valve__Dash___Mvm___Period___stVal_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve__Dash___Mvm___Period___stVal__timeseries .?valve__Dash___Mvm___Period___stVal__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint .?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve__Dash___Mvm___Period___stVal__timeseries_datapoint_value .?valve__Dash___Mvm___Period___stVal__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?ABC <http://example.org/types#hasName> ?ABC_name_on_path .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve__Dash___Mvm_ <http://example.org/types#hasName> ?valve__Dash___Mvm__name_on_path .?valve__Dash___Mvm___Period___stVal_ <http://example.org/types#hasName> ?valve__Dash___Mvm___Period___stVal__name_on_path .?valve_PosPct_ <http://example.org/types#hasName> "PosPct" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct_ .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> "mag" .?valve_PosPct_ <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct___Period___mag_ .?valve_PosPct___Period___mag_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve_PosPct___Period___mag__timeseries .?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve_PosPct___Period___mag__timeseries_datapoint .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve_PosPct___Period___mag__timeseries_datapoint_value .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve_PosPct_ <http://example.org/types#hasName> ?valve_PosPct__name_on_path .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> ?valve_PosPct___Period___mag__name_on_path . FILTER(((?timestamp >= "2021-11-30T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>) && (?timestamp <= "2021-12-01T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>))) BIND(CONCAT(?ABC_name_on_path, "-", ?valve_name_on_path, ".", ?valve__Dash___Mvm__name_on_path, ".", ?valve__Dash___Mvm___Period___stVal__name_on_path) AS ?valve__Dash___Mvm___Period___stVal__path_name) BIND(CONCAT(?valve_name_on_path, ".", ?valve_PosPct__name_on_path, ".", ?valve_PosPct___Period___mag__name_on_path) AS ?valve_PosPct___Period___mag__path_name) BIND(CONCAT(?ABC, "-", ?valve, ".", ?valve__Dash___Mvm_, ".", ?valve__Dash___Mvm___Period___stVal_) AS ?valve_path_name) } GROUP BY ?valve_path_name ?timestamp} }"#.to_string();
    assert_eq!(expected_query_str, actual.to_string());
}

#[rstest]
fn test_only_group_existing_path_translation(mut translator: Translator) {
    let q = r#"
    ABC-[valve]"HLV"."Mvm".[stval]"stVal"
    [valve]."PosPct"."mag"
    from 2021-12-01T00:00:01+01:00
    to 2021-12-02T00:00:01+01:00
    group stval
"#;
    let (_, tsq) = ts_query(q).expect("No problemo");
    let actual = translator.translate(&tsq);
    //println!("{}", actual);
    let expected_query_str = r#"SELECT ?stval_path_name ?valve_PosPct___Period___mag__path_name ?stval_timeseries_datapoint_value ?valve_PosPct___Period___mag__timeseries_datapoint_value ?timestamp WHERE { {SELECT (SAMPLE(?stval_path_name) AS ?stval_path_name) (<https://github.com/magbak/chrontext#nestAggregation>(?valve_PosPct___Period___mag__path_name) AS ?valve_PosPct___Period___mag__path_name) (SAMPLE(?stval_timeseries_datapoint_value) AS ?stval_timeseries_datapoint_value) (<https://github.com/magbak/chrontext#nestAggregation>(?valve_PosPct___Period___mag__timeseries_datapoint_value) AS ?valve_PosPct___Period___mag__timeseries_datapoint_value) ?stval_path_name ?timestamp WHERE { ?ABC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type_var_0 .?type_var_0 <http://example.org/types#hasName> "ABC" .?valve <http://example.org/types#hasName> "HLV" .?ABC <http://example.org/types#hasOneDashRelation> ?valve .?valve__Dash___Mvm_ <http://example.org/types#hasName> "Mvm" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve__Dash___Mvm_ .?stval <http://example.org/types#hasName> "stVal" .?valve__Dash___Mvm_ <http://example.org/types#hasOnePeriodRelation> ?stval .?stval <https://github.com/magbak/chrontext#hasTimeseries> ?stval_timeseries .?stval_timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?stval_timeseries_datapoint .?stval_timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?stval_timeseries_datapoint_value .?stval_timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?ABC <http://example.org/types#hasName> ?ABC_name_on_path .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve__Dash___Mvm_ <http://example.org/types#hasName> ?valve__Dash___Mvm__name_on_path .?stval <http://example.org/types#hasName> ?stval_name_on_path .?valve_PosPct_ <http://example.org/types#hasName> "PosPct" .?valve <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct_ .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> "mag" .?valve_PosPct_ <http://example.org/types#hasOnePeriodRelation> ?valve_PosPct___Period___mag_ .?valve_PosPct___Period___mag_ <https://github.com/magbak/chrontext#hasTimeseries> ?valve_PosPct___Period___mag__timeseries .?valve_PosPct___Period___mag__timeseries <https://github.com/magbak/chrontext#hasDataPoint> ?valve_PosPct___Period___mag__timeseries_datapoint .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasValue> ?valve_PosPct___Period___mag__timeseries_datapoint_value .?valve_PosPct___Period___mag__timeseries_datapoint <https://github.com/magbak/chrontext#hasTimestamp> ?timestamp .?valve <http://example.org/types#hasName> ?valve_name_on_path .?valve_PosPct_ <http://example.org/types#hasName> ?valve_PosPct__name_on_path .?valve_PosPct___Period___mag_ <http://example.org/types#hasName> ?valve_PosPct___Period___mag__name_on_path . FILTER(((?timestamp >= "2021-11-30T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>) && (?timestamp <= "2021-12-01T23:00:01+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>))) BIND(CONCAT(?ABC_name_on_path, "-", ?valve_name_on_path, ".", ?valve__Dash___Mvm__name_on_path, ".", ?stval_name_on_path) AS ?stval_path_name) BIND(CONCAT(?valve_name_on_path, ".", ?valve_PosPct__name_on_path, ".", ?valve_PosPct___Period___mag__name_on_path) AS ?valve_PosPct___Period___mag__path_name) } GROUP BY ?stval_path_name ?timestamp} }"#.to_string();
    assert_eq!(expected_query_str, actual.to_string());
}
