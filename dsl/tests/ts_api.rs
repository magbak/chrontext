use dsl::ast::{
    ArrowType, Connective, ConnectiveType, DataType, ElementConstraint, Glue, Group, InputOutput,
    Path, PathElement, PathElementOrConnective, TsApi, TypedLabel,
};
use dsl::parser::ts_api;

#[test]
fn test_basic_api() {
    let q = r#"
    ABC-[valve]"HLV"."Mvm"."stVal"? -> status:Boolean
    [valve]."PosPct"."mag" -> magnitude:Real
    [valve]."myKpi":KPIType <- myKpi:Real
    group valve
"#;
    let (_, actual) = ts_api(q).expect("Noproblems");
    let expected = TsApi::new(
        vec![
            InputOutput::new(
                Path::new(
                    vec![
                        PathElementOrConnective::PathElement(PathElement::new(
                            None,
                            Some(ElementConstraint::TypeName("ABC".to_string())),
                        )),
                        PathElementOrConnective::Connective(Connective::new(
                            ConnectiveType::Dash,
                            1,
                        )),
                        PathElementOrConnective::PathElement(PathElement::new(
                            Some(Glue::new("valve")),
                            Some(ElementConstraint::Name("HLV".to_string())),
                        )),
                        PathElementOrConnective::Connective(Connective::new(
                            ConnectiveType::Period,
                            1,
                        )),
                        PathElementOrConnective::PathElement(PathElement::new(
                            None,
                            Some(ElementConstraint::Name("Mvm".to_string())),
                        )),
                        PathElementOrConnective::Connective(Connective::new(
                            ConnectiveType::Period,
                            1,
                        )),
                        PathElementOrConnective::PathElement(PathElement::new(
                            None,
                            Some(ElementConstraint::Name("stVal".to_string())),
                        )),
                    ],
                    true,
                ),
                ArrowType::Right,
                TypedLabel::new("status", DataType::Boolean),
            ),
            InputOutput::new(
                Path::from_vec(vec![
                    PathElementOrConnective::PathElement(PathElement::new(
                        Some(Glue::new("valve")),
                        None,
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(ElementConstraint::Name("PosPct".to_string())),
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(ElementConstraint::Name("mag".to_string())),
                    )),
                ]),
                ArrowType::Right,
                TypedLabel::new("magnitude", DataType::Real),
            ),
            InputOutput::new(
                Path::from_vec(vec![
                    PathElementOrConnective::PathElement(PathElement::new(
                        Some(Glue::new("valve")),
                        None,
                    )),
                    PathElementOrConnective::Connective(Connective::new(ConnectiveType::Period, 1)),
                    PathElementOrConnective::PathElement(PathElement::new(
                        None,
                        Some(ElementConstraint::TypeNameAndName(
                            "myKpi".to_string(),
                            "KPIType".to_string(),
                        )),
                    )),
                ]),
                ArrowType::Left,
                TypedLabel::new("myKpi", DataType::Real),
            ),
        ],
        Group::new(vec!["valve"]),
    );
    assert_eq!(expected, actual);
}
