extern crate nom;

use crate::ast::{
    Aggregation, ArrowType, BooleanOperator, ConditionedPath, Connective, ConnectiveType, DataType,
    ElementConstraint, Glue, GraphPathPattern, Group, InputOutput, LiteralData, Path, PathElement,
    PathElementOrConnective, PathOrLiteralData, TsApi, TsQuery, TypedLabel,
};
use chrono::{DateTime, Utc};
use dateparser::DateTimeUtc;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{
    alpha1, alphanumeric0, alphanumeric1, char, digit1, newline, not_line_ending, space0, space1,
};
use nom::combinator::{not, opt};
use nom::error::{Error, ErrorKind};
use nom::multi::{many0, many1, separated_list1};
use nom::sequence::{delimited, pair, tuple};
use nom::Err::Failure;
use nom::IResult;
use std::str::FromStr;
use std::time::Duration;

fn not_keyword(k: &str) -> IResult<&str, &str> {
    not(alt((
        tag("from"),
        tag("to"),
        tag("aggregate"),
        tag("true"),
        tag("false"),
        tag("group"),
    )))(k)?;
    Ok((k, k))
}

fn connective(c: &str) -> IResult<&str, Connective> {
    let (c, conns) = alt((
        many1(char('.')),
        many1(char('-')),
        many1(char(':')),
        many1(char(';')),
        many1(char('/')),
        many1(char('\\')),
    ))(c)?;
    assert!(conns.len() > 0);
    Ok((
        c,
        Connective::new(ConnectiveType::new(conns.get(0).unwrap()), conns.len()),
    ))
}

fn glue(g: &str) -> IResult<&str, PathElement> {
    let (g, gstr) = delimited(tag("["), alphanumeric1, tag("]"))(g)?;
    Ok((g, PathElement::new(Some(Glue::new(gstr)), None)))
}

fn name_constraint(n: &str) -> IResult<&str, ElementConstraint> {
    let (n, s) = delimited(tag("\""), alphanumeric1, tag("\""))(n)?;
    Ok((n, ElementConstraint::Name(s.to_string())))
}

fn type_constraint(t: &str) -> IResult<&str, ElementConstraint> {
    let (t, (_, f, s)) = tuple((not_keyword, alpha1, alphanumeric0))(t)?;
    Ok((t, ElementConstraint::TypeName(f.to_string() + s)))
}

fn type_and_name_constraint(tn: &str) -> IResult<&str, ElementConstraint> {
    let (tn, (_, n, _, _, t)) =
        tuple((tag("\""), alphanumeric1, tag("\""), char(':'), alpha1))(tn)?;
    Ok((
        tn,
        ElementConstraint::TypeNameAndName(n.to_string(), t.to_string()),
    ))
}

fn element_constraint(e: &str) -> IResult<&str, PathElement> {
    let (e, c) = alt((type_and_name_constraint, name_constraint, type_constraint))(e)?;
    Ok((e, PathElement::new(None, Some(c))))
}

fn glued_element(e: &str) -> IResult<&str, PathElement> {
    let (e, (g, c)) = pair(glue, element_constraint)(e)?;
    Ok((e, PathElement::new(g.glue, c.element)))
}

fn path_element(p: &str) -> IResult<&str, PathElement> {
    alt((glued_element, glue, element_constraint))(p)
}

fn singleton_path(p: &str) -> IResult<&str, Path> {
    let (p, el) = path_element(p)?;
    Ok((
        p,
        Path::from_vec(vec![PathElementOrConnective::PathElement(el)]),
    ))
}

fn path_triple(p: &str) -> IResult<&str, Path> {
    let (p, (pe, conn, mut pa)) = tuple((path_element, connective, path))(p)?;
    let conn_or = PathElementOrConnective::Connective(conn);
    let pe_or = PathElementOrConnective::PathElement(pe);
    pa.prepend(conn_or);
    pa.prepend(pe_or);
    Ok((p, pa))
}

fn path(p: &str) -> IResult<&str, Path> {
    alt((path_triple, singleton_path))(p)
}

fn questionable_path(p: &str) -> IResult<&str, Path> {
    let (p, (mut pa, qm)) = tuple((path, opt(pair(space0, char('?')))))(p)?;
    if qm.is_some() {
        pa.optional = true;
    }
    Ok((p, pa))
}

fn numeric_literal(l: &str) -> IResult<&str, LiteralData> {
    let (l, (num1, opt_num2)) = pair(digit1, opt(pair(tag("."), digit1)))(l)?;
    match opt_num2 {
        Some((dot, num2)) => Ok((
            l,
            LiteralData::Real(
                f64::from_str(&(num1.to_owned() + dot + num2)).expect("Failed to parse float64"),
            ),
        )),
        None => Ok((
            l,
            LiteralData::Integer(i32::from_str(num1).expect("Failed to parse int32")),
        )),
    }
}

fn string_literal(s: &str) -> IResult<&str, LiteralData> {
    let (s, lit) = delimited(tag("\""), not_line_ending, tag("\""))(s)?;
    Ok((s, LiteralData::String(lit.to_string())))
}

fn boolean_literal(b: &str) -> IResult<&str, LiteralData> {
    let (b, lit) = alt((tag("true"), tag("false")))(b)?;
    let boolean = if lit == "true" { true } else { false };
    Ok((b, LiteralData::Boolean(boolean)))
}

fn literal(l: &str) -> IResult<&str, LiteralData> {
    alt((numeric_literal, string_literal, boolean_literal))(l)
}

fn literal_as_path_or_literal(l: &str) -> IResult<&str, PathOrLiteralData> {
    let (l, lit) = literal(l)?;
    Ok((l, PathOrLiteralData::Literal(lit)))
}

fn boolean_operator(o: &str) -> IResult<&str, BooleanOperator> {
    let (o, opstr) = alt((
        tag("="),
        tag("!="),
        tag(">"),
        tag("<"),
        tag(">="),
        tag("<="),
        tag("like"),
    ))(o)?;
    Ok((o, BooleanOperator::new(opstr)))
}

fn path_as_path_or_literal(p: &str) -> IResult<&str, PathOrLiteralData> {
    let (p, path) = path(p)?;
    Ok((p, PathOrLiteralData::Path(path)))
}

fn path_or_literal(pl: &str) -> IResult<&str, PathOrLiteralData> {
    alt((path_as_path_or_literal, literal_as_path_or_literal))(pl)
}

fn conditioned_path(cp: &str) -> IResult<&str, ConditionedPath> {
    let (cp, (p, _, bop, _, pol)) = tuple((
        questionable_path,
        space0,
        boolean_operator,
        space0,
        path_or_literal,
    ))(cp)?;
    Ok((cp, ConditionedPath::new(p, bop, pol)))
}

fn path_as_conditioned(p: &str) -> IResult<&str, ConditionedPath> {
    let (p, pa) = path(p)?;
    Ok((p, ConditionedPath::from_path(pa)))
}

fn graph_pattern(g: &str) -> IResult<&str, GraphPathPattern> {
    let (g, cp) = many1(tuple((
        many0(newline),
        space0,
        alt((conditioned_path, path_as_conditioned)),
        many0(newline),
    )))(g)?;
    Ok((
        g,
        GraphPathPattern::new(cp.into_iter().map(|(_, _, c, _)| c).collect()),
    ))
}

//Will fail when attempting invalid datetime!
fn datetime(d: &str) -> IResult<&str, DateTime<Utc>> {
    let (d, r) = not_line_ending(d)?;
    let dt_res = r.parse::<DateTimeUtc>();
    match dt_res {
        Ok(dt) => Ok((d, dt.0)),
        Err(_) => Err(Failure(Error {
            input: d,
            code: ErrorKind::Permutation,
        })),
    }
}

fn from(f: &str) -> IResult<&str, DateTime<Utc>> {
    let (f, (_, _, _, dt, _, _)) = tuple((
        space0,
        tag("from"),
        space1,
        datetime,
        space0,
        many0(newline),
    ))(f)?;
    Ok((f, dt))
}

fn to(t: &str) -> IResult<&str, DateTime<Utc>> {
    let (t, (_, _, _, dt, _, _)) =
        tuple((space0, tag("to"), space1, datetime, space0, many0(newline)))(t)?;
    Ok((t, dt))
}

fn group(g: &str) -> IResult<&str, Group> {
    let (g, (_, _, _, var_names, _, _)) = tuple((
        space0,
        tag("group"),
        space1,
        separated_list1(tag(","), alpha1),
        space0,
        many0(newline),
    ))(g)?;
    Ok((g, Group::new(var_names)))
}

fn duration(d: &str) -> IResult<&str, Duration> {
    let (d, dur_str) = not_line_ending(d)?;
    let dur_res = duration_str::parse(dur_str);
    match dur_res {
        Ok(dur) => Ok((d, dur)),
        Err(_) => Err(Failure(Error {
            input: d,
            code: ErrorKind::Fail,
        })),
    }
}

fn aggregation(a: &str) -> IResult<&str, Aggregation> {
    let (a, (_, _, _, funcname, _, duration, _)) = tuple((
        space0,
        tag("aggregate"),
        space1,
        alpha1,
        space1,
        duration,
        many0(newline),
    ))(a)?;
    Ok((a, Aggregation::new(funcname, duration)))
}

pub fn ts_query(t: &str) -> IResult<&str, TsQuery> {
    let (t, (_, graph_pattern, from_datetime, to_datetime, group, aggregation)) = tuple((
        many0(newline),
        graph_pattern,
        opt(from),
        opt(to),
        opt(group),
        opt(aggregation),
    ))(t)?;
    Ok((
        t,
        TsQuery::new(
            graph_pattern,
            group,
            from_datetime,
            to_datetime,
            aggregation,
        ),
    ))
}

fn data_type(d: &str) -> IResult<&str, DataType> {
    let (d, dt) = alpha1(d)?;
    Ok((d, DataType::new(dt)))
}

fn arrow(a: &str) -> IResult<&str, ArrowType> {
    let (a, arrow) = alt((tag("->"), tag("<-")))(a)?;
    Ok((a, ArrowType::new(arrow)))
}

fn typed_label(t: &str) -> IResult<&str, TypedLabel> {
    let (t, (label, _, data_type)) = tuple((alpha1, char(':'), data_type))(t)?;
    Ok((t, TypedLabel::new(label, data_type)))
}

fn input_output(io: &str) -> IResult<&str, InputOutput> {
    let (io, (_, path, _, arrow_type, _, label, _, _)) = tuple((
        space0,
        questionable_path,
        space1,
        arrow,
        space1,
        typed_label,
        space0,
        many0(newline),
    ))(io)?;
    Ok((io, InputOutput::new(path, arrow_type, label)))
}

pub fn ts_api(a: &str) -> IResult<&str, TsApi> {
    let (a, (_, _, inputs_outputs, group)) =
        tuple((space0, many0(newline), many1(input_output), group))(a)?;
    Ok((a, TsApi::new(inputs_outputs, group)))
}

#[test]
fn test_parse_path() {
    assert_eq!(
        connective("-"),
        Ok(("", Connective::new(ConnectiveType::Dash, 1)))
    );
    assert_eq!(
        path("Abc.\"cda\""),
        Ok((
            "",
            Path::from_vec(vec![
                PathElementOrConnective::PathElement(PathElement::new(
                    None,
                    Some(ElementConstraint::TypeName("Abc".to_string()))
                )),
                PathElementOrConnective::Connective(Connective {
                    connective_type: ConnectiveType::Period,
                    number_of: 1
                }),
                PathElementOrConnective::PathElement(PathElement::new(
                    None,
                    Some(ElementConstraint::Name("cda".to_string()))
                ))
            ])
        ))
    );
}

#[test]
fn test_parse_conditioned_path_literal() {
    let lhs = Path::from_vec(vec![
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("Abc".to_string())),
        )),
        PathElementOrConnective::Connective(Connective {
            connective_type: ConnectiveType::Period,
            number_of: 1,
        }),
        PathElementOrConnective::PathElement(PathElement::new(
            Some(Glue::new("mynode")),
            Some(ElementConstraint::Name("cda".to_string())),
        )),
    ]);
    assert_eq!(
        conditioned_path("Abc.[mynode]\"cda\" > 25"),
        Ok((
            "",
            ConditionedPath::new(
                lhs,
                BooleanOperator::GT,
                PathOrLiteralData::Literal(LiteralData::Integer(25))
            )
        ))
    );
}

#[test]
fn test_parse_conditioned_path_other_path() {
    let lhs = Path::from_vec(vec![
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("Abc".to_string())),
        )),
        PathElementOrConnective::Connective(Connective {
            connective_type: ConnectiveType::Period,
            number_of: 1,
        }),
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("cda".to_string())),
        )),
    ]);
    let rhs = Path::from_vec(vec![
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("acadadad".to_string())),
        )),
        PathElementOrConnective::Connective(Connective {
            connective_type: ConnectiveType::Dash,
            number_of: 1,
        }),
        PathElementOrConnective::PathElement(PathElement::new(
            None,
            Some(ElementConstraint::TypeName("bca".to_string())),
        )),
    ]);
    assert_eq!(
        conditioned_path("Abc.cda > acadadad-bca"),
        Ok((
            "",
            ConditionedPath::new(lhs, BooleanOperator::GT, PathOrLiteralData::Path(rhs))
        ))
    );
}
