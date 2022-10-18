use spargebra::algebra::GraphPattern;

pub fn apply_pushups(
    graph_pattern: GraphPattern,
    graph_pattern_pushups: &mut Vec<GraphPattern>,
) -> GraphPattern {
    graph_pattern_pushups
        .drain(0..graph_pattern_pushups.len())
        .fold(graph_pattern, |acc, elem| GraphPattern::LeftJoin {
            left: Box::new(acc),
            right: Box::new(elem),
            expression: None,
        })
}
