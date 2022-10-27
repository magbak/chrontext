use super::StaticQueryRewriter;
use crate::change_types::ChangeType;
use crate::query_context::{Context, PathEntry};
use crate::rewriting::graph_patterns::GPReturn;
use crate::rewriting::pushups::apply_pushups;
use spargebra::algebra::{Expression, GraphPattern};

impl StaticQueryRewriter {
    pub fn rewrite_left_join(
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
        expression_opt: &Option<Expression>,
        context: &Context,
    ) -> GPReturn {
        let mut left_rewrite = self.rewrite_graph_pattern(
            left,
            &context.extension_with(PathEntry::LeftJoinLeftSide),
        );
        let mut right_rewrite = self.rewrite_graph_pattern(
            right,
            &context.extension_with(PathEntry::LeftJoinRightSide),
        );
        let mut expression_rewrite_opt = None;

        if left_rewrite.graph_pattern.is_some() {
            if right_rewrite.graph_pattern.is_some() {
                left_rewrite.with_scope(&mut right_rewrite);

                if let Some(expression) = expression_opt {
                    expression_rewrite_opt = Some(self.rewrite_expression(
                        expression,
                        &ChangeType::Relaxed,
                        &left_rewrite.variables_in_scope,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ));
                }
                if let Some(mut expression_rewrite) = expression_rewrite_opt {
                    if expression_rewrite.expression.is_some() {
                        let use_change;
                        if expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                            && &left_rewrite.change_type == &ChangeType::NoChange
                            && &right_rewrite.change_type == &ChangeType::NoChange
                        {
                            use_change = ChangeType::NoChange;
                        } else if (expression_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || expression_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Relaxed)
                            && (&left_rewrite.change_type == &ChangeType::NoChange
                                || &left_rewrite.change_type == &ChangeType::Relaxed)
                            && (&right_rewrite.change_type == &ChangeType::NoChange
                                || &right_rewrite.change_type == &ChangeType::Relaxed)
                        {
                            use_change = ChangeType::Relaxed;
                        } else if (expression_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || expression_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Constrained)
                            && (&left_rewrite.change_type == &ChangeType::NoChange
                                || &left_rewrite.change_type == &ChangeType::Constrained)
                            && (&right_rewrite.change_type == &ChangeType::NoChange
                                || &right_rewrite.change_type == &ChangeType::Constrained)
                        {
                            use_change = ChangeType::Constrained;
                        } else {
                            return GPReturn::none();
                        }
                        let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                        let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                        left_rewrite
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(apply_pushups(
                                    left_graph_pattern,
                                    &mut expression_rewrite.graph_pattern_pushups,
                                )),
                                right: Box::new(right_graph_pattern),
                                expression: Some(expression_rewrite.expression.take().unwrap()),
                            })
                            .with_change_type(use_change);
                        return left_rewrite;
                    } else {
                        //Expression rewrite is none, but we had an original expression
                        if (&left_rewrite.change_type == &ChangeType::NoChange
                            || &left_rewrite.change_type == &ChangeType::Relaxed)
                            && (&right_rewrite.change_type == &ChangeType::NoChange
                                || &right_rewrite.change_type == &ChangeType::Relaxed)
                        {
                            let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                            let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                            left_rewrite
                                .with_graph_pattern(GraphPattern::LeftJoin {
                                    left: Box::new(apply_pushups(
                                        left_graph_pattern,
                                        &mut expression_rewrite.graph_pattern_pushups,
                                    )),
                                    right: Box::new(right_graph_pattern),
                                    expression: None,
                                })
                                .with_change_type(ChangeType::Relaxed);
                            return left_rewrite;
                        } else {
                            return GPReturn::none();
                        }
                    }
                } else {
                    //No original expression
                    if &left_rewrite.change_type == &ChangeType::NoChange
                        && &right_rewrite.change_type == &ChangeType::NoChange
                    {
                        let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                        let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                        left_rewrite
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                                expression: None,
                            })
                            .with_change_type(ChangeType::NoChange);
                        return left_rewrite;
                    } else if (&left_rewrite.change_type == &ChangeType::NoChange
                        || &left_rewrite.change_type == &ChangeType::Relaxed)
                        && (&right_rewrite.change_type == &ChangeType::NoChange
                            || &right_rewrite.change_type == &ChangeType::Relaxed)
                    {
                        let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                        let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                        left_rewrite
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                                expression: None,
                            })
                            .with_change_type(ChangeType::Relaxed);
                        return left_rewrite;
                    } else if (&left_rewrite.change_type == &ChangeType::NoChange
                        || &left_rewrite.change_type == &ChangeType::Constrained)
                        && (&right_rewrite.change_type == &ChangeType::NoChange
                            || &right_rewrite.change_type == &ChangeType::Constrained)
                    {
                        let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                        let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                        left_rewrite
                            .with_graph_pattern(GraphPattern::LeftJoin {
                                left: Box::new(left_graph_pattern),
                                right: Box::new(right_graph_pattern),
                                expression: None,
                            })
                            .with_change_type(ChangeType::Constrained);
                        return left_rewrite;
                    }
                }
            } else {
                //left some, right none
                if let Some(expression) = expression_opt {
                    expression_rewrite_opt = Some(self.rewrite_expression(
                        expression,
                        &ChangeType::Relaxed,
                        &left_rewrite.variables_in_scope,
                        &context.extension_with(PathEntry::LeftJoinExpression),
                    ));
                }
                if expression_rewrite_opt.is_some()
                    && expression_rewrite_opt
                        .as_ref()
                        .unwrap()
                        .expression
                        .is_some()
                {
                    if let Some(mut expression_rewrite) = expression_rewrite_opt {
                        if (expression_rewrite.change_type.as_ref().unwrap()
                            == &ChangeType::NoChange
                            || expression_rewrite.change_type.as_ref().unwrap()
                                == &ChangeType::Relaxed)
                            && (&left_rewrite.change_type == &ChangeType::NoChange
                                || &left_rewrite.change_type == &ChangeType::Relaxed)
                        {
                            let left_graph_pattern = left_rewrite.graph_pattern.take().unwrap();
                            left_rewrite
                                .with_graph_pattern(GraphPattern::Filter {
                                    expr: expression_rewrite.expression.take().unwrap(),
                                    inner: Box::new(apply_pushups(
                                        left_graph_pattern,
                                        &mut expression_rewrite.graph_pattern_pushups,
                                    )),
                                })
                                .with_change_type(ChangeType::Relaxed);
                            return left_rewrite;
                        }
                    }
                } else {
                    if &left_rewrite.change_type == &ChangeType::NoChange
                        || &left_rewrite.change_type == &ChangeType::Relaxed
                    {
                        left_rewrite.with_change_type(ChangeType::Relaxed);
                        return left_rewrite;
                    }
                }
            }
        } else if right_rewrite.graph_pattern.is_some()
        //left none, right some
        {
            if let Some(expression) = expression_opt {
                expression_rewrite_opt = Some(self.rewrite_expression(
                    expression,
                    &ChangeType::Relaxed,
                    &right_rewrite.variables_in_scope,
                    &context.extension_with(PathEntry::LeftJoinExpression),
                ));
            }
            if let Some(mut expression_rewrite) = expression_rewrite_opt {
                if expression_rewrite.expression.is_some()
                    && (expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::NoChange
                        || expression_rewrite.change_type.as_ref().unwrap() == &ChangeType::Relaxed)
                    && (&right_rewrite.change_type == &ChangeType::NoChange
                        || &right_rewrite.change_type == &ChangeType::Relaxed)
                {
                    let right_graph_pattern = right_rewrite.graph_pattern.take().unwrap();
                    right_rewrite
                        .with_graph_pattern(GraphPattern::Filter {
                            inner: Box::new(apply_pushups(
                                right_graph_pattern,
                                &mut expression_rewrite.graph_pattern_pushups,
                            )),
                            expr: expression_rewrite.expression.take().unwrap(),
                        })
                        .with_change_type(ChangeType::Relaxed);
                    return right_rewrite;
                }
            } else {
                if &right_rewrite.change_type == &ChangeType::NoChange
                    || &right_rewrite.change_type == &ChangeType::Relaxed
                {
                    right_rewrite.with_change_type(ChangeType::Relaxed);
                    return right_rewrite;
                }
            }
        }
        GPReturn::none()
    }
}
