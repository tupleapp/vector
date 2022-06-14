use diagnostic::{DiagnosticList, DiagnosticMessage, Severity, Span};
use lookup::LookupBuf;
use parser::ast::{self, Node, QueryTarget};

use crate::parser::ast::RootExpr;
use crate::{
    expression::*,
    program::ProgramInfo,
    state::{ExternalEnv, LocalEnv},
    Function, Program,
};

pub(crate) type Diagnostics = Vec<Box<dyn DiagnosticMessage>>;

pub(crate) struct Compiler<'a> {
    fns: &'a [Box<dyn Function>],
    diagnostics: Diagnostics,
    fallible: bool,
    abortable: bool,
    local: LocalEnv,
    external_queries: Vec<LookupBuf>,
    external_assignments: Vec<LookupBuf>,

    /// A list of variables that are missing, because the rhs expression of the
    /// assignment failed to compile.
    ///
    /// This list allows us to avoid printing "undefined variable" compilation
    /// errors when the reason for it being undefined is another compiler error.
    skip_missing_query_target: Vec<(QueryTarget, LookupBuf)>,

    /// Track which expression in a chain of expressions is fallible.
    ///
    /// It is possible for this state to switch from `None`, to `Some(T)` and
    /// back to `None`, if the parent expression of a fallible expression
    /// nullifies the fallibility of that expression.
    fallible_expression_error: Option<Box<dyn DiagnosticMessage>>,
}

impl<'a> Compiler<'a> {
    pub(super) fn new(fns: &'a [Box<dyn Function>]) -> Self {
        Self {
            fns,
            diagnostics: vec![],
            fallible: false,
            abortable: false,
            local: LocalEnv::default(),
            external_queries: vec![],
            external_assignments: vec![],
            skip_missing_query_target: vec![],
            fallible_expression_error: None,
        }
    }

    /// An intenal function used by `compile_for_repl`.
    ///
    /// This should only be used for its intended purpose.
    pub(super) fn new_with_local_state(fns: &'a [Box<dyn Function>], local: LocalEnv) -> Self {
        let mut compiler = Self::new(fns);
        compiler.local = local;
        compiler
    }

    pub(super) fn compile(
        mut self,
        ast: parser::Program,
        external: &mut ExternalEnv,
    ) -> Result<(Program, DiagnosticList), DiagnosticList> {
        let expressions = self.compile_root_exprs(ast, external);

        let (errors, warnings): (Vec<_>, Vec<_>) =
            self.diagnostics.into_iter().partition(|diagnostic| {
                matches!(diagnostic.severity(), Severity::Bug | Severity::Error)
            });

        if !errors.is_empty() {
            return Err(errors.into());
        }

        let info = ProgramInfo {
            fallible: self.fallible,
            abortable: self.abortable,
            target_queries: self.external_queries,
            target_assignments: self.external_assignments,
        };

        let expressions = Block::new(expressions, self.local);

        Ok((Program { expressions, info }, warnings.into()))
    }

    fn compile_exprs(
        &mut self,
        nodes: impl IntoIterator<Item = Node<ast::Expr>>,
        external: &mut ExternalEnv,
    ) -> Option<Vec<Expr>> {
        let mut exprs = vec![];
        for node in nodes {
            let expr = self.compile_expr(node, external)?;
            let type_def = expr.type_def((&self.local, external));
            exprs.push(expr);

            if type_def.is_never() {
                // This is a terminal expression. Further expressions must not be
                // compiled since they will never execute, but could alter the types of
                // variables in local or external scopes through assignments.
                break;
            }
        }
        Some(exprs)
    }

    fn compile_expr(&mut self, node: Node<ast::Expr>, external: &mut ExternalEnv) -> Option<Expr> {
        use ast::Expr::*;

        let span = node.span();

        let expr = match node.into_inner() {
            Literal(node) => self.compile_literal(node, external),
            Container(node) => self.compile_container(node, external).map(Into::into),
            IfStatement(node) => self.compile_if_statement(node, external).map(Into::into),
            Op(node) => self.compile_op(node, external).map(Into::into),
            Assignment(node) => self.compile_assignment(node, external).map(Into::into),
            Query(node) => self.compile_query(node, external).map(Into::into),
            FunctionCall(node) => self.compile_function_call(node, external).map(Into::into),
            Variable(node) => self.compile_variable(node, external).map(Into::into),
            Unary(node) => self.compile_unary(node, external).map(Into::into),
            Abort(node) => self.compile_abort(node, external).map(Into::into),
        }?;

        // If the previously compiled expression is fallible, _and_ we are
        // currently not tracking any existing fallible expression in the chain
        // of expressions, then this is the first expression within that chain
        // that can cause the entire chain to be fallible.
        if expr.type_def((&self.local, external)).is_fallible()
            && self.fallible_expression_error.is_none()
        {
            let error = crate::expression::Error::Fallible { span };
            self.fallible_expression_error = Some(Box::new(error) as _);
        }

        Some(expr)
    }

    #[cfg(feature = "expr-literal")]
    fn compile_literal(
        &mut self,
        node: Node<ast::Literal>,
        external: &mut ExternalEnv,
    ) -> Option<Expr> {
        use ast::Literal::*;
        use bytes::Bytes;

        let (span, lit) = node.take();

        let literal = match lit {
            String(template) => {
                if let Some(v) = template.as_literal_string() {
                    Ok(Literal::String(Bytes::from(v.to_string())))
                } else {
                    // Rewrite the template into an expression and compile that block.
                    return self.compile_expr(
                        Node::new(span, template.rewrite_to_concatenated_strings()),
                        external,
                    );
                }
            }
            RawString(v) => Ok(Literal::String(Bytes::from(v))),
            Integer(v) => Ok(Literal::Integer(v)),
            Float(v) => Ok(Literal::Float(v)),
            Boolean(v) => Ok(Literal::Boolean(v)),
            Regex(v) => regex::Regex::new(&v)
                .map_err(|err| literal::Error::from((span, err)))
                .map(|r| Literal::Regex(r.into())),
            // TODO: support more formats (similar to Vector's `Convert` logic)
            Timestamp(v) => v
                .parse()
                .map(Literal::Timestamp)
                .map_err(|err| literal::Error::from((span, err))),
            Null => Ok(Literal::Null),
        };

        literal
            .map(Into::into)
            .map_err(|err| self.diagnostics.push(Box::new(err)))
            .ok()
    }

    #[cfg(not(feature = "expr-literal"))]
    fn compile_literal(&mut self, node: Node<ast::Literal>, _: &mut ExternalEnv) -> Option<Expr> {
        self.handle_missing_feature_error(node.span(), "expr-literal")
    }

    fn compile_container(
        &mut self,
        node: Node<ast::Container>,
        external: &mut ExternalEnv,
    ) -> Option<Container> {
        use ast::Container::*;

        let variant = match node.into_inner() {
            Group(node) => self.compile_group(*node, external)?.into(),
            Block(node) => self.compile_block(node, external)?.into(),
            Array(node) => self.compile_array(node, external)?.into(),
            Object(node) => self.compile_object(node, external)?.into(),
        };

        Some(Container::new(variant))
    }

    fn compile_group(
        &mut self,
        node: Node<ast::Group>,
        external: &mut ExternalEnv,
    ) -> Option<Group> {
        let expr = self.compile_expr(node.into_inner().into_inner(), external)?;

        Some(Group::new(expr))
    }

    fn compile_root_exprs(
        &mut self,
        nodes: impl IntoIterator<Item = Node<ast::RootExpr>>,
        external: &mut ExternalEnv,
    ) -> Vec<Expr> {
        let mut node_exprs = vec![];

        // After a terminating expression, the state is stored, but the remaining expressions are checked.
        let mut terminated_state = None;

        for root_expr in nodes {
            match root_expr.into_inner() {
                RootExpr::Expr(node_expr) => {
                    self.fallible_expression_error = None;

                    if let Some(expr) = self.compile_expr(node_expr, external) {
                        if let Some(error) = self.fallible_expression_error.take() {
                            self.diagnostics.push(error);
                        }

                        if terminated_state.is_none() {
                            let type_def = expr.type_def((&self.local, external));
                            node_exprs.push(expr);
                            // an expression that has the "never" type is a terminating expression
                            if type_def.is_never() {
                                terminated_state =
                                    Some((self.local.clone(), external.target().clone()))
                            }
                        }
                    }
                }
                RootExpr::Error(err) => self.handle_parser_error(err),
            }
        }

        if let Some((local, details)) = terminated_state {
            self.local = local;
            external.update_target(details);
        }

        if node_exprs.is_empty() {
            node_exprs.push(Expr::Noop(Noop));
        }
        node_exprs
    }

    fn compile_block(
        &mut self,
        node: Node<ast::Block>,
        external: &mut ExternalEnv,
    ) -> Option<Block> {
        // We get a copy of the current local state, so that we can use it to
        // remove any *new* state added in the block, as that state is lexically
        // scoped to the block, and must not be visible to the rest of the
        // program.
        let local_snapshot = self.local.clone();

        // We can now start compiling the expressions within the block, which
        // will use the existing local state of the compiler, as blocks have
        // access to any state of their parent expressions.
        let exprs = match self.compile_exprs(node.into_inner().into_iter(), external) {
            Some(exprs) => exprs,
            None => {
                self.local = local_snapshot.apply_child_scope(self.local.clone());
                return None;
            }
        };

        // Now that we've compiled the expressions, we pass them into the block,
        // and also a copy of the local state, which includes any state added by
        // the compiled expressions in the block.
        let block = Block::new(exprs, self.local.clone());

        // Take the local state snapshot captured before we started compiling
        // the block, and merge back into it any mutations that happened to
        // state the snapshot was already tracking. Then, revert the compiler
        // local state to the updated snapshot.
        self.local = local_snapshot.apply_child_scope(self.local.clone());

        Some(block)
    }

    fn compile_array(
        &mut self,
        node: Node<ast::Array>,
        external: &mut ExternalEnv,
    ) -> Option<Array> {
        let exprs = self.compile_exprs(node.into_inner().into_iter(), external)?;

        Some(Array::new(exprs))
    }

    fn compile_object(
        &mut self,
        node: Node<ast::Object>,
        external: &mut ExternalEnv,
    ) -> Option<Object> {
        use std::collections::BTreeMap;

        let (keys, exprs): (Vec<String>, Vec<Option<Expr>>) = node
            .into_inner()
            .into_iter()
            .map(|(k, expr)| (k.into_inner(), self.compile_expr(expr, external)))
            .unzip();

        let exprs = exprs.into_iter().collect::<Option<Vec<_>>>()?;

        Some(Object::new(
            keys.into_iter().zip(exprs).collect::<BTreeMap<_, _>>(),
        ))
    }

    #[cfg(feature = "expr-if_statement")]
    fn compile_if_statement(
        &mut self,
        node: Node<ast::IfStatement>,
        external: &mut ExternalEnv,
    ) -> Option<IfStatement> {
        let ast::IfStatement {
            predicate,
            consequent,
            alternative,
        } = node.into_inner();

        let predicate = self
            .compile_predicate(predicate, external)?
            .map_err(|err| self.diagnostics.push(Box::new(err)))
            .ok()?;

        let original_locals = self.local.clone();
        let original_external = external.target().clone();

        let consequent = self.compile_block(consequent, external)?;

        match alternative {
            Some(block) => {
                let consequent_locals = self.local.clone();
                let consequent_external = external.target().clone();

                self.local = original_locals;

                let else_block = self.compile_block(block, external)?;

                // assignments must be the result of either the if or else block, but not the original value
                self.local = self.local.clone().merge(consequent_locals);
                external.update_target(consequent_external.merge(external.target().clone()));

                Some(IfStatement {
                    predicate,
                    consequent,
                    alternative: Some(else_block),
                })
            }
            None => {
                // assignments must be the result of either the if block or the original value
                self.local = self.local.clone().merge(original_locals);
                external.update_target(original_external.merge(external.target().clone()));

                Some(IfStatement {
                    predicate,
                    consequent,
                    alternative: None,
                })
            }
        }
    }

    #[cfg(not(feature = "expr-if_statement"))]
    fn compile_if_statement(
        &mut self,
        node: Node<ast::IfStatement>,
        _: &mut ExternalEnv,
    ) -> Option<Expr> {
        self.handle_missing_feature_error(node.span(), "expr-if_statement")
    }

    #[cfg(feature = "expr-if_statement")]
    fn compile_predicate(
        &mut self,
        node: Node<ast::Predicate>,
        external: &mut ExternalEnv,
    ) -> Option<predicate::Result> {
        use ast::Predicate::*;

        let (span, predicate) = node.take();

        let exprs = match predicate {
            One(node) => vec![self.compile_expr(*node, external)?],
            Many(nodes) => self.compile_exprs(nodes, external)?,
        };

        Some(Predicate::new(
            Node::new(span, exprs),
            (&self.local, external),
            self.fallible_expression_error.as_deref(),
        ))
    }

    #[cfg(feature = "expr-op")]
    fn compile_op(&mut self, node: Node<ast::Op>, external: &mut ExternalEnv) -> Option<Op> {
        use parser::ast::Opcode;

        let op = node.into_inner();
        let ast::Op(lhs, opcode, rhs) = op;

        let lhs_span = lhs.span();
        let lhs = Node::new(lhs_span, self.compile_expr(*lhs, external)?);

        // If we're using error-coalescing, we need to negate any tracked
        // fallibility error state for the lhs expression.
        if opcode.inner() == &Opcode::Err {
            self.fallible_expression_error = None;
        }

        let rhs_span = rhs.span();
        let rhs = Node::new(rhs_span, self.compile_expr(*rhs, external)?);

        Op::new(lhs, opcode, rhs, (&mut self.local, external))
            .map_err(|err| self.diagnostics.push(Box::new(err)))
            .ok()
    }

    #[cfg(not(feature = "expr-op"))]
    fn compile_op(&mut self, node: Node<ast::Op>, _: &mut ExternalEnv) -> Option<Expr> {
        self.handle_missing_feature_error(node.span(), "expr-op")
    }

    /// Rewrites the ast for `a |= b` to be `a = a | b`.
    #[cfg(feature = "expr-assignment")]
    fn rewrite_to_merge(
        &mut self,
        span: diagnostic::Span,
        target: &Node<ast::AssignmentTarget>,
        expr: Box<Node<ast::Expr>>,
        external: &mut ExternalEnv,
    ) -> Option<Box<Node<Expr>>> {
        Some(Box::new(Node::new(
            span,
            Expr::Op(self.compile_op(
                Node::new(
                    span,
                    ast::Op(
                        Box::new(Node::new(target.span(), target.inner().to_expr(span))),
                        Node::new(span, ast::Opcode::Merge),
                        expr,
                    ),
                ),
                external,
            )?),
        )))
    }

    #[cfg(feature = "expr-assignment")]
    fn compile_assignment(
        &mut self,
        node: Node<ast::Assignment>,
        external: &mut ExternalEnv,
    ) -> Option<Assignment> {
        use assignment::Variant;
        use ast::{Assignment::*, AssignmentOp};
        use value::Value;

        let assignment = node.into_inner();

        let node = match assignment {
            Single { target, op, expr } => {
                let span = expr.span();

                match op {
                    AssignmentOp::Assign => {
                        let expr = self
                            .compile_expr(*expr, external)
                            .map(|expr| Box::new(Node::new(span, expr)))
                            .or_else(|| {
                                self.skip_missing_assignment_target(target.clone().into_inner());
                                None
                            })?;

                        Node::new(span, Variant::Single { target, expr })
                    }
                    AssignmentOp::Merge => {
                        let expr = self.rewrite_to_merge(span, &target, expr, external)?;
                        Node::new(span, Variant::Single { target, expr })
                    }
                }
            }
            Infallible { ok, err, op, expr } => {
                let span = expr.span();

                let node = match op {
                    AssignmentOp::Assign => {
                        let expr = self
                            .compile_expr(*expr, external)
                            .map(|expr| Box::new(Node::new(span, expr)))
                            .or_else(|| {
                                self.skip_missing_assignment_target(ok.clone().into_inner());
                                self.skip_missing_assignment_target(err.clone().into_inner());
                                None
                            })?;

                        let node = Variant::Infallible {
                            ok,
                            err,
                            expr,
                            default: Value::Null,
                        };
                        Node::new(span, node)
                    }
                    AssignmentOp::Merge => {
                        let expr = self.rewrite_to_merge(span, &ok, expr, external)?;
                        let node = Variant::Infallible {
                            ok,
                            err,
                            expr,
                            default: Value::Null,
                        };

                        Node::new(span, node)
                    }
                };

                // If the RHS expression is marked as fallible, the "infallible"
                // assignment nullifies this fallibility, and thus no error
                // should be emitted.
                self.fallible_expression_error = None;

                node
            }
        };

        let assignment = Assignment::new(
            node,
            &mut self.local,
            external,
            self.fallible_expression_error.as_deref(),
        )
        .map_err(|err| self.diagnostics.push(Box::new(err)))
        .ok()?;

        // Track any potential external target assignments within the program.
        //
        // This data is exposed to the caller of the compiler, to allow any
        // potential external optimizations.
        for target in assignment.targets() {
            if let assignment::Target::External(path) = target {
                self.external_assignments.push(path);
            }
        }

        Some(assignment)
    }

    #[cfg(not(feature = "expr-assignment"))]
    fn compile_assignment(
        &mut self,
        node: Node<ast::Assignment>,
        _: &mut ExternalEnv,
    ) -> Option<Expr> {
        self.handle_missing_feature_error(node.span(), "expr-assignment")
    }

    #[cfg(feature = "expr-query")]
    fn compile_query(
        &mut self,
        node: Node<ast::Query>,
        external: &mut ExternalEnv,
    ) -> Option<Query> {
        let ast::Query { target, path } = node.into_inner();

        if self
            .skip_missing_query_target
            .contains(&(target.clone().into_inner(), path.clone().into_inner()))
        {
            return None;
        }

        let path = path.into_inner();
        let target = self.compile_query_target(target, external)?;

        // Track any potential external target queries within the program.
        //
        // This data is exposed to the caller of the compiler, to allow any
        // potential external optimizations.
        if let Target::External = target {
            self.external_queries.push(path.clone())
        }

        Some(Query::new(target, path))
    }

    #[cfg(not(feature = "expr-query"))]
    fn compile_query(&mut self, node: Node<ast::Query>, _: &mut ExternalEnv) -> Option<Expr> {
        self.handle_missing_feature_error(node.span(), "expr-query")
    }

    #[cfg(feature = "expr-query")]
    fn compile_query_target(
        &mut self,
        node: Node<ast::QueryTarget>,
        external: &mut ExternalEnv,
    ) -> Option<query::Target> {
        use ast::QueryTarget::*;

        let span = node.span();

        let target = match node.into_inner() {
            External => Target::External,
            Internal(ident) => {
                let variable = self.compile_variable(Node::new(span, ident), external)?;
                Target::Internal(variable)
            }
            Container(container) => {
                let container = self.compile_container(Node::new(span, container), external)?;
                Target::Container(container)
            }
            FunctionCall(call) => {
                let call = self.compile_function_call(Node::new(span, call), external)?;
                Target::FunctionCall(call)
            }
        };

        Some(target)
    }

    #[cfg(feature = "expr-function_call")]
    fn compile_function_call(
        &mut self,
        node: Node<ast::FunctionCall>,
        external: &mut ExternalEnv,
    ) -> Option<FunctionCall> {
        let call_span = node.span();
        let ast::FunctionCall {
            ident,
            abort_on_error,
            arguments,
            closure,
        } = node.into_inner();

        // TODO: Remove this (hacky) code once dynamic path syntax lands.
        //
        // See: https://github.com/vectordotdev/vector/issues/12547
        if ident.as_deref() == "get" {
            self.external_queries.push(LookupBuf::root())
        }

        let arguments = arguments
            .into_iter()
            .map(|node| {
                Some(Node::new(
                    node.span(),
                    self.compile_function_argument(node, external)?,
                ))
            })
            .collect::<Option<_>>()?;

        if abort_on_error {
            self.fallible = true;
        }

        let (closure_variables, closure_block) = match closure {
            Some(closure) => {
                let span = closure.span();
                let ast::FunctionClosure { variables, block } = closure.into_inner();
                (Some(Node::new(span, variables)), Some(block))
            }
            None => (None, None),
        };

        // Keep track of the known scope *before* we compile the closure.
        //
        // This allows us to revert to any known state that the closure
        // arguments might overwrite.
        let local_snapshot = self.local.clone();

        // First, we create a new function-call builder to validate the
        // expression.
        function_call::Builder::new(
            call_span,
            ident,
            abort_on_error,
            arguments,
            self.fns,
            &mut self.local,
            external,
            closure_variables,
        )
        // Then, we compile the closure block, and compile the final
        // function-call expression, including the attached closure.
        .map_err(|err| self.diagnostics.push(Box::new(err)))
        .ok()
        .and_then(|builder| {
            let block = match closure_block {
                None => None,
                Some(block) => {
                    let span = block.span();
                    match self.compile_block(block, external) {
                        Some(block) => Some(Node::new(span, block)),
                        None => return None,
                    }
                }
            };

            builder
                .compile(
                    &mut self.local,
                    external,
                    block,
                    local_snapshot,
                    &mut self.fallible_expression_error,
                )
                .map_err(|err| self.diagnostics.push(Box::new(err)))
                .ok()
        })
    }

    #[cfg(feature = "expr-function_call")]
    fn compile_function_argument(
        &mut self,
        node: Node<ast::FunctionArgument>,
        external: &mut ExternalEnv,
    ) -> Option<FunctionArgument> {
        let ast::FunctionArgument { ident, expr } = node.into_inner();
        let expr = Node::new(expr.span(), self.compile_expr(expr, external)?);

        Some(FunctionArgument::new(ident, expr))
    }

    #[cfg(not(feature = "expr-function_call"))]
    fn compile_function_call(
        &mut self,
        node: Node<ast::FunctionCall>,
        _: &mut ExternalEnv,
    ) -> Option<Noop> {
        // Guard against `dead_code` lint, to avoid having to sprinkle
        // attributes all over the place.
        let _ = self.fns;

        self.handle_missing_feature_error(node.span(), "expr-function_call");
        None
    }

    fn compile_variable(
        &mut self,
        node: Node<ast::Ident>,
        _external: &mut ExternalEnv,
    ) -> Option<Variable> {
        let (span, ident) = node.take();

        if self
            .skip_missing_query_target
            .contains(&(QueryTarget::Internal(ident.clone()), LookupBuf::root()))
        {
            return None;
        }

        Variable::new(span, ident, &self.local)
            .map_err(|err| self.diagnostics.push(Box::new(err)))
            .ok()
    }

    #[cfg(feature = "expr-unary")]
    fn compile_unary(
        &mut self,
        node: Node<ast::Unary>,
        external: &mut ExternalEnv,
    ) -> Option<Unary> {
        use ast::Unary::*;

        let variant = match node.into_inner() {
            Not(node) => self.compile_not(node, external)?.into(),
        };

        Some(Unary::new(variant))
    }

    #[cfg(not(feature = "expr-unary"))]
    fn compile_unary(&mut self, node: Node<ast::Unary>, _: &mut ExternalEnv) -> Option<Expr> {
        use ast::Unary::*;

        let span = match node.into_inner() {
            Not(node) => node.take().1.take().0,
        };

        self.handle_missing_feature_error(span.span(), "expr-unary")
    }

    #[cfg(feature = "expr-unary")]
    fn compile_not(&mut self, node: Node<ast::Not>, external: &mut ExternalEnv) -> Option<Not> {
        let (not, expr) = node.into_inner().take();

        let node = Node::new(expr.span(), self.compile_expr(*expr, external)?);

        Not::new(node, not.span(), (&self.local, external))
            .map_err(|err| self.diagnostics.push(Box::new(err)))
            .ok()
    }

    #[cfg(feature = "expr-abort")]
    fn compile_abort(
        &mut self,
        node: Node<ast::Abort>,
        external: &mut ExternalEnv,
    ) -> Option<Abort> {
        self.abortable = true;
        let (span, abort) = node.take();
        let message = match abort.message {
            Some(node) => Some(
                (*node).map_option(|expr| self.compile_expr(Node::new(span, expr), external))?,
            ),
            None => None,
        };

        Abort::new(span, message, (&self.local, external))
            .map_err(|err| self.diagnostics.push(Box::new(err)))
            .ok()
    }

    #[cfg(not(feature = "expr-abort"))]
    fn compile_abort(&mut self, node: Node<ast::Abort>, _: &mut ExternalEnv) -> Option<Expr> {
        self.handle_missing_feature_error(node.span(), "expr-abort")
    }

    fn handle_parser_error(&mut self, error: parser::Error) {
        self.diagnostics.push(Box::new(error))
    }

    #[allow(dead_code)]
    fn handle_missing_feature_error(&mut self, span: Span, feature: &'static str) -> Option<Expr> {
        self.diagnostics
            .push(Box::new(Error::Missing { span, feature }));

        None
    }

    #[cfg(feature = "expr-assignment")]
    fn skip_missing_assignment_target(&mut self, target: ast::AssignmentTarget) {
        let query = match target {
            ast::AssignmentTarget::Noop => return,
            ast::AssignmentTarget::Query(ast::Query { target, path }) => {
                (target.into_inner(), path.into_inner())
            }
            ast::AssignmentTarget::Internal(ident, path) => (
                QueryTarget::Internal(ident),
                path.unwrap_or_else(LookupBuf::root),
            ),
            ast::AssignmentTarget::External(path) => {
                (QueryTarget::External, path.unwrap_or_else(LookupBuf::root))
            }
        };

        self.skip_missing_query_target.push(query);
    }
}
