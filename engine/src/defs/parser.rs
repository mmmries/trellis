//! Recursive-descent parser for the transform-definition grammar.
//!
//! Concrete syntax (documented in full in `docs/decisions/0004-transform-definition-grammar.md`):
//!
//! ```text
//! TRANSFORM <target>
//! FROM <source>
//! SELECT <expr> AS <field> [, <expr> AS <field> ...]
//! [WHERE <predicate>]
//! ```
//!
//! `FROM <source>` is where a future aggregate (`GROUP BY <cols>`) or
//! cross-join (`JOIN <other> ON <cond>`) key-space clause will slot in;
//! this slice only accepts the 1-1 case (clause absent) and rejects both
//! keywords by name if present. `<expr>` supports column references,
//! numeric and string literals, `+`, `>` (issue #65), and `name(args)`
//! function calls against [`super::registry::FUNCTIONS`] (issue #64);
//! `<predicate>` accepts only the literal `TRUE`.
//!
//! A second, standalone statement form (ADR-0006, issue #24) declares a
//! named relationship rather than a transform:
//!
//! ```text
//! RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
//! ```
//!
//! It's parsed by [`parse_relationship`], a sibling entry point to [`parse`]
//! rather than a case [`parse`] itself dispatches on — see [`parse`]'s doc
//! comment for why. This slice is grammar + AST only: referencing a
//! relationship from a calculated field, cardinality validation, and catalog
//! storage are all separate, later issues.

use super::ast::{Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef};
use super::error::ParseError;
use super::lexer::{Token, lex};
use super::registry::{
    AGGREGATE_FUNCTIONS, NON_IMMUTABLE_NAMES, lookup_aggregate_function, lookup_function,
    lookup_operator,
};

const OPERATOR_CHARS: &[char] = &['+', '-', '*', '/', '%', '>', '<', '='];

/// Parses a transform definition's source text into a [`TransformDef`].
///
/// This entry point is unchanged by issue #24's `RELATIONSHIP` statement: it
/// stays `TransformDef`-typed so existing callers (the catalog, the tests
/// above) don't need to unwrap a statement-kind enum. See
/// [`parse_relationship`] for the sibling entry point that parses the new
/// standalone-relationship grammar; both share the same lexer and error type,
/// and a caller that doesn't yet know which kind of statement it has can
/// peek the first token itself (`RELATIONSHIP` vs. `TRANSFORM`) to choose
/// between them, the same check [`parse_relationship`] makes internally.
pub fn parse(input: &str) -> Result<TransformDef, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        is_aggregate: false,
    }
    .parse_transform_def()
}

/// Parses a standalone relationship declaration's source text into a
/// [`RelationshipDef`] (ADR-0006, issue #24):
///
/// ```text
/// RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
/// ```
pub fn parse_relationship(input: &str) -> Result<RelationshipDef, ParseError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        is_aggregate: false,
    }
    .parse_relationship_def()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Set once [`Self::parse_key_space_clause`] has parsed a `GROUP BY`.
    /// Gates whether [`Self::parse_primary`] will accept a
    /// `SUM`/`MIN`/`MAX`/`AVG` call — a plain field rather than threading
    /// key-space through every expression-parsing method, since it's fixed
    /// for the whole statement by the time fields are parsed.
    is_aggregate: bool,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), ParseError> {
        match self.advance() {
            Token::Ident(s) if s.eq_ignore_ascii_case(kw) => Ok(()),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: format!("'{kw}'"),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: format!("'{kw}'"),
                found: other.describe(),
            }),
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "an identifier".to_string(),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: "an identifier".to_string(),
                found: other.describe(),
            }),
        }
    }

    fn peek_is_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn peek_is_symbol(&self, c: char) -> bool {
        matches!(self.peek(), Token::Symbol(s) if *s == c)
    }

    fn expect_symbol(&mut self, c: char) -> Result<(), ParseError> {
        match self.advance() {
            Token::Symbol(s) if s == c => Ok(()),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: format!("'{c}'"),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: format!("'{c}'"),
                found: other.describe(),
            }),
        }
    }

    /// Parses a dot-qualified `<table>.<col>` reference, the new-relative-to
    /// existing-statement-forms syntax ADR-0006 introduces for a
    /// relationship's endpoints. There's no reuse target in the expression
    /// grammar for this: `parse_primary`'s `a.b` handling parses an
    /// identifier then rejects a following `.` as an
    /// [`ParseError::UnsupportedRelationshipPath`], which is specific to
    /// expression context and not what a relationship declaration's
    /// endpoints should report on malformed input.
    fn expect_table_dot_column(&mut self) -> Result<(String, String), ParseError> {
        let table = self.expect_ident()?;
        self.expect_symbol('.')?;
        let column = self.expect_ident()?;
        Ok((table, column))
    }

    fn parse_transform_def(&mut self) -> Result<TransformDef, ParseError> {
        self.expect_keyword("TRANSFORM")?;
        let target = self.expect_ident()?;

        self.expect_keyword("FROM")?;
        let source = self.expect_ident()?;

        let key_space = self.parse_key_space_clause()?;

        self.expect_keyword("SELECT")?;
        let fields = self.parse_field_list()?;

        let predicate = if self.peek_is_keyword("WHERE") {
            self.advance();
            self.parse_predicate()?
        } else {
            Predicate::True
        };

        self.reject_trailing_key_space_clause()?;

        match self.advance() {
            Token::Eof => {}
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "end of input".to_string(),
                    found: other.describe(),
                });
            }
        }

        Ok(TransformDef {
            target,
            source,
            key_space,
            fields,
            predicate,
        })
    }

    /// Parses `RELATIONSHIP <name> FROM <from_table>.<fk_col> TO
    /// <to_table>.<pk_col>` (ADR-0006, issue #24). `RELATIONSHIP`/`FROM`/`TO`
    /// are matched case-insensitively, matching every other keyword in this
    /// grammar (`expect_keyword`); `name` and the four table/column
    /// components are captured as raw identifiers, exactly like
    /// `parse_transform_def`'s `target`/`source` — validating them as real
    /// tables/columns (cardinality, existence, cycles) is deferred to a
    /// later issue per ADR-0006's own scoping.
    fn parse_relationship_def(&mut self) -> Result<RelationshipDef, ParseError> {
        self.expect_keyword("RELATIONSHIP")?;
        let name = self.expect_ident()?;

        self.expect_keyword("FROM")?;
        let (from_table, from_col) = self.expect_table_dot_column()?;

        self.expect_keyword("TO")?;
        let (to_table, to_col) = self.expect_table_dot_column()?;

        match self.advance() {
            Token::Eof => {}
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "end of input".to_string(),
                    found: other.describe(),
                });
            }
        }

        Ok(RelationshipDef {
            name,
            from_table,
            from_col,
            to_table,
            to_col,
        })
    }

    /// Parses the key-space clause in its ADR-0004-reserved slot, between
    /// `FROM <source>` and `SELECT`: absent -> [`KeySpace::OneToOne`],
    /// `GROUP BY <col>[, <col>...]` -> [`KeySpace::Aggregate`]. `JOIN` is
    /// rejected outright — cross-join key-spaces aren't supported by any
    /// part of this grammar yet.
    fn parse_key_space_clause(&mut self) -> Result<KeySpace, ParseError> {
        if self.peek_is_keyword("JOIN") {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "JOIN".to_string(),
                detail: "cross-join key-spaces are not supported by this grammar slice \
                    (see issue #22 and docs/transforms.md#granularity)"
                    .to_string(),
            });
        }
        if self.peek_is_keyword("GROUP") {
            self.advance();
            self.expect_keyword("BY")?;
            let mut group_by = vec![self.expect_ident()?];
            while self.peek_is_symbol(',') {
                self.advance();
                group_by.push(self.expect_ident()?);
            }
            self.is_aggregate = true;
            return Ok(KeySpace::Aggregate { group_by });
        }
        Ok(KeySpace::OneToOne)
    }

    /// Rejects a `JOIN`/`GROUP BY` key-space clause with a construct-specific
    /// error if one starts at the current position — called at the end of
    /// the statement, since SQL-natural placement (`SELECT ... GROUP BY
    /// ...`) would otherwise fall through to a generic "expected end of
    /// input" error rather than naming ADR-0004's actual reserved slot.
    fn reject_trailing_key_space_clause(&self) -> Result<(), ParseError> {
        if self.peek_is_keyword("JOIN") {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "JOIN".to_string(),
                detail: "cross-join key-spaces are not supported by this grammar slice \
                    (see issue #22 and docs/transforms.md#granularity)"
                    .to_string(),
            });
        }
        if self.peek_is_keyword("GROUP") {
            return Err(ParseError::UnsupportedKeySpace {
                construct: "GROUP BY".to_string(),
                detail: "GROUP BY must appear directly after FROM <source> and before \
                    SELECT (ADR-0004's reserved slot), not after SELECT/WHERE"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn parse_field_list(&mut self) -> Result<Vec<FieldDef>, ParseError> {
        let mut fields = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            self.expect_keyword("AS")?;
            let name = self.expect_ident()?;
            fields.push(FieldDef { name, expr });

            if self.peek_is_symbol(',') {
                self.advance();
                continue;
            }
            break;
        }
        Ok(fields)
    }

    /// Parses a binary-operator expression flat and left-associative: there
    /// is no precedence table, so `a OP1 b OP2 c` always builds
    /// `(a OP1 b) OP2 c`. See the precedence-invariant doc-comment on
    /// [`super::registry::OPERATORS`] (issue #67) before adding an operator
    /// — this is currently safe only because the type lattice happens to
    /// reject every regrouping that would otherwise silently diverge from
    /// Postgres's real precedence.
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_primary()?;
        loop {
            let symbol = match self.peek() {
                Token::Symbol(c) if OPERATOR_CHARS.contains(c) => *c,
                _ => break,
            };
            self.advance();
            let symbol_str = symbol.to_string();
            let op = lookup_operator(&symbol_str).ok_or(ParseError::UnsupportedOperator {
                operator: symbol_str,
            })?;
            let rhs = self.parse_primary()?;
            lhs = Expr::BinaryOp {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.advance() {
            Token::Number(n) => Ok(Expr::NumberLiteral(n)),
            Token::String(s) => Ok(Expr::StringLiteral(s)),
            Token::Ident(name) => {
                let upper = name.to_ascii_uppercase();

                if NON_IMMUTABLE_NAMES.contains(&upper.as_str()) {
                    return Err(ParseError::NonImmutableConstruct { name });
                }

                if self.peek_is_symbol('.') {
                    self.advance();
                    let field = self.expect_ident()?;
                    return Err(ParseError::UnsupportedRelationshipPath {
                        path: format!("{name}.{field}"),
                    });
                }

                if self.peek_is_symbol('(') {
                    self.advance();

                    if AGGREGATE_FUNCTIONS.contains(&upper.as_str()) {
                        if !self.is_aggregate {
                            self.skip_balanced_parens()?;
                            return Err(ParseError::UnsupportedKeySpace {
                                construct: format!("{name}(...)"),
                                detail: "aggregate functions require an aggregate key-space \
                                    (GROUP BY), which this grammar slice does not support"
                                    .to_string(),
                            });
                        }

                        if upper == "COUNT" {
                            // `COUNT(*)` (row-counting) is the only supported
                            // shape — `COUNT(<column>)` is not a plain
                            // `parse_call_args` expression, so it's parsed
                            // directly here rather than through
                            // `AGGREGATE_FUNCTION_SPECS`'s expression-argument
                            // machinery.
                            if self.peek_is_symbol('*') {
                                self.advance();
                                match self.advance() {
                                    Token::Symbol(')') => {}
                                    Token::Eof => {
                                        return Err(ParseError::UnexpectedEof {
                                            expected: "')'".to_string(),
                                        });
                                    }
                                    other => {
                                        return Err(ParseError::UnexpectedToken {
                                            expected: "')'".to_string(),
                                            found: other.describe(),
                                        });
                                    }
                                }
                                return Ok(Expr::FunctionCall {
                                    name: upper,
                                    args: Vec::new(),
                                });
                            }
                            self.skip_balanced_parens()?;
                            return Err(ParseError::UnsupportedAggregateFunction { name: upper });
                        }

                        let spec = lookup_aggregate_function(&upper)
                            .expect("every non-COUNT AGGREGATE_FUNCTIONS name is in AGGREGATE_FUNCTION_SPECS");
                        let args = self.parse_call_args()?;
                        if args.len() != spec.arg_types.len() {
                            return Err(ParseError::FunctionArityMismatch {
                                name,
                                expected: spec.arg_types.len(),
                                found: args.len(),
                            });
                        }
                        return Ok(Expr::FunctionCall { name: upper, args });
                    }

                    let spec = match lookup_function(&upper) {
                        Some(spec) => spec,
                        None => {
                            self.skip_balanced_parens()?;
                            return Err(ParseError::UnsupportedFunction { name });
                        }
                    };

                    let args = self.parse_call_args()?;
                    if args.len() != spec.arg_types.len() {
                        return Err(ParseError::FunctionArityMismatch {
                            name,
                            expected: spec.arg_types.len(),
                            found: args.len(),
                        });
                    }

                    return Ok(Expr::FunctionCall { name: upper, args });
                }

                Ok(Expr::Column(name))
            }
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "an expression".to_string(),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: "an expression".to_string(),
                found: other.describe(),
            }),
        }
    }

    /// Parses a call's comma-separated argument list up to and including the
    /// `)` matching a `(` already consumed by the caller. General
    /// expression syntax (issue #64): each argument is a full `parse_expr`,
    /// so calls can nest.
    fn parse_call_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        if self.peek_is_symbol(')') {
            self.advance();
            return Ok(args);
        }
        loop {
            args.push(self.parse_expr()?);
            if self.peek_is_symbol(',') {
                self.advance();
                continue;
            }
            break;
        }
        match self.advance() {
            Token::Symbol(')') => Ok(args),
            Token::Eof => Err(ParseError::UnexpectedEof {
                expected: "')'".to_string(),
            }),
            other => Err(ParseError::UnexpectedToken {
                expected: "')'".to_string(),
                found: other.describe(),
            }),
        }
    }

    /// Consumes tokens up to and including the `)` matching a `(` already
    /// consumed by the caller, so a rejected function call doesn't need its
    /// argument list to itself be valid expression syntax.
    fn skip_balanced_parens(&mut self) -> Result<(), ParseError> {
        let mut depth = 1;
        loop {
            match self.advance() {
                Token::Symbol('(') => depth += 1,
                Token::Symbol(')') => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Token::Eof => {
                    return Err(ParseError::UnexpectedEof {
                        expected: "')'".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    fn parse_predicate(&mut self) -> Result<Predicate, ParseError> {
        match self.advance() {
            Token::Ident(s) if s.eq_ignore_ascii_case("true") => Ok(Predicate::True),
            other => Err(ParseError::UnsupportedPredicate {
                detail: format!(
                    "only a literal TRUE is accepted (general predicates are deferred; \
                        see docs/open-questions.md); found {}",
                    other.describe()
                ),
            }),
        }
    }
}
