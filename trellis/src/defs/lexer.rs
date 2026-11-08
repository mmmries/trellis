//! Hand-rolled lexer for the transform-definition grammar. The language is
//! small (keywords, identifiers, numeric literals, a handful of symbols) so
//! a lookup-table lexer + recursive-descent parser is simplest; no
//! parser-combinator crate is needed.

use super::error::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// An unquoted identifier or keyword. Under [`Identifiers::Folded`] it
    /// is folded to lower case, as Postgres folds it.
    Ident(String),
    /// A double-quoted identifier's name, exactly as written (`""` resolved
    /// to `"`). Never a keyword or a reserved word (ADR-0022).
    QuotedIdent(String),
    Number(String),
    /// A single-quoted string literal's decoded contents (quotes stripped,
    /// `''` escapes resolved to a literal `'`), per SQL's spelling.
    String(String),
    Symbol(char),
    /// A run of two or more adjacent operator characters, lexed as Postgres
    /// lexes it (see [`lex`]): `<=`, `>=`, `<>`, `!=`, or a run this grammar
    /// has no operator for (`<=>`, `!=-`, `%-`), which the parser refuses by
    /// name.
    Operator(String),
    Eof,
}

impl Token {
    pub fn describe(&self) -> String {
        match self {
            Token::Ident(s) => format!("identifier '{s}'"),
            Token::QuotedIdent(s) => format!("quoted identifier \"{}\"", s.replace('"', "\"\"")),
            Token::Number(s) => format!("number '{s}'"),
            Token::String(s) => format!("string '{s}'"),
            Token::Symbol(c) => format!("'{c}'"),
            Token::Operator(s) => format!("'{s}'"),
            Token::Eof => "end of input".to_string(),
        }
    }
}

/// How a grammar version reads identifiers (ADR-0018, ADR-0022).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Identifiers {
    /// Grammar 1: an unquoted identifier keeps its case, as if every name
    /// were quoted, and there are no quoted identifiers. Only stored text
    /// written under grammar 1 is read this way.
    Exact,
    /// Grammar 2 and later, as Postgres reads them: an unquoted identifier
    /// folds to lower case, a quoted one is exact, and a name over
    /// [`MAX_NAME_BYTES`] is refused.
    Folded,
}

/// The longest name Postgres keeps, in bytes (`NAMEDATALEN - 1`). Postgres
/// truncates a longer one; Trellis refuses it, so two long names that share
/// a prefix never become one.
pub const MAX_NAME_BYTES: usize = 63;

/// Refuses `name` if it is longer than [`MAX_NAME_BYTES`].
fn check_name_length(name: &str) -> Result<(), ParseError> {
    if name.len() <= MAX_NAME_BYTES {
        return Ok(());
    }
    let mut end = MAX_NAME_BYTES;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    Err(ParseError::NameTooLong {
        name: name.to_string(),
        truncated: name[..end].to_string(),
    })
}

/// The double-quoted identifier opening at `chars[start]` (a `"`): its name
/// and the index just past its closing quote. `""` inside it is a `"`, as
/// in Postgres. A zero-length name is refused as Postgres refuses it, and
/// so are a `.` (Trellis addresses a table by its `<schema>.<table>` text,
/// which such a name would make ambiguous) and a non-ASCII character, which
/// no unquoted name can hold either.
fn scan_quoted_ident(chars: &[char], start: usize) -> Result<(String, usize), ParseError> {
    let mut name = String::new();
    let mut i = start + 1;
    loop {
        match chars.get(i) {
            Some('"') if chars.get(i + 1) == Some(&'"') => {
                name.push('"');
                i += 2;
            }
            Some('"') => {
                i += 1;
                break;
            }
            Some(ch) => {
                name.push(*ch);
                i += 1;
            }
            None => return Err(ParseError::UnterminatedQuotedIdentifier),
        }
    }
    if name.is_empty() {
        return Err(ParseError::ZeroLengthQuotedIdentifier);
    }
    let refuse = |detail: &str| {
        Err(ParseError::UnsupportedQuotedIdentifier {
            name: name.clone(),
            detail: detail.to_string(),
        })
    };
    if name.contains('.') {
        return refuse(
            "a name holding a '.' is not supported, since Trellis addresses a table as \
             '<schema>.<table>'; rename it",
        );
    }
    if !name.is_ascii() || name.contains('\0') {
        return refuse("a name may hold only ASCII characters other than NUL");
    }
    Ok((name, i))
}

const SYMBOLS: &[char] = &[
    '+', '-', '*', '/', '%', '=', ',', '(', ')', '.', '<', '>', '!',
];

/// The characters Postgres lexes into operators (`scan.l`'s `op_chars`).
const OP_CHARS: &str = "~!@#^&|`?+-*/%<>=";

/// Operator characters that let a run end in `+` or `-`.
const OP_RUN_KEEPS_SIGN: &str = "~!@#^&|`?%";

/// The length of the operator run starting at `chars[0]`, by Postgres's
/// rules: the longest stretch of operator characters that stops before a
/// `--` or `/*` comment start, and, unless it holds one of
/// `~ ! @ # ^ & | ` ? %`, gives back its trailing `+` and `-` (down to one
/// character) so `a<-1` is `a < -1` but `a!=-1` is the single operator `!=-`.
fn operator_run_len(chars: &[char]) -> usize {
    let mut n = 0;
    while let Some(&c) = chars.get(n) {
        if !OP_CHARS.contains(c) {
            break;
        }
        let next = chars.get(n + 1);
        if n > 0 && ((c == '-' && next == Some(&'-')) || (c == '/' && next == Some(&'*'))) {
            break;
        }
        n += 1;
    }
    if n > 1
        && matches!(chars[n - 1], '+' | '-')
        && !chars[..n].iter().any(|c| OP_RUN_KEEPS_SIGN.contains(*c))
    {
        while n > 1 && matches!(chars[n - 1], '+' | '-') {
            n -= 1;
        }
    }
    n
}

/// Identifier-start characters as Postgres's `ident_start` has them: a
/// letter, `_`, or any non-ASCII character (its high-bit bytes).
fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || !c.is_ascii()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumberShape {
    /// Digits with an optional fraction: the only form this grammar reads.
    Plain,
    /// A form Postgres's lexer reads as one number that this grammar does
    /// not implement: an exponent, a `0x`/`0o`/`0b` integer, or `_` digit
    /// separators.
    Other,
}

/// `D(_?D)*` from `start`: the end of a digit run in `radix`, or `None` if
/// it does not begin with a digit. The second value is whether a `_`
/// separator was used.
fn scan_digits(chars: &[char], start: usize, radix: u32) -> Option<(usize, bool)> {
    let digit = |i: usize| chars.get(i).is_some_and(|c| c.is_digit(radix));
    if !digit(start) {
        return None;
    }
    let (mut i, mut underscore) = (start + 1, false);
    loop {
        if digit(i) {
            i += 1;
        } else if chars.get(i) == Some(&'_') && digit(i + 1) {
            underscore = true;
            i += 2;
        } else {
            return Some((i, underscore));
        }
    }
}

/// The longest numeric literal at `start` (a digit), as Postgres's `scan.l`
/// matches it: a `0x`/`0o`/`0b` integer, or decimal digits with an optional
/// fraction (`1.` and `1.5`, never a leading `.`) and an optional exponent
/// (`e`, an optional sign, digits). A `.` joins the number only before a
/// digit or an identifier character, so `1..2` is the number `1` and two
/// stray dots for the parser.
fn scan_number(chars: &[char], start: usize) -> (usize, NumberShape) {
    if chars[start] == '0'
        && let Some(radix) = chars.get(start + 1).and_then(|c| match c {
            'x' | 'X' => Some(16),
            'o' | 'O' => Some(8),
            'b' | 'B' => Some(2),
            _ => None,
        })
    {
        let from = if chars.get(start + 2) == Some(&'_') {
            start + 3
        } else {
            start + 2
        };
        if let Some((end, _)) = scan_digits(chars, from, radix) {
            return (end, NumberShape::Other);
        }
    }
    let Some((mut end, mut other)) = scan_digits(chars, start, 10) else {
        unreachable!("the caller checked for a digit")
    };
    if chars.get(end) == Some(&'.') {
        if let Some((frac, underscore)) = scan_digits(chars, end + 1, 10) {
            end = frac;
            other |= underscore;
        } else if chars.get(end + 1).is_some_and(|c| is_ident_start(*c)) {
            end += 1; // `1.e3` is a number, `1.a` is junk after `1.`
        }
    }
    if matches!(chars.get(end), Some('e' | 'E')) {
        let sign = usize::from(matches!(chars.get(end + 1), Some('+' | '-')));
        if let Some((exp, _)) = scan_digits(chars, end + 1 + sign, 10) {
            return (exp, NumberShape::Other);
        }
    }
    (
        end,
        if other {
            NumberShape::Other
        } else {
            NumberShape::Plain
        },
    )
}

/// Lexes `input`, reading identifiers as `identifiers` says.
pub(crate) fn lex_in(input: &str, identifiers: Identifiers) -> Result<Vec<Token>, ParseError> {
    let folded = identifiers == Identifiers::Folded;
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if c.is_whitespace() {
            i += 1;
            continue;
        }

        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let mut name: String = chars[start..i].iter().collect();
            if folded {
                // Postgres's Unicode-escape spelling, `U&"d\0061t\+000061"`.
                if name.eq_ignore_ascii_case("u")
                    && chars.get(i) == Some(&'&')
                    && chars.get(i + 1) == Some(&'"')
                {
                    return Err(ParseError::UnicodeEscapeIdentifier);
                }
                name.make_ascii_lowercase();
                check_name_length(&name)?;
            }
            tokens.push(Token::Ident(name));
            continue;
        }

        if c == '"' && folded {
            let (name, end) = scan_quoted_ident(&chars, i)?;
            check_name_length(&name)?;
            tokens.push(Token::QuotedIdent(name));
            i = end;
            continue;
        }

        if c.is_ascii_digit() {
            let (end, shape) = scan_number(&chars, i);
            let number: String = chars[i..end].iter().collect();
            // Postgres (15+) refuses a number directly followed by an
            // identifier character ("trailing junk after numeric literal"),
            // so `1AND`, `1e3AND` and `1.a` are refused, not read as a number
            // and a word.
            if chars.get(end).is_some_and(|ch| is_ident_start(*ch)) {
                let mut stop = end;
                while stop < chars.len()
                    && (is_ident_start(chars[stop])
                        || chars[stop].is_ascii_digit()
                        || chars[stop] == '$')
                {
                    stop += 1;
                }
                return Err(ParseError::NumericTrailingJunk {
                    number,
                    junk: chars[end..stop].iter().collect(),
                });
            }
            if shape != NumberShape::Plain {
                return Err(ParseError::UnsupportedNumericForm { text: number });
            }
            i = end;
            tokens.push(Token::Number(number));
            continue;
        }

        if c == '\'' {
            let mut text = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                        text.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        text.push(*ch);
                        i += 1;
                    }
                    None => return Err(ParseError::UnterminatedString),
                }
            }
            tokens.push(Token::String(text));
            continue;
        }

        // Postgres's `<expr>::<type>` cast sugar. Caught here,
        // at the character the user actually typed, so it gets a message
        // naming the two spellings this grammar *does* accept rather than
        // the lexer's generic "unexpected character ':'". See
        // `super::typed_literal` for why `::` isn't one of them.
        if c == ':' {
            return Err(ParseError::UnsupportedCastOperator);
        }

        // In SQL `--` starts a comment, so `a--1` is `a`. This grammar has
        // no comments, and reading the two characters as two minuses would
        // give the same text another meaning (`a - (-1)`), so the pair is
        // refused by name.
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            return Err(ParseError::DoubleMinus);
        }

        // Operators are lexed as runs, exactly as Postgres does, so a
        // two-character comparison exists only when its characters are
        // adjacent (`a<=b`, never `a< =b`) and `a!=-1` is one operator.
        if OP_CHARS.contains(c) {
            let n = operator_run_len(&chars[i..]);
            if n > 1 {
                tokens.push(Token::Operator(chars[i..i + n].iter().collect()));
                i += n;
                continue;
            }
        }

        if SYMBOLS.contains(&c) {
            tokens.push(Token::Symbol(c));
            i += 1;
            continue;
        }

        return Err(ParseError::UnexpectedChar { found: c });
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}
