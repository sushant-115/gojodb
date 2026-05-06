// Package parser implements a simple query language (GQL) for GojoDB.
//
// Supported statements:
//   SELECT <cols> FROM <index> [WHERE pred] [ORDER BY field [ASC|DESC]] [LIMIT n]
//   INSERT INTO <index> (key, value) VALUES ('k', 'v')
//   DELETE FROM <index> WHERE key = 'k'
//   UPDATE <index> SET value = 'v' WHERE key = 'k'
//
// Predicates:
//   key = 'x'
//   key BETWEEN 'a' AND 'b'
//   CONTAINS(field, 'text')  -- full-text / inverted-index search
//   NEAR(lat, lon, radiusMetres)  -- spatial proximity
//   WITHIN(minLat, minLon, maxLat, maxLon)  -- bounding-box
package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// -----------------------------------------------------------------------
// Token types
// -----------------------------------------------------------------------

// TokenType categorises a lexical token.
type TokenType int

const (
	// Literals
	TOKEN_IDENT  TokenType = iota // unquoted identifier
	TOKEN_STRING                  // single-quoted string literal
	TOKEN_NUMBER                  // integer or float literal

	// Keywords
	TOKEN_SELECT
	TOKEN_INSERT
	TOKEN_INTO
	TOKEN_UPDATE
	TOKEN_SET
	TOKEN_DELETE
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_AND
	TOKEN_OR
	TOKEN_NOT
	TOKEN_BETWEEN
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_ASC
	TOKEN_DESC
	TOKEN_LIMIT
	TOKEN_VALUES
	TOKEN_CONTAINS // full-text predicate
	TOKEN_NEAR     // spatial proximity
	TOKEN_WITHIN   // spatial bounding box

	// Operators / punctuation
	TOKEN_EQ    // =
	TOKEN_NEQ   // !=  <>
	TOKEN_LT    // <
	TOKEN_LTE   // <=
	TOKEN_GT    // >
	TOKEN_GTE   // >=
	TOKEN_COMMA // ,
	TOKEN_LPAREN
	TOKEN_RPAREN
	TOKEN_STAR // *
	TOKEN_EOF
	TOKEN_ILLEGAL
)

var keywords = map[string]TokenType{
	"SELECT":   TOKEN_SELECT,
	"INSERT":   TOKEN_INSERT,
	"INTO":     TOKEN_INTO,
	"UPDATE":   TOKEN_UPDATE,
	"SET":      TOKEN_SET,
	"DELETE":   TOKEN_DELETE,
	"FROM":     TOKEN_FROM,
	"WHERE":    TOKEN_WHERE,
	"AND":      TOKEN_AND,
	"OR":       TOKEN_OR,
	"NOT":      TOKEN_NOT,
	"BETWEEN":  TOKEN_BETWEEN,
	"ORDER":    TOKEN_ORDER,
	"BY":       TOKEN_BY,
	"ASC":      TOKEN_ASC,
	"DESC":     TOKEN_DESC,
	"LIMIT":    TOKEN_LIMIT,
	"VALUES":   TOKEN_VALUES,
	"CONTAINS": TOKEN_CONTAINS,
	"NEAR":     TOKEN_NEAR,
	"WITHIN":   TOKEN_WITHIN,
}

// Token is a single lexical unit.
type Token struct {
	Type    TokenType
	Literal string // raw text
	Pos     int    // byte offset in source
}

func (t Token) String() string {
	return fmt.Sprintf("Token{%d, %q}", t.Type, t.Literal)
}

// -----------------------------------------------------------------------
// Lexer
// -----------------------------------------------------------------------

// Lexer turns a query string into a stream of Tokens.
type Lexer struct {
	input []rune
	pos   int
}

// NewLexer creates a Lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{input: []rune(input)}
}

func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) advance() rune {
	ch := l.input[l.pos]
	l.pos++
	return ch
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.pos++
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{Type: TOKEN_EOF, Pos: l.pos}
	}
	start := l.pos
	ch := l.advance()
	switch {
	case ch == '\'':
		return l.readString(start)
	case unicode.IsLetter(ch) || ch == '_':
		return l.readIdent(start)
	case unicode.IsDigit(ch) || (ch == '-' && l.pos < len(l.input) && unicode.IsDigit(l.peek())):
		return l.readNumber(start)
	case ch == '=':
		return Token{Type: TOKEN_EQ, Literal: "=", Pos: start}
	case ch == '!':
		if l.pos < len(l.input) && l.peek() == '=' {
			l.advance()
			return Token{Type: TOKEN_NEQ, Literal: "!=", Pos: start}
		}
		return Token{Type: TOKEN_ILLEGAL, Literal: "!", Pos: start}
	case ch == '<':
		if l.pos < len(l.input) && l.peek() == '=' {
			l.advance()
			return Token{Type: TOKEN_LTE, Literal: "<=", Pos: start}
		}
		if l.pos < len(l.input) && l.peek() == '>' {
			l.advance()
			return Token{Type: TOKEN_NEQ, Literal: "<>", Pos: start}
		}
		return Token{Type: TOKEN_LT, Literal: "<", Pos: start}
	case ch == '>':
		if l.pos < len(l.input) && l.peek() == '=' {
			l.advance()
			return Token{Type: TOKEN_GTE, Literal: ">=", Pos: start}
		}
		return Token{Type: TOKEN_GT, Literal: ">", Pos: start}
	case ch == ',':
		return Token{Type: TOKEN_COMMA, Literal: ",", Pos: start}
	case ch == '(':
		return Token{Type: TOKEN_LPAREN, Literal: "(", Pos: start}
	case ch == ')':
		return Token{Type: TOKEN_RPAREN, Literal: ")", Pos: start}
	case ch == '*':
		return Token{Type: TOKEN_STAR, Literal: "*", Pos: start}
	default:
		return Token{Type: TOKEN_ILLEGAL, Literal: string(ch), Pos: start}
	}
}

func (l *Lexer) readString(start int) Token {
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.advance()
		if ch == '\'' {
			if l.pos < len(l.input) && l.peek() == '\'' {
				// Escaped single-quote '' → single '
				l.advance()
				sb.WriteRune('\'')
				continue
			}
			return Token{Type: TOKEN_STRING, Literal: sb.String(), Pos: start}
		}
		sb.WriteRune(ch)
	}
	return Token{Type: TOKEN_ILLEGAL, Literal: sb.String(), Pos: start} // unterminated
}

func (l *Lexer) readIdent(start int) Token {
	for l.pos < len(l.input) && (unicode.IsLetter(l.input[l.pos]) || unicode.IsDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
		l.pos++
	}
	lit := strings.ToUpper(string(l.input[start:l.pos]))
	if tt, ok := keywords[lit]; ok {
		return Token{Type: tt, Literal: lit, Pos: start}
	}
	return Token{Type: TOKEN_IDENT, Literal: string(l.input[start:l.pos]), Pos: start}
}

func (l *Lexer) readNumber(start int) Token {
	for l.pos < len(l.input) && (unicode.IsDigit(l.input[l.pos]) || l.input[l.pos] == '.') {
		l.pos++
	}
	return Token{Type: TOKEN_NUMBER, Literal: string(l.input[start:l.pos]), Pos: start}
}

// Tokenize returns all tokens (excluding EOF) for the given input.
func Tokenize(input string) []Token {
	l := NewLexer(input)
	var tokens []Token
	for {
		t := l.NextToken()
		if t.Type == TOKEN_EOF {
			break
		}
		tokens = append(tokens, t)
	}
	return tokens
}

// -----------------------------------------------------------------------
// AST node types
// -----------------------------------------------------------------------

// Statement is the top-level AST node.
type Statement interface {
	stmtNode()
}

// SelectStmt represents: SELECT <columns> FROM <index> [WHERE pred] [ORDER BY ...] [LIMIT n]
type SelectStmt struct {
	Columns   []string   // nil / ["*"] means all
	IndexName string
	Where     Predicate // nil = no filter
	OrderBy   *OrderBy
	Limit     int32 // 0 = no limit
}

func (*SelectStmt) stmtNode() {}

// InsertStmt represents: INSERT INTO <index> (key, value) VALUES ('k', 'v')
type InsertStmt struct {
	IndexName string
	Key       string
	Value     string
}

func (*InsertStmt) stmtNode() {}

// DeleteStmt represents: DELETE FROM <index> WHERE key = 'k'
type DeleteStmt struct {
	IndexName string
	Where     Predicate
}

func (*DeleteStmt) stmtNode() {}

// UpdateStmt represents: UPDATE <index> SET value = 'v' WHERE key = 'k'
type UpdateStmt struct {
	IndexName string
	Value     string
	Where     Predicate
}

func (*UpdateStmt) stmtNode() {}

// OrderBy captures the ORDER BY clause.
type OrderBy struct {
	Field string
	Desc  bool
}

// -----------------------------------------------------------------------
// Predicate node types
// -----------------------------------------------------------------------

// Predicate is any expression that yields a boolean.
type Predicate interface {
	predNode()
}

// EqPredicate: key = 'value'
type EqPredicate struct {
	Field string
	Value string
}

func (*EqPredicate) predNode() {}

// RangePredicate: key BETWEEN 'a' AND 'b'
type RangePredicate struct {
	Field    string
	StartKey string
	EndKey   string
}

func (*RangePredicate) predNode() {}

// CompPredicate: field <op> 'value'   (for <, <=, >, >=, !=)
type CompPredicate struct {
	Field string
	Op    TokenType
	Value string
}

func (*CompPredicate) predNode() {}

// TextSearchPredicate: CONTAINS(field, 'query')
type TextSearchPredicate struct {
	Field string
	Query string
}

func (*TextSearchPredicate) predNode() {}

// NearPredicate: NEAR(lat, lon, radiusMetres)
type NearPredicate struct {
	Lat    float64
	Lon    float64
	Radius float64
}

func (*NearPredicate) predNode() {}

// WithinPredicate: WITHIN(minLat, minLon, maxLat, maxLon)
type WithinPredicate struct {
	MinLat, MinLon float64
	MaxLat, MaxLon float64
}

func (*WithinPredicate) predNode() {}

// AndPredicate: left AND right
type AndPredicate struct {
	Left, Right Predicate
}

func (*AndPredicate) predNode() {}

// OrPredicate: left OR right
type OrPredicate struct {
	Left, Right Predicate
}

func (*OrPredicate) predNode() {}

// NotPredicate: NOT pred
type NotPredicate struct {
	Pred Predicate
}

func (*NotPredicate) predNode() {}

// -----------------------------------------------------------------------
// Parser
// -----------------------------------------------------------------------

// Parser implements recursive-descent parsing of GQL statements.
type Parser struct {
	lexer   *Lexer
	current Token
	peeked  Token
	hasPeek bool
}

// NewParser creates a Parser for the given query string.
func NewParser(query string) *Parser {
	p := &Parser{lexer: NewLexer(query)}
	p.current = p.lexer.NextToken()
	return p
}

func (p *Parser) next() Token {
	if p.hasPeek {
		p.hasPeek = false
		p.current = p.peeked
	} else {
		p.current = p.lexer.NextToken()
	}
	return p.current
}

func (p *Parser) peek() Token {
	if !p.hasPeek {
		p.peeked = p.lexer.NextToken()
		p.hasPeek = true
	}
	return p.peeked
}

func (p *Parser) expect(tt TokenType) (Token, error) {
	t := p.current
	if t.Type != tt {
		return t, fmt.Errorf("expected token %d, got %d (%q) at pos %d", tt, t.Type, t.Literal, t.Pos)
	}
	p.next()
	return t, nil
}

func (p *Parser) expectIdent() (Token, error) {
	// Accept both identifiers and keywords used as column/index names.
	t := p.current
	if t.Type != TOKEN_IDENT && t.Type < TOKEN_SELECT {
		return t, fmt.Errorf("expected identifier, got %d (%q) at pos %d", t.Type, t.Literal, t.Pos)
	}
	p.next()
	return t, nil
}

// Parse parses a single statement from the input.
func (p *Parser) Parse() (Statement, error) {
	switch p.current.Type {
	case TOKEN_SELECT:
		return p.parseSelect()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_DELETE:
		return p.parseDelete()
	case TOKEN_UPDATE:
		return p.parseUpdate()
	default:
		return nil, fmt.Errorf("unexpected token %q at pos %d", p.current.Literal, p.current.Pos)
	}
}

// Parse parses a query string into a Statement.
func Parse(query string) (Statement, error) {
	return NewParser(strings.TrimSpace(query)).Parse()
}

// -----------------------------------------------------------------------
// Statement parsers
// -----------------------------------------------------------------------

func (p *Parser) parseSelect() (*SelectStmt, error) {
	p.next() // consume SELECT

	// Column list
	cols, err := p.parseColumnList()
	if err != nil {
		return nil, err
	}

	// FROM <index>
	if _, err = p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}
	indexToken, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	stmt := &SelectStmt{Columns: cols, IndexName: indexToken.Literal}

	// Optional WHERE
	if p.current.Type == TOKEN_WHERE {
		p.next()
		pred, predErr := p.parsePredicate()
		if predErr != nil {
			return nil, predErr
		}
		stmt.Where = pred
	}

	// Optional ORDER BY
	if p.current.Type == TOKEN_ORDER {
		p.next()
		if _, err = p.expect(TOKEN_BY); err != nil {
			return nil, err
		}
		fieldTok, fieldErr := p.expectIdent()
		if fieldErr != nil {
			return nil, fieldErr
		}
		ob := &OrderBy{Field: fieldTok.Literal}
		if p.current.Type == TOKEN_DESC {
			ob.Desc = true
			p.next()
		} else if p.current.Type == TOKEN_ASC {
			p.next()
		}
		stmt.OrderBy = ob
	}

	// Optional LIMIT
	if p.current.Type == TOKEN_LIMIT {
		p.next()
		numTok, numErr := p.expect(TOKEN_NUMBER)
		if numErr != nil {
			return nil, numErr
		}
		n, parseErr := strconv.ParseInt(numTok.Literal, 10, 32)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %s", numTok.Literal)
		}
		stmt.Limit = int32(n)
	}

	return stmt, nil
}

func (p *Parser) parseColumnList() ([]string, error) {
	if p.current.Type == TOKEN_STAR {
		p.next()
		return []string{"*"}, nil
	}
	var cols []string
	for {
		tok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, tok.Literal)
		if p.current.Type != TOKEN_COMMA {
			break
		}
		p.next() // consume comma
	}
	return cols, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	p.next() // consume INSERT
	if _, err := p.expect(TOKEN_INTO); err != nil {
		return nil, err
	}
	indexTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	// (key, value)
	if _, err = p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_IDENT); err != nil { // 'key' column label
		return nil, err
	}
	if _, err = p.expect(TOKEN_COMMA); err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_IDENT); err != nil { // 'value' column label
		return nil, err
	}
	if _, err = p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	// VALUES
	if _, err = p.expect(TOKEN_VALUES); err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	keyTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_COMMA); err != nil {
		return nil, err
	}
	valTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return &InsertStmt{
		IndexName: indexTok.Literal,
		Key:       keyTok.Literal,
		Value:     valTok.Literal,
	}, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	p.next() // consume DELETE
	if _, err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}
	indexTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	stmt := &DeleteStmt{IndexName: indexTok.Literal}

	if p.current.Type == TOKEN_WHERE {
		p.next()
		pred, predErr := p.parsePredicate()
		if predErr != nil {
			return nil, predErr
		}
		stmt.Where = pred
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	p.next() // consume UPDATE
	indexTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	// SET value = '...'
	if _, err = p.expect(TOKEN_SET); err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_IDENT); err != nil { // column name
		return nil, err
	}
	if _, err = p.expect(TOKEN_EQ); err != nil {
		return nil, err
	}
	valTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}

	stmt := &UpdateStmt{IndexName: indexTok.Literal, Value: valTok.Literal}

	if p.current.Type == TOKEN_WHERE {
		p.next()
		pred, predErr := p.parsePredicate()
		if predErr != nil {
			return nil, predErr
		}
		stmt.Where = pred
	}

	return stmt, nil
}

// -----------------------------------------------------------------------
// Predicate parsing  (precedence: OR < AND < NOT < atom)
// -----------------------------------------------------------------------

func (p *Parser) parsePredicate() (Predicate, error) {
	return p.parseOrPred()
}

func (p *Parser) parseOrPred() (Predicate, error) {
	left, err := p.parseAndPred()
	if err != nil {
		return nil, err
	}
	for p.current.Type == TOKEN_OR {
		p.next()
		right, rightErr := p.parseAndPred()
		if rightErr != nil {
			return nil, rightErr
		}
		left = &OrPredicate{Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAndPred() (Predicate, error) {
	left, err := p.parseNotPred()
	if err != nil {
		return nil, err
	}
	for p.current.Type == TOKEN_AND {
		p.next()
		right, rightErr := p.parseNotPred()
		if rightErr != nil {
			return nil, rightErr
		}
		left = &AndPredicate{Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseNotPred() (Predicate, error) {
	if p.current.Type == TOKEN_NOT {
		p.next()
		pred, err := p.parseAtomPred()
		if err != nil {
			return nil, err
		}
		return &NotPredicate{Pred: pred}, nil
	}
	return p.parseAtomPred()
}

func (p *Parser) parseAtomPred() (Predicate, error) {
	// Grouped predicate
	if p.current.Type == TOKEN_LPAREN {
		p.next()
		pred, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		if _, err = p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
		return pred, nil
	}

	// CONTAINS(field, 'query')
	if p.current.Type == TOKEN_CONTAINS {
		return p.parseContains()
	}
	// NEAR(lat, lon, radius)
	if p.current.Type == TOKEN_NEAR {
		return p.parseNear()
	}
	// WITHIN(minLat, minLon, maxLat, maxLon)
	if p.current.Type == TOKEN_WITHIN {
		return p.parseWithin()
	}

	// field <op> value  or  field BETWEEN a AND b
	fieldTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	switch p.current.Type {
	case TOKEN_BETWEEN:
		p.next()
		startTok, startErr := p.expect(TOKEN_STRING)
		if startErr != nil {
			return nil, startErr
		}
		if _, err = p.expect(TOKEN_AND); err != nil {
			return nil, err
		}
		endTok, endErr := p.expect(TOKEN_STRING)
		if endErr != nil {
			return nil, endErr
		}
		return &RangePredicate{
			Field:    fieldTok.Literal,
			StartKey: startTok.Literal,
			EndKey:   endTok.Literal,
		}, nil

	case TOKEN_EQ:
		p.next()
		valTok, valErr := p.expectStringOrIdent()
		if valErr != nil {
			return nil, valErr
		}
		return &EqPredicate{Field: fieldTok.Literal, Value: valTok.Literal}, nil

	case TOKEN_NEQ, TOKEN_LT, TOKEN_LTE, TOKEN_GT, TOKEN_GTE:
		op := p.current.Type
		p.next()
		valTok, valErr := p.expectStringOrIdent()
		if valErr != nil {
			return nil, valErr
		}
		return &CompPredicate{Field: fieldTok.Literal, Op: op, Value: valTok.Literal}, nil
	}

	return nil, fmt.Errorf("unexpected token %q after field name at pos %d", p.current.Literal, p.current.Pos)
}

func (p *Parser) expectStringOrIdent() (Token, error) {
	t := p.current
	if t.Type == TOKEN_STRING || t.Type == TOKEN_IDENT || t.Type == TOKEN_NUMBER {
		p.next()
		return t, nil
	}
	return t, fmt.Errorf("expected string or identifier, got %q at pos %d", t.Literal, t.Pos)
}

func (p *Parser) parseContains() (*TextSearchPredicate, error) {
	p.next() // consume CONTAINS
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	fieldTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_COMMA); err != nil {
		return nil, err
	}
	queryTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err = p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &TextSearchPredicate{Field: fieldTok.Literal, Query: queryTok.Literal}, nil
}

func (p *Parser) parseNear() (*NearPredicate, error) {
	p.next() // consume NEAR
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	lat, err := p.parseFloat()
	if err != nil {
		return nil, fmt.Errorf("NEAR: lat: %w", err)
	}
	if _, err = p.expect(TOKEN_COMMA); err != nil {
		return nil, err
	}
	lon, err := p.parseFloat()
	if err != nil {
		return nil, fmt.Errorf("NEAR: lon: %w", err)
	}
	if _, err = p.expect(TOKEN_COMMA); err != nil {
		return nil, err
	}
	radius, err := p.parseFloat()
	if err != nil {
		return nil, fmt.Errorf("NEAR: radius: %w", err)
	}
	if _, err = p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &NearPredicate{Lat: lat, Lon: lon, Radius: radius}, nil
}

func (p *Parser) parseWithin() (*WithinPredicate, error) {
	p.next() // consume WITHIN
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	minLat, err := p.parseFloat()
	if err != nil {
		return nil, err
	}
	coords := [4]float64{minLat}
	for i := 1; i < 4; i++ {
		if _, err = p.expect(TOKEN_COMMA); err != nil {
			return nil, err
		}
		coords[i], err = p.parseFloat()
		if err != nil {
			return nil, err
		}
	}
	if _, err = p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &WithinPredicate{
		MinLat: coords[0], MinLon: coords[1],
		MaxLat: coords[2], MaxLon: coords[3],
	}, nil
}

func (p *Parser) parseFloat() (float64, error) {
	tok := p.current
	if tok.Type != TOKEN_NUMBER {
		return 0, fmt.Errorf("expected number, got %q at pos %d", tok.Literal, tok.Pos)
	}
	p.next()
	return strconv.ParseFloat(tok.Literal, 64)
}
