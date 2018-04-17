package elseql

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"text/scanner"
)

/* SELECT a,b,c FACETS d,e,f FROM t WHERE expr FILTER expr ORDER BY g,h,i LIMIT n,m */

const (
	DEBUG = true

	id_sep     = '.'
	list_sep   = ','
	all_fields = '*'

	//
	// reserved keywords
	//
	SELECT Keyword = iota
	FACETS
	SCRIPT
	FROM
	WHERE
	FILTER
	EXIST
	MISSING
	ORDER
	BY
	LIMIT
	ASC
	DESC
	AND
	OR
	NOT
	IN
	BETWEEN

	NO_KEYWORD Keyword = -1

	//
	// operators
	//
	EQ Operator = iota
	NE
	LT
	LTE
	GT
	GTE
	OP_AND
	OP_OR
	OP_NOT
	STRING_EXPR
	EXISTS_EXPR
	MISSING_EXPR

	NO_OPERATOR Operator = -1
)

type Keyword int

func (k Keyword) String() string {
	return keywordToString[k]
}

func (k Keyword) Lower() string {
	return strings.ToLower(keywordToString[k])
}

func FindKeyword(ks string) (k Keyword, ok bool) {
	k, ok = stringToKeyword[strings.ToUpper(ks)]
	return
}

type Operator int

func (op Operator) String() string {
	return opToString[op]
}

var (
	stringToKeyword = map[string]Keyword{
		"SELECT":  SELECT,
		"FACETS":  FACETS,
		"SCRIPT":  SCRIPT,
		"FROM":    FROM,
		"WHERE":   WHERE,
		"FILTER":  FILTER,
		"EXIST":   EXIST,
		"MISSING": MISSING,
		"ORDER":   ORDER,
		"BY":      BY,
		"LIMIT":   LIMIT,
		"ASC":     ASC,
		"DESC":    DESC,
		"AND":     AND,
		"OR":      OR,
		"NOT":     NOT,
		"IN":      IN,
		"BETWEEN": BETWEEN,
	}

	keywordToString = map[Keyword]string{
		SELECT:  "SELECT",
		FACETS:  "FACETS",
		SCRIPT:  "SCRIPT",
		FROM:    "FROM",
		WHERE:   "WHERE",
		FILTER:  "FILTER",
		EXIST:   "EXIST",
		MISSING: "MISSING",
		ORDER:   "ORDER",
		BY:      "BY",
		LIMIT:   "LIMIT",
		ASC:     "ASC",
		DESC:    "DESC",
		AND:     "AND",
		OR:      "OR",
		NOT:     "NOT",
		IN:      "IN",
		BETWEEN: "BETWEEN",
	}

	opToString = map[Operator]string{
		EQ:           "=",
		NE:           "!=",
		LT:           "<",
		LTE:          "<=",
		GT:           ">",
		GTE:          ">=",
		OP_AND:       "AND",
		OP_OR:        "OR",
		OP_NOT:       "NOT",
		STRING_EXPR:  "\"\"",
		EXISTS_EXPR:  "EXIST",
		MISSING_EXPR: "MISSING",
	}
)

type NameValue struct {
	Name  string
	Value interface{}
}

func (nv NameValue) String() string {
	return fmt.Sprintf("{%v: %v}", nv.Name, nv.Value)
}

func (nv NameValue) Strings() (n, v string) {
	n = nv.Name
	if s, ok := nv.Value.(string); ok {
		v = fmt.Sprintf("%q", s)
	} else {
		v = fmt.Sprintf("%v", nv.Value)
	}

	return
}

/*
 * This is the output of a parsed statement
 */
type Query struct {
	SelectList []string
	FacetList  []string

	Index      string
	WhereExpr  *Expression
	FilterExpr *Expression

	Script    *NameValue
	OrderList []NameValue

	From int
	Size int
}

func (q *Query) String() string {
	return fmt.Sprintf(`Select %v
    Facet %v
    Index %v
    Where %v
    Filter %v
    Script %v
    Order %v
    From %v
    Size %v`, q.SelectList,
		q.FacetList,
		q.Index,
		q.WhereExpr,
		q.FilterExpr.QueryString(),
		q.Script,
		q.OrderList,
		q.From, q.Size)
}

type Expression struct {
	op       Operator
	operands []interface{}
}

func newExpression(op Operator) *Expression {
	return &Expression{op: op}
}

func (e *Expression) String() string {
	if e == nil {
		return ""
	}

	return fmt.Sprintf("{%q %v}", e.op, e.operands)
}

/*
 * Return a query in Lucene syntax
 */
func (e *Expression) QueryString() string {
	if e == nil {
		return ""
	}

	switch e.op {
	case STRING_EXPR:
		return fmt.Sprintf("%v", e.operands[0])

	case OP_NOT:
		expr := e.operands[0].(Expression)
		return "NOT " + expr.QueryString()

	case EQ:
		nv := e.operands[0].(NameValue)
		return nv.String()

	case NE:
		nv := e.operands[0].(NameValue)
		return "NOT " + nv.String()

	case LT:
		n, v := e.operands[0].(NameValue).Strings()
		return n + ":{* TO " + v + "}"

	case LTE:
		n, v := e.operands[0].(NameValue).Strings()
		return n + ":[* TO " + v + "]"

	case GT:
		n, v := e.operands[0].(NameValue).Strings()
		return n + ":{" + v + " TO *}"

	case GTE:
		n, v := e.operands[0].(NameValue).Strings()
		return n + ":[" + v + " TO *]"

	case OP_AND, OP_OR:
		return e.join(e.op.String())
	}

	return e.String()
}

func (e *Expression) isExistsExpression() bool {
	return e.op == EXISTS_EXPR
}

func (e *Expression) isMissingExpression() bool {
	return e.op == MISSING_EXPR
}

func (e *Expression) join(sep string) string {
	expr := e.operands[0].(Expression)

	ret := e.String()

	for _, op := range e.operands {
		expr = op.(Expression)
		ret += fmt.Sprintf(" %v %v", sep, expr)
	}

	return ret
}

func (e *Expression) addOperand(expr interface{}) *Expression {
	e.operands = append(e.operands, expr)
	return e
}

func (e *Expression) getOperand() interface{} {
	return e.operands[0]
}

func singleOperand(op Operator, expr interface{}) *Expression {
	return newExpression(op).addOperand(expr)
}

func nameValueExpression(op Operator, name string, value interface{}) *Expression {
	return newExpression(op).addOperand(NameValue{name, value})
}

func addExpression(result, current *Expression) *Expression {
	if result == nil {
		return current
	}

	return result.addOperand(current)
}

func addOperatorExpression(op Operator, result, current *Expression) *Expression {
	if result == nil {
		return singleOperand(op, current)
	}

	result.addOperand(current)
	if result.op == op {
		return result
	}

	return singleOperand(op, result)
}

/*
 * Parse error
 */
type ParseError string

func (e ParseError) Error() string {
	return string(e)
}

type ElseParser struct {
	QueryString string // input query string
	query       Query  // output query
	parsed      bool   // already parsed

	scanner   *scanner.Scanner
	lastToken rune
	lastText  string
}

func NewParser(queryString string) *ElseParser {
	p := &ElseParser{
		QueryString: queryString,
		parsed:      false,
		scanner:     &scanner.Scanner{},
	}

	p.scanner.Init(strings.NewReader(p.QueryString))
	return p
}

func (p *ElseParser) nextToken() rune {
	if p.lastText == "" {
		if p.lastToken = p.scanner.Scan(); p.lastToken != scanner.EOF {
			p.lastText = p.scanner.TokenText()
		} else {
			p.lastText = ""
		}
	}

	return p.lastToken
}

func (p *ElseParser) Query() *Query {
	if err := p.Parse(); err == nil {
		return &p.query
	} else {
		return nil
	}
}

/*
 * Parse required keyword
 */
func (p *ElseParser) parseRequired(k Keyword) (err error) {
	_, err = p.parseKeyword(k, false)
	return
}

/*
 * Parse (optional) keyword
 */
func (p *ElseParser) parseKeyword(k Keyword, optional bool) (bool, error) {
	if p.nextToken() == scanner.EOF {
		if optional {
			return false, nil
		} else {
			return false, p.parseError(k.String())
		}
	}

	if p.lastToken == scanner.Ident && k.Lower() == strings.ToLower(p.lastText) {
		if DEBUG {
			log.Println("got keyword", k)
		}

		p.lastText = ""
		return true, nil
	}

	if optional {
		return false, nil
	}

	return false, p.parseError(k.String())
}

/*
 * Parse keyword in set (or default)
 */
func (p *ElseParser) parseKeywords(kset []Keyword, kdefault Keyword) Keyword {
	if p.nextToken() == scanner.EOF {
		return kdefault
	}

	if p.lastToken == scanner.Ident {
		k, ok := FindKeyword(p.lastText)
		if !ok {
			return kdefault
		}

		for _, kk := range kset {
			if k == kk {
				p.lastText = ""
				return k
			}
		}
	}

	return kdefault
}

/*
 * Parse boolean value (true or false)
 */
func (p *ElseParser) parseBooleanOperator(odefault Operator) Operator {
	if p.nextToken() == scanner.EOF {
		return odefault
	}

	if p.lastToken == scanner.Ident {
		if k, ok := FindKeyword(p.lastText); ok {
			if k == AND {
				p.lastText = ""
				return OP_AND
			}
			if k == OR {
				p.lastText = ""
				return OP_OR
			}
		}
	}

	return odefault
}

/*
 * Parsing failed, return a meaningful error
 */
func (p *ElseParser) parseError(expected string) error {
	switch p.lastToken {
	case scanner.EOF:
		return ParseError("Expected " + expected + ", got EOL")

	case scanner.Int, scanner.Float:
		return ParseError("Expected " + expected + ", got number " + p.lastText)

	default:
		return ParseError("Expected " + expected + ", got " + p.lastText)
	}
}

/*
 * Parse ID
 */
func (p *ElseParser) parseId() string {
	if p.nextToken() == scanner.EOF {
		return ""
	}

	if p.lastToken == scanner.Ident {
		word := p.lastText

		if DEBUG {
			log.Println("got " + word)
		}

		if _, ok := FindKeyword(word); ok {
			return ""
		}

		p.lastText = ""
		return word
	}

	return ""
}

/*
 * Parse IDENTIFIER ( id.id... )
 */
func (p *ElseParser) parseIdentifier() (string, error) {
	nv, err := p.parseOrderIdentifier(false)
	return nv.Name, err
}

/*
* Parse IDENTIFIER ( id.id... ) with optional sort order
 */
func (p *ElseParser) parseOrderIdentifier(sortorder bool) (NameValue, error) {
	state := 0 // 0: id, 1: sep, 2: sort
	ident := ""
	order := ""

	for {
		//
		// expecting ID
		//
		if state == 0 {
			if word := p.parseId(); word != "" {
				ident += word
				state = 1
			}
		}

		//
		// expecting SEPARATOR
		//
		if state == 1 {
			match, err := p.parseToken(id_sep, true)
			if err != nil {
				log.Println(err)
				return NameValue{}, err
			}

			if match {
				ident += string(id_sep)
				state = 0
				continue
			}

			if sortorder {
				state = 2
			}
		}

		//
		// expect sortorder
		//
		if state == 2 {
			order = p.parseKeywords([]Keyword{ASC, DESC}, ASC).String()
		}

		break
	}

	if len(ident) > 0 {
		return NameValue{ident, order}, nil
	}

	return NameValue{}, p.parseError("identifier")
}

/*
 * Parse (comma separated) list of IDENTIFIERS
 */
func (p *ElseParser) parseIdentifiers() ([]string, error) {
	var result []string

	for {
		id, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		result = append(result, id)

		if match, _ := p.parseToken(list_sep, true); match == false {
			break
		}
	}

	return result, nil
}

/*
 * Parse (comma separated) list of IDENTIFIERS (for sort/order by)
 */
func (p *ElseParser) parseOrderIdentifiers() ([]NameValue, error) {
	var result []NameValue

	for {
		nv, err := p.parseOrderIdentifier(true)
		if err != nil {
			return nil, err
		}

		result = append(result, nv)

		if match, _ := p.parseToken(list_sep, true); match == false {
			break
		}
	}

	return result, nil
}

/*
 * Parse (optional) TOKEN
 */
func (p *ElseParser) parseToken(ptoken rune, optional bool) (bool, error) {
	token := p.nextToken()
	if token == ptoken {
		p.lastText = ""
		return true, nil
	}

	if optional {
		return false, nil
	} else {
		return false, p.parseError(`"` + string(token) + `"`)
	}
}

/*
 * Parse NUMBER
 */
func (p *ElseParser) parseInteger() (int, error) {
	token := p.nextToken()

	if token == scanner.Int {
		n := p.lastText
		p.lastText = ""
		return strconv.Atoi(n)
	}

	if token == scanner.Float {
		n := p.lastText
		p.lastText = ""
		f, err := strconv.ParseFloat(n, 64)
		return int(f), err
	}

	return 0, p.parseError("integer")
}

/*
 * Parse (quoted) string
 */
func (p *ElseParser) parseString() (string, error) {
	token := p.nextToken()

	if token == scanner.String || token == scanner.RawString {
		s := p.lastText
		p.lastText = ""
		return s, nil
	}

	return "", p.parseError("quoted string")
}

/*
 * Parse value (string or number)
 */
func (p *ElseParser) parseValue() (interface{}, error) {
	token := p.nextToken()

	if token == scanner.String || token == scanner.RawString {
		s := p.lastText
		p.lastText = ""
		return s, nil
	}

	if token == scanner.Int {
		n := p.lastText
		p.lastText = ""
		return strconv.Atoi(n)
	}

	if token == scanner.Float {
		n := p.lastText
		p.lastText = ""
		return strconv.ParseFloat(n, 64)
	}

	return 0, p.parseError("value")
}

func (p *ElseParser) parseOperator() (Operator, error) {
	op := NO_OPERATOR
	var err error

	switch p.nextToken() {
	case '=':
		p.lastText = ""
		op = EQ

	case '!':
		p.lastText = ""
		if token := p.nextToken(); token == '=' {
			p.lastText = ""
			op = NE
		} else {
			err = p.parseError("=")
		}

	case '>':
		p.lastText = ""
		if token := p.nextToken(); token == '=' {
			p.lastText = ""
			op = GTE
		} else {
			op = GT
		}

	case '<':
		p.lastText = ""
		if token := p.nextToken(); token == '=' {
			p.lastText = ""
			op = LTE
		} else {
			op = LT
		}

	default:
		err = p.parseError("operator")
	}

	if DEBUG {
		log.Println("got operator", op, "error", err)
	}

	return op, err
}

func (p *ElseParser) parseDone() bool {
	return p.nextToken() == scanner.EOF
}

func (p *ElseParser) parseExpression() (*Expression, error) {
	var result *Expression

	for !p.parseDone() {
		var expr *Expression
		not, _ := p.parseKeyword(NOT, true)

		stringExpr, _ := p.parseString()
		if stringExpr != "" {
			expr = singleOperand(STRING_EXPR, stringExpr)
		} else {
			name, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			op, err := p.parseOperator()
			if err != nil {
				return nil, err
			}
			value, err := p.parseValue()
			if err != nil {
				return nil, err
			}

			expr = nameValueExpression(op, name, value)
		}

		if not {
			expr = singleOperand(OP_NOT, expr)
		}

		if obool := p.parseBooleanOperator(NO_OPERATOR); obool == NO_OPERATOR {
			return addExpression(result, expr), nil
		} else {
			result = addOperatorExpression(obool, result, expr)
		}
	}

	return result, nil
}

func (p *ElseParser) parseFilter() (*Expression, error) {
	if match, _ := p.parseKeyword(EXIST, true); match {
		field, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		return singleOperand(EXISTS_EXPR, field), nil
	} else if match, _ = p.parseKeyword(MISSING, true); match {
		field, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		return singleOperand(MISSING_EXPR, field), nil
	} else {
		return p.parseExpression()
	}
}

/*
 * parse scriptId = "script expression"
 */
func (p *ElseParser) parseScript() (*NameValue, error) {
	id := p.parseId()
	if id == "" {
		return nil, p.parseError("id")
	}

	if op, _ := p.parseOperator(); op != EQ {
		return nil, p.parseError("=")
	}

	script, _ := p.parseString()
	if script == "" {
		return nil, p.parseError("script")
	}

	return &NameValue{id, script}, nil
}

/*
* Parse ELSEQL statement
 */
func (p *ElseParser) Parse() (err error) {
	if p.parsed {
		return
	}

	p.parsed = true

	if err = p.parseRequired(SELECT); err != nil {
		return
	}

	if match, _ := p.parseToken(all_fields, true); match {
		p.query.SelectList = nil // all fields
	} else {
		p.query.SelectList, err = p.parseIdentifiers()
		if err != nil {
			return
		}
	}

	if match, _ := p.parseKeyword(FACETS, true); match {
		p.query.FacetList, err = p.parseIdentifiers()
		if err != nil {
			return
		}
	}

	if match, _ := p.parseKeyword(SCRIPT, true); match {
		p.query.Script, err = p.parseScript()
		if err != nil {
			return
		}
	}

	if err = p.parseRequired(FROM); err != nil {
		return
	}

	p.query.Index, err = p.parseIdentifier()
	if err != nil {
		return
	}

	if match, _ := p.parseKeyword(WHERE, true); match {
		p.query.WhereExpr, err = p.parseExpression()
		if err != nil {
			return
		}
	}

	if match, _ := p.parseKeyword(FILTER, true); match {
		p.query.FilterExpr, err = p.parseFilter()
		if err != nil {
			return
		}
	}

	if match, _ := p.parseKeyword(ORDER, true); match {
		if err = p.parseRequired(BY); err != nil {
			return
		}
		if p.query.OrderList, err = p.parseOrderIdentifiers(); err != nil {
			return
		}
	}

	if match, _ := p.parseKeyword(LIMIT, true); match {
		v, ierr := p.parseInteger()
		if ierr != nil {
			return ierr
		}

		if match, _ := p.parseToken(list_sep, true); match {
			p.query.From = v
			v, ierr = p.parseInteger()
			if ierr != nil {
				return ierr
			}
		}

		p.query.Size = v
	}

	if !p.parseDone() {
		return p.parseError("EOF")
	}

	return nil
}
