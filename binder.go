// Package pgq - generating PostgreSql queries using a template
package sqlb

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/n-r-w/nerr"
	"golang.org/x/exp/slices"
)

type Option int

const (
	JsonPath = Option(iota)
	Json
	NoStringE // не добавлять E в начало строки
)

// Parser - parser for identifying variables of the form :var in an sql query
type Parser struct {
	//  SQL шаблон
	sqlTemplate string
	// Результаты парсинга
	parsed []*data
	// Ключ имя распарсенной переменной
	parsedMap map[string]*data
	// Распарсен ли шаблон
	isParced bool
}

// NewParser - create SqlBinderParser
func NewParser(sqlTemplate string) *Parser {
	return &Parser{
		sqlTemplate: sqlTemplate,
		parsed:      []*data{},
		parsedMap:   map[string]*data{},
		isParced:    false,
	}
}

// data - parsing results for a single variable
type data struct {
	// Название переменной
	name string
	// Положение переменной в строке sql
	pos int
}

// SqlTemplate - SQL template
func (p *Parser) SqlTemplate() string {
	return p.sqlTemplate
}

// ParcedVariables - list of variables in an SQL expression
func (p *Parser) ParcedVariables() []string {
	var res []string
	for _, d := range p.parsed {
		res = append(res, d.name)
	}

	return res
}

// IsVariableParsed - is the variable parsed
func (p *Parser) IsVariableParsed(v string) bool {
	if p.parsedMap == nil {
		return false
	}

	_, ok := p.parsedMap[strings.ToLower(v)]
	return ok
}

// Calculate - substitute values into variables and get the result
func (p *Parser) Calculate(values map[string]string) (string, error) {
	if !p.isParced {
		p.isParced = true
		if err := p.Parse(); err != nil {
			return "", err
		}
	}

	if len(p.parsed) == 0 {
		return p.sqlTemplate, nil
	}

	var sql strings.Builder
	sql.Grow(len(p.sqlTemplate) + len(values)*10)
	shift := 0

	for _, d := range p.parsed {
		// Остаток слева
		sql.WriteString(p.sqlTemplate[shift:d.pos])
		// Заменяем переменную
		value, ok := values[d.name]
		if !ok {
			return "", nerr.New(fmt.Sprintf("bind value not found for: %s", d.name))
		}
		sql.WriteString(value)
		shift = d.pos + len(d.name)
	}

	// Остаток справа
	last := p.parsed[len(p.parsed)-1]
	remains := len(p.sqlTemplate) - (last.pos + len(last.name))
	sql.WriteString(p.sqlTemplate[len(p.sqlTemplate)-remains:])

	return sql.String(), nil
}

func (p *Parser) Parse() error {
	if p.parsedMap == nil {
		p.parsedMap = make(map[string]*data)
	}

	commentFound := false // найден комментарий
	commentLine := false  // комментарий в режиме строки (символы --)

	stringFound := false // найдено начало строки sql (символ ')
	varFound := false    // найдено начало переменной
	firstVarPos := -1

	for i := 0; i < len(p.sqlTemplate); i++ {
		c := p.sqlTemplate[i]

		if commentFound {
			// состояние поиска окончания комментария
			if c == '\n' && commentLine {
				commentFound = false
				commentLine = false
			}

			if c == '*' && (i == len(p.sqlTemplate)-1 || p.sqlTemplate[i+1] == '/') {
				if i != len(p.sqlTemplate)-1 {
					i++ // переходим на символ вперед, чтобы пропустить '/'
				}
				commentFound = false
				commentLine = false
			}

			continue
		}

		if stringFound {
			// В состоянии поиска закрытия строки
			if c == '\'' {
				// Найдена потенциальная закрывающая ковычка
				if i < len(p.sqlTemplate)-1 && p.sqlTemplate[i+1] == '\'' {
					// Это двойная одиночная ковычка - пропускаем и переходим на один символ вперед
					i++
					continue
				}
				stringFound = false
			}
			continue
		}

		if c == '\'' {
			// Найдена открывающая ковычка
			stringFound = true
		}

		if varFound {
			// В режиме поиска конца переменной
			alnum := isAllnum(c)
			if stringFound || (i == len(p.sqlTemplate)-1 || !alnum) {
				// В конце строки или найден не алфавитно-цифровой символ
				d := &data{
					name: p.sqlTemplate[firstVarPos : firstVarPos+i-firstVarPos],
					pos:  firstVarPos,
				}

				// Завершающий символ переменной в конце строки
				if i == len(p.sqlTemplate)-1 && alnum && !stringFound {
					d.name += string(c)
				}

				p.parsed = append(p.parsed, d)
				p.parsedMap[d.name] = d

				varFound = false

				if strings.TrimSpace(d.name) == ":" {
					p.parsed = []*data{}
					p.parsedMap = map[string]*data{}
					return nerr.New("found ':' without variable")
				}
			}
			continue
		}

		if !stringFound && c == ':' && i != len(p.sqlTemplate)-1 && p.sqlTemplate[i+1] == ':' {
			// найдено ::
			i++
			continue
		}

		if !stringFound && c == ':' {
			// найдено начало переменной
			varFound = true
			firstVarPos = i
			continue
		}

		if c == '/' && i != len(p.sqlTemplate)-1 && p.sqlTemplate[i+1] == '*' {
			// Начало многострочного комментария
			commentFound = true
			commentLine = false
			i++
			continue
		}

		if c == '-' && i != len(p.sqlTemplate)-1 && p.sqlTemplate[i+1] == '-' {
			// Начало однострочного комментария
			commentFound = true
			commentLine = true
			i++
			continue
		}
	}

	p.isParced = true

	return nil
}

// isAllnum - is the symbol alphanumeric
func isAllnum(ch byte) bool {
	return ch-'a' < 26 || ch-'A' < 26 || ch-'0' < 10 || ch == '_'
}

// SqlBinder - substitution of values in the Sql query template
type SqlBinder struct {
	// Парсер
	parcer *Parser
	// Пары переменная-значение
	values map[string]string
	// Результат парсинга
	sql        string
	calculated bool
}

var parcedCacheMutex sync.Mutex
var parcedCache map[string]*Parser

// NewBinder - create SqlBinder
// key is used to exclude repeated parsing of identical queries. The result of parsing is saved
func NewBinder(template string, key string) *SqlBinder {
	var parcer *Parser

	if len(key) > 0 {
		parcedCacheMutex.Lock()

		if parcedCache == nil {
			parcedCache = make(map[string]*Parser)
		}

		var ok bool
		if parcer, ok = parcedCache[key]; !ok {
			parcer = NewParser(template)
			parcer.Parse()
			parcedCache[key] = parcer
		} else if len(parcer.SqlTemplate()) != len(template) {
			panic(fmt.Sprintf("same key for different templates: %s", key))
		}

		parcedCacheMutex.Unlock()
	} else {
		parcer = NewParser(template)
	}

	return &SqlBinder{
		parcer:     parcer,
		values:     map[string]string{},
		sql:        "",
		calculated: false,
	}
}

// Clear - resets everything except the template
func (b *SqlBinder) Clear() {
	b.calculated = false
	b.sql = ""
	b.values = map[string]string{}
}

// Bind - replace the format bind in the Sql string :bind to the value of the value variable
func (b *SqlBinder) Bind(variable string, value interface{}, options ...Option) error {
	if len(variable) == 0 {
		return nerr.New("empty variable")
	}

	if b.calculated {
		return nerr.New("bind after calculate")
	}

	if _, ok := b.values[variable]; ok {
		return nerr.New(fmt.Sprintf("already binded %s", variable))
	}

	var v string
	if variable[0] != ':' {
		v = ":" + variable
	} else {
		v = variable
	}

	val, err := ToSql(value, options...)
	if err != nil {
		return err
	}

	b.values[v] = val

	return nil
}

// ToSql - convert any value to sql string
func ToSql(v interface{}, options ...Option) (string, error) {
	var val string

	if v != nil {
		switch v := v.(type) {
		case time.Duration:
			total := int64(v.Seconds())
			if total <= 60*60*24 {
				h := int(total / (60 * 60))
				m := int(total/60) - h*60
				s := total % 60
				val = fmt.Sprintf("'%d:%d:%d'", h, m, s)
			} else {
				return "", nerr.New(fmt.Sprintf("can't bind time.Duration, value: %v", v))
			}

		case time.Time:
			val = "'" + v.Format("2006-01-02 15:04:05.000000 -0700") + "'"

		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			val = fmt.Sprintf("%d", v)
		case float32, float64:
			val = fmt.Sprintf("%v", v)
		case string:
			val = strings.TrimSpace(v)
			if len(val) != 0 {
				val = prepareString(val, options...)
			}
		case bool:
			if v {
				val = "TRUE"
			} else {
				val = "FALSE"
			}
		case []byte:
			val = "E'\\\\x" + hex.EncodeToString(v) + "'"

		default:
			if slices.Contains(options, Json) {
				j, err := json.Marshal(v)
				if err != nil {
					return "", nerr.New(err, "can't parse json")
				}
				return prepareString(string(j), prepareOptions(options, []Option{NoStringE})...), nil
			}

			val = strings.TrimSpace(fmt.Sprintf("%v", v))
			if len(val) != 0 {
				val = prepareString(val, options...)
			}
		}
	}

	if len(val) == 0 {
		val = "NULL"
	}

	return val, nil
}

// Bind - replace the format bind in the Sql string :bind to the value of the value variable
func (b *SqlBinder) BindValues(values map[string]interface{}) error {
	for variable, value := range values {
		if err := b.Bind(variable, value); err != nil {
			return err
		}
	}

	return nil
}

func prepareString(s string, options ...Option) string {
	prep := strings.ReplaceAll(s, "'", "\\'")

	if slices.Contains(options, JsonPath) {
		return `"` + prep + `"`

	}

	if slices.Contains(options, NoStringE) {
		return prep
	}

	return "E'" + prep + "'"
}

// Sql - get the result of substituting variables into a template
func (b *SqlBinder) Sql() (string, error) {
	if !b.calculated {
		b.calculated = true

		var err error
		b.sql, err = b.parcer.Calculate(b.values)
		if err != nil {
			return "", err
		}
	}

	return b.sql, nil
}

// IsVariableParsed - checks whether there is such a variable in the list of parsed
func (b *SqlBinder) IsVariableParsed(v string) bool {
	return b.parcer.IsVariableParsed(v)
}

// ParcedVariables - list of variables in an SQL expression
func (b *SqlBinder) ParcedVariables() []string {
	return b.parcer.ParcedVariables()
}

// BindOne - replace the format bind in the Sql string :bind to the value of the value variable
func BindOne(template string, variable string, value interface{}, key string) (string, error) {
	binder := NewBinder(template, key)
	if err := binder.Bind(variable, value); err != nil {
		return "", err
	}

	return binder.Sql()
}

// Bind - сразу биндит и генерит sql
func Bind(template string, values map[string]interface{}, key string) (string, error) {
	binder := NewBinder(template, key)
	if err := binder.BindValues(values); err != nil {
		return "", err
	}

	return binder.Sql()
}

// prepareOptions - Оставить только те свойства, которые требуются (если они есть)
func prepareOptions(options []Option, required []Option) []Option {
	res := []Option{}
	for _, v := range options {
		if slices.Contains(required, v) {
			res = append(res, v)
		}
	}
	return res
}

// подготовка значения перез записью в БД. Превращает 0 или пустую строку в nil
func VNull(v interface{}) interface{} {
	switch d := v.(type) {
	case int:
		if d == 0 {
			return nil
		}
		return d
	case uint:
		if d == 0 {
			return nil
		}
		return d
	case int8:
		if d == 0 {
			return nil
		}
		return d
	case int16:
		if d == 0 {
			return nil
		}
		return d
	case int32:
		if d == 0 {
			return nil
		}
		return d
	case int64:
		if d == 0 {
			return nil
		}
		return d
	case uint8:
		if d == 0 {
			return nil
		}
		return d
	case uint16:
		if d == 0 {
			return nil
		}
		return d
	case uint32:
		if d == 0 {
			return nil
		}
		return d
	case uint64:
		if d == 0 {
			return nil
		}
		return d
	case string:
		if len(strings.TrimSpace(d)) == 0 {
			return nil
		}
		return d
	case []byte:
		if len(d) == 0 {
			return nil
		}
		return d
	default:
		return v
	}
}
