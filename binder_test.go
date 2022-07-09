package sqlb

import (
	"testing"
	"time"
)

const (
	templateSql1 = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = :var1 AND key2 = :var2 -- comment`

	resultSql1 = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = 123 AND key2 = 456 -- comment`

	templateSql2 = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = :var1 -- comment`

	resultSqlInt = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = 123 -- comment`
	resultSqlString = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = E'12\'3' -- comment`
	resultSqlFloat = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = 123.456 -- comment`
	resultSqlDateTime = `-- comment :var
		/* comment :var */
		SELECT field1, field2 
		FROM table 
		WHERE key1 = '2022-05-31 16:15:42.000234 +0000' -- comment`
)

func TestSqlBinderParser_Calculate(t *testing.T) {
	parser := NewParser(templateSql1)

	sql, err := parser.Calculate(map[string]string{
		":var1": "123",
		":var2": "456",
	})

	if err != nil {
		t.Errorf("SqlBinderParser.Calculate() error = %v", err)
		return
	}

	if resultSql1 != sql {
		t.Errorf("SqlBinderParser.Calculate():\n%s\nwant:\n%s", sql, resultSql1)
		return
	}
}

func TestSqlBinder_Sql(t *testing.T) {
	type Test struct {
		name     string
		template string
		variable string
		value    any
		result   string
	}

	tests := []Test{
		{
			name:     "int",
			template: templateSql2,
			variable: ":var1",
			value:    123,
			result:   resultSqlInt,
		},
		{
			name:     "float",
			template: templateSql2,
			variable: ":var1",
			value:    123.456,
			result:   resultSqlFloat,
		},
		{
			name:     "DateTime",
			template: templateSql2,
			variable: ":var1",
			value:    time.Date(2022, 05, 31, 16, 15, 42, 234567, time.UTC),
			result:   resultSqlDateTime,
		},
		{
			name:     "string",
			template: templateSql2,
			variable: ":var1",
			value:    `12'3`,
			result:   resultSqlString,
		},
		{
			name:     "null1",
			template: "INSERT INTO table (field1) values(:field1)",
			variable: ":field1",
			value:    nil,
			result:   "INSERT INTO table (field1) values(NULL)",
		},
		{
			name:     "null2",
			template: "INSERT INTO table (field1) values(:field1)",
			variable: ":field1",
			value:    "",
			result:   "INSERT INTO table (field1) values(NULL)",
		},
		{
			name:     "bool",
			template: "INSERT INTO table (field1) values(:field1)",
			variable: ":field1",
			value:    true,
			result:   "INSERT INTO table (field1) values(TRUE)",
		},
		{
			name:     "bytes",
			template: "INSERT INTO table (field1) values(:field1)",
			variable: ":field1",
			value:    []byte("qwerty"),
			result:   `INSERT INTO table (field1) values(E'\\x717765727479')`,
		},
	}

	for i := 1; i <= 2; i++ { // двойной прогон для проверки кэширования парсинга по ключу
		for _, test := range tests {
			binder := NewBinder(test.template, test.name)
			if err := binder.Bind(test.variable, test.value); err != nil {
				t.Errorf("SqlBinder.Sql() %s: %v", test.name, err)
				return
			}
			sql, err := binder.Sql()
			if err != nil {
				t.Errorf("SqlBinder.Sql() %s: %v", test.name, err)
				return
			}

			if test.result != sql {
				t.Errorf("SqlBinder.Sql() %s:\n%s\nwant:\n%s", test.name, sql, test.result)
			}
		}
	}
}

// Кастомные типы данных
func TestSqlBinder_BindTypes(t *testing.T) {
	template := "SELECT * FROM table WHERE id=:id"

	type MyString string
	s := MyString("test")

	sql, err := BindOne(template, "id", s, "")
	if err != nil {
		t.Fatal(err)
	}

	req := "SELECT * FROM table WHERE id=E'test'"
	if sql != req {
		t.Fatalf("%s, wants: %s", sql, req)
	}

	type MyInt int
	i := MyInt(123)

	sql, err = BindOne(template, "id", i, "")
	if err != nil {
		t.Fatal(err)
	}

	req = "SELECT * FROM table WHERE id=123"
	if sql != req {
		t.Fatalf("%s, wants: %s", sql, req)
	}

	type MyFloat float64
	f := MyFloat(123.45)

	sql, err = BindOne(template, "id", f, "")
	if err != nil {
		t.Fatal(err)
	}

	req = "SELECT * FROM table WHERE id=123.45"
	if sql != req {
		t.Fatalf("%s, wants: %s", sql, req)
	}
}
