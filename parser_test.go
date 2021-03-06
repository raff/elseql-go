package elseql

import "testing"

func TestParse(t *testing.T) {

	parser := NewParser("SELECT * FROM table WHERE x <= `hello` ORDER BY name ASC, value DESC")
	t.Log(parser.QueryString)

	if err := parser.Parse(); err != nil {
		t.Error(err)
	} else {
		t.Log(parser.Query().String())
	}
}

func TestParse2(t *testing.T) {
	parser := NewParser("SELECT * FROM table WHERE x.desc <= `hello` ORDER BY name ASC, value DESC")
	t.Log(parser.QueryString)

	if err := parser.Parse(); err != nil {
		t.Error(err)
	} else {
		t.Log(parser.Query().String())
	}
}

func TestParse3(t *testing.T) {
	parser := NewParser("SELECT * FROM table WHERE x <= `hello` AND y >= `there` ORDER BY name ASC, value DESC")
	t.Log(parser.QueryString)

	if err := parser.Parse(); err != nil {
		t.Error(err)
	} else {
		t.Log(parser.Query().String())
	}
}
