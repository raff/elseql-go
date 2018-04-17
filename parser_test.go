package elseql

import "testing"

func TestParse(t *testing.T) {

	parser := NewParser("SELECT * FROM table WHERE x <= `hello`")
	t.Log(parser.QueryString)

	if err := parser.Parse(); err != nil {
		t.Error(err)
	} else {
		t.Log(parser.Query().String())
	}
}
