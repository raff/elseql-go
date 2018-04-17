package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/gobs/pretty"
	"github.com/raff/elseql-go"
)

func main() {
	q := strings.Join(os.Args[1:], " ")

	es := elseql.NewClient("http://localhost:9200")
	res, err := es.Search(q)
	if err != nil {
		fmt.Println("ERROR", err)
	} else {
		fmt.Println("RESULT")
		pretty.PrettyPrint(res)
	}
}
