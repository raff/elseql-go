package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/gobs/pretty"
	"github.com/raff/elseql-go"
)

func main() {
	url := flag.String("url", "http://localhost:9200", "ElasticSearch endpoint")
	flag.Parse()

	q := strings.Join(os.Args[1:], " ")

	es := elseql.NewClient(*url)
	res, err := es.Search(q)
	if err != nil {
		fmt.Println("ERROR", err)
	} else {
		fmt.Println("RESULT")
		pretty.PrettyPrint(res)
	}
}
