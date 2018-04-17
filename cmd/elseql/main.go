package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/gobs/pretty"
	"github.com/raff/elseql-go"
)

func main() {
	url := flag.String("url", "http://localhost:9200", "ElasticSearch endpoint")
	flag.BoolVar(&elseql.Debug, "debug", false, "log debug info")
	flag.Parse()

	q := strings.Join(flag.Args(), " ")

	es := elseql.NewClient(*url)
	res, err := es.Search(q)
	if err != nil {
		fmt.Println("ERROR", err)
	} else {
		fmt.Println("RESULT")
		pretty.PrettyPrint(res)
	}
}
