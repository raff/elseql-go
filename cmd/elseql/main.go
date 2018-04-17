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
	full := flag.Bool("full", false, "return full ElasticSearch results")
	flag.BoolVar(&elseql.Debug, "debug", false, "log debug info")
	flag.Parse()

	q := strings.Join(flag.Args(), " ")

	es := elseql.NewClient(*url)
	rType := elseql.Data
	if *full {
		rType = elseql.Full
	}
	res, err := es.Search(q, rType)
	if err != nil {
		fmt.Println("ERROR", err)
	} else {
		fmt.Println("RESULT")
		pretty.PrettyPrint(res)
	}
}
