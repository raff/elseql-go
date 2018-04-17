package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/gobs/pretty"
	"github.com/raff/elseql-go"
)

func main() {
	url := flag.String("url", "http://localhost:9200", "ElasticSearch endpoint")
	format := flag.String("format", "data", "format of results: full, data, list, csv")
	flag.BoolVar(&elseql.Debug, "debug", false, "log debug info")
	flag.Parse()

	q := strings.Join(flag.Args(), " ")

	es := elseql.NewClient(*url)

	var rType elseql.ReturnType

	switch *format {
	case "full":
		rType = elseql.Full
	case "data":
		rType = elseql.Data
	case "list":
		rType = elseql.List
	case "csv":
		rType = elseql.StringList
	}

	res, err := es.Search(q, rType)
	if err != nil {
		fmt.Println("ERROR", err)
	} else {
		if *format == "csv" {
			w := csv.NewWriter(os.Stdout)
			for _, r := range res["results"].([]interface{}) {
				w.Write(r.([]string))
			}

			w.Flush()
		} else {
			fmt.Println("RESULT")
			pretty.PrettyPrint(res)
		}
	}
}
