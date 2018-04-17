package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/gobs/pretty"
	"github.com/raff/elseql-go"
)

func main() {
	url := flag.String("url", "http://localhost:9200", "ElasticSearch endpoint")
	format := flag.String("format", "data", "format of results: full, data, list, csv")
	pprint := flag.String("print", "", `how to print/indent output: use pretty for pretty-print or "  " to indent`)
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
		log.Println("ERROR", err)
	} else {
		if *format == "csv" {
			w := csv.NewWriter(os.Stdout)
			for _, r := range res["results"].([]interface{}) {
				w.Write(r.([]string))
			}
			w.Flush()
		} else if *pprint == "pretty" {
			pretty.PrettyPrint(res)
		} else {
			enc := json.NewEncoder(os.Stdout)
			enc.SetEscapeHTML(false)
			enc.SetIndent(*pprint, *pprint)
			enc.Encode(res)
		}
	}
}
