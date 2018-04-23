package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/gobs/pretty"
	"github.com/peterh/liner"
	"github.com/raff/elseql-go"
)

var (
	keywords = []string{
		"SELECT",
		// "COUNT",
		"FACETS",
		"FROM",
		"WHERE",
		"AND",
		"OR",
		"ORDER BY",
		"ASC",
		"DESC",
		"LIMIT",
		"NEXT",
		"_all",
		"keyword",
	}
)

const (
	historyfile = ".elseql"
)

func init() {
	sort.Strings(keywords)
}

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

	runQuery := func(q string) {
		res, err := es.Search(q, rType)
		if err != nil {
			log.Println("ERROR", err)
		} else {
			if *format == "csv" {
				w := csv.NewWriter(os.Stdout)
				for _, r := range res["rows"].([]interface{}) {
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

	if q != "" {
		runQuery(q)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	if f, err := os.Open(historyfile); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	defer func() {
		if f, err := os.Create(historyfile); err == nil {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	line.SetWordCompleter(func(line string, pos int) (head string, completions []string, tail string) {
		head = line[:pos]
		tail = line[pos:]

		i := strings.LastIndex(head, " ")
		w := head[i+1:]

		head = strings.TrimSuffix(head, w)
		w = strings.ToUpper(w)

		for _, n := range keywords {
			if strings.HasPrefix(n, w) {
				completions = append(completions, n)
			}
		}
		return
	})

	var cmd string
	var multi bool

	prompt := map[bool]string{
		false: "> ",
		true:  ": ",
	}

	for {
		l, err := line.Prompt(prompt[multi])
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				return
			}
			fmt.Println(err)
			return
		}

		if multi == false {
			if l == "[[[" {
				multi = true
				cmd = ""
				continue
			} else {
				cmd = l
			}
		} else {
			if l == "]]]" {
				multi = false
			} else {
				cmd += " " + l
				continue
			}
		}

		cmd = strings.TrimSpace(cmd)
		if len(l) == 0 {
			continue
		}

		line.AppendHistory(cmd)
		runQuery(cmd)
	}
}
