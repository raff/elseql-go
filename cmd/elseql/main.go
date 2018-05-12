package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/gobs/httpclient"
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
		".keyword",

		".format",
	}

	formats = []string{
		"data",
		"full",
		"list",
		"csv",
		"csv-headers",
	}

	historyfile = ".elseql"
)

func init() {
	sort.Strings(keywords)
	sort.Strings(formats)

	// check current directory
	if _, err := os.Stat(historyfile); os.IsNotExist(err) {
		// check home directory
		homepath := path.Join(os.Getenv("HOME"), historyfile)
		if _, err = os.Stat(homepath); err == nil {
			historyfile = homepath
		}
	}
}

func returnType(f string, r elseql.ReturnType) elseql.ReturnType {
	switch f {
	case "full":
		r = elseql.Full
	case "data":
		r = elseql.Data
	case "list":
		r = elseql.List
	case "csv", "csv-headers", "local-csv", "local-csv-headers":
		r = elseql.StringList
	default:
		log.Printf("invalid format %q - use full,data,list,csv or csv-headers", f)
	}

	return r
}

func main() {
	url := flag.String("url", "http://localhost:9200", "ElasticSearch endpoint")
	format := flag.String("format", "data", "format of results: full, data, list, csv, csv-headers")
	pprint := flag.String("print", " ", `how to print/indent output: use pretty for pretty-print or "  " to indent`)
	proxy := flag.Bool("proxy", false, "if true, we are talking to a proxy server")
	flag.BoolVar(&elseql.Debug, "debug", false, "log debug info")
	flag.Parse()

	q := strings.Join(flag.Args(), " ")
	rType := returnType(*format, elseql.Data)
	rFormat := *format

	var runQuery func(string) (int, int)

	if *proxy {
		esproxy := httpclient.NewHttpClient(*url)
		esproxy.Verbose = elseql.Debug

		runQuery = func(q string) (int, int) {
			sFormat := rFormat
			if rFormat == "local-csv" || rFormat == "local-csv-headers" || *pprint == "" {
				sFormat = "list"
			}
			res, err := esproxy.Get("", map[string]interface{}{
				"q": q,
				"f": sFormat,
			}, nil)
			if err == nil {
				err = res.ResponseError()
			}
			if err != nil {
				log.Println("ERROR", err)
				return -1, -1
			} else {
				if rFormat == "csv" || rFormat == "csv-headers" || *pprint == "" {
					io.Copy(os.Stdout, res.Body)
				} else if rFormat == "local-csv" || rFormat == "local-csv-headers" {
					var data struct {
						Columns []string   `json:"columns"`
						Rows    [][]string `json:"rows"`
					}

					err = json.NewDecoder(res.Body).Decode(&data)

					w := csv.NewWriter(os.Stdout)
					if rFormat == "local-csv-headers" {
						w.Write(data.Columns)
					}
					for _, r := range data.Rows {
						w.Write(r)
					}
					w.Flush()
				} else {
					var data interface{}
					err = json.NewDecoder(res.Body).Decode(&data)
					if err != nil {
						log.Println("ERROR", err)
					} else if *pprint == "pretty" {
						pretty.PrettyPrint(data)
					} else {
						enc := json.NewEncoder(os.Stdout)
						enc.SetEscapeHTML(false)
						enc.SetIndent("", *pprint)
						enc.Encode(data)
					}
				}

				n, _ := strconv.Atoi(res.Header.Get("x-elseql-count"))
				t, _ := strconv.Atoi(res.Header.Get("x-elseql-total"))
				return n, t
			}
		}
	} else {
		es := elseql.NewClient(*url)

		runQuery = func(q string) (int, int) {
			res, err := es.Search(q, "", "", rType)
			if err != nil {
				log.Println("ERROR", err)
				return -1, -1
			} else {
				if rFormat == "csv" || rFormat == "csv-headers" {
					w := csv.NewWriter(os.Stdout)
					if rFormat == "csv-headers" {
						w.Write(res["columns"].([]string))
					}
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

				if rFormat == "full" {
					hits := res["hits"].(map[string]interface{})
					return len(hits["hits"].([]interface{})), int(hits["total"].(float64))
				} else {
					return len(res["rows"].([]interface{})), res["total"].(int)
				}
			}
		}
	}

	if q != "" {
		runQuery(q)
		return
	}

	hasHistory := false
	line := liner.NewLiner()
	defer line.Close()

	if f, err := os.Open(historyfile); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	defer func() {
		if hasHistory {
			if f, err := os.Create(historyfile); err == nil {
				line.WriteHistory(f)
				f.Close()
			}
		}
	}()

	line.SetWordCompleter(func(line string, pos int) (head string, completions []string, tail string) {
		head = line[:pos]
		tail = line[pos:]

		i := strings.LastIndex(head, " ")
		w := head[i+1:]

		head = strings.TrimSuffix(head, w)
		w = strings.ToUpper(w)

		matches := keywords
		if strings.HasPrefix(line, ".format ") {
			matches = formats
		}

		for _, n := range matches {
			if strings.HasPrefix(strings.ToUpper(n), w) {
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
		hasHistory = true

		if strings.HasPrefix(cmd, ".format ") {
			parts := strings.Split(cmd, " ")
			rFormat = parts[1]
			rType = returnType(parts[1], rType)
			continue
		}

		fmt.Println()

		n, t := runQuery(cmd)
		if n >= 0 {
			fmt.Printf("\n%v ROWS, %v TOTAL\n", n, t)
		}
	}
}
