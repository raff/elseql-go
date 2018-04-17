package elseql

import (
	"fmt"
	"log"
	"strings"

	"github.com/gobs/httpclient"
	"github.com/gobs/simplejson"
)

type jmap = map[string]interface{}

type ElseSearch struct {
	client *httpclient.HttpClient
}

func NewClient(endpoint string) *ElseSearch {
	return &ElseSearch{client: httpclient.NewHttpClient(endpoint)}
}

func nvList(lin []NameValue) (lout []jmap) {
	for _, nv := range lin {
		lout = append(lout, jmap{nv.Name: nv.Value})
	}

	return
}

func stringify(v interface{}) string {
	switch vv := v.(type) {
	case string:
		return vv

	case nil:
		return ""
	}

	return fmt.Sprintf("%v", v)
}

func getpath(m jmap, k string) (ret interface{}) {
	parts := strings.Split(k, ".")
	ret = m
	for _, k := range parts {
	redo:
		if mm, ismap := ret.(jmap); ismap {
			if val, ok := mm[k]; ok {
				ret = val
			} else {
				return nil
			}
		} else if aa, isarray := ret.([]interface{}); isarray {
			if len(aa) > 0 {
				ret = aa[0]
				goto redo
			} else {
				return nil
			}
		}
	}
	return
}

// void search(String queryString, Util.Format format, boolean streaming, boolean debug)

type ReturnType int

const (
	Full ReturnType = iota
	Data
	List
	StringList
)

func (es *ElseSearch) Search(queryString string, returnType ReturnType) (jmap, error) {
	parser := NewParser(queryString)

	if err := parser.Parse(); err != nil {
		return nil, err
	}

	var jq jmap

	query := parser.Query()

	if query.WhereExpr != nil {
		jq = jmap{
			"query": jmap{
				"query_string": jmap{
					"query": query.WhereExpr.QueryString(),
					//"default_operator": "AND",
				},
			},
		}
	} else {
		jq = jmap{"query": jmap{"match_all": jmap{}}}
	}

	if query.FilterExpr != nil {
		var filter jmap

		if query.FilterExpr.ExistsExpression() {
			filter = jmap{
				"exists": jmap{
					"field": query.FilterExpr.GetOperand().(string),
				},
			}
		} else if query.FilterExpr.MissingExpression() {
			filter = jmap{
				"missing": jmap{
					"field": query.FilterExpr.GetOperand().(string),
				},
			}
		} else {
			filter = jmap{
				"query": jmap{
					"query_string": jmap{
						"query":            query.FilterExpr.QueryString(),
						"default_operator": "AND",
					},
				},
			}
		}

		jq["filter"] = filter
	}

	if len(query.FacetList) > 0 {
		facets := jmap{}

		for _, f := range query.FacetList {
			facets[f] = jmap{"terms": jmap{"field": f}}
		}

		jq["aggs"] = facets
	}

	if query.Script != nil {
		jq["script_fields"] = jmap{
			query.Script.Name: jmap{
				"script": query.Script.Value.(string),
				"lang":   "expression",
			},
		}
	}

	if len(query.SelectList) > 0 {
		jq["_source"] = query.SelectList
	}

	if len(query.OrderList) > 0 {
		jq["sort"] = nvList(query.OrderList)
	}

	if query.Size >= 0 {
		jq["from"] = query.From
		jq["size"] = query.Size
	}

	if Debug {
		log.Println("SEARCH", query.Index, simplejson.MustDumpString(jq))
	}

	res, err := es.client.SendRequest(es.client.Path(query.Index+"/_search"), es.client.JsonBody(jq))
	defer res.Close()

	if err != nil {
		return nil, err
	}

	if err = res.ResponseError(); err != nil {
		return nil, err
	}

	full := res.Json().MustMap()

	switch returnType {
	case Full:
		return full, nil

	case Data:
		data := jmap{}
		if aggs, ok := full["aggregations"]; ok {
			data["facets"] = aggs
		}

		hits := full["hits"].(jmap)
		list := hits["hits"].([]interface{})
		rows := make([]interface{}, 0, len(list))
		for _, r := range list {
			rows = append(rows, r.(jmap)["_source"])
		}
		data["rows"] = rows
		data["total"] = int(hits["total"].(float64))
		return data, nil

	case List, StringList:
		data := jmap{}
		if aggs, ok := full["aggregations"]; ok {
			data["facets"] = aggs
		}

		l := len(query.SelectList)

		hits := full["hits"].(jmap)
		list := hits["hits"].([]interface{})
		rows := make([]interface{}, 0, len(list))
		for _, r := range list {
			m := r.(jmap)["_source"].(jmap)

			if returnType == StringList {
				a := make([]string, l)
				for i, k := range query.SelectList {
					a[i] = stringify(getpath(m, k))
				}
				rows = append(rows, a)
			} else {
				a := make([]interface{}, l)
				for i, k := range query.SelectList {
					a[i] = getpath(m, k)
				}
				rows = append(rows, a)
			}

		}
		data["columns"] = query.SelectList
		data["rows"] = rows
		data["total"] = int(hits["total"].(float64))
		return data, nil
	}

	return nil, nil
}
