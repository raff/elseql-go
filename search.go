package elseql

import (
	"encoding/base64"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/gobs/httpclient"
	"github.com/gobs/simplejson"
)

type jobj = interface{}
type jmap = map[string]interface{}
type jarr = []interface{}

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

func stringify(v jobj) string {
	switch vv := v.(type) {
	case string:
		return vv

	case nil:
		return ""

	case jmap:
		return strings.TrimSpace(simplejson.MustDumpString(vv))

	case jarr:
		return strings.TrimSpace(simplejson.MustDumpString(vv))
	}

	return fmt.Sprintf("%v", v)
}

func getpath(m jmap, k string) (ret jobj) {
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
		} else if aa, isarray := ret.(jarr); isarray {
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

func encodeObject(obj jobj) string {
	jb := simplejson.MustDumpBytes(obj)
	return base64.RawURLEncoding.EncodeToString(jb)
}

func decodeObject(encoded string) jobj {
	// assume it's a base64 encoded object
	if bb, err := base64.RawURLEncoding.DecodeString(encoded); err == nil {
		encoded = string(bb)
	}

	// here it should be a JSON encoded object
	ret, err := simplejson.LoadString(encoded)
	if err != nil {
		return nil
	}

	return ret.Data()
}

type SearchError struct {
	Err   error
	Query string
}

func (e SearchError) Error() string {
	return fmt.Sprintf("Error: %q Query: %v", e.Err, e.Query)
}

func (es *ElseSearch) Search(queryString, after string, returnType ReturnType) (jmap, error) {
	var jq jmap
	var index string
	var columns []string

	if strings.HasPrefix(queryString, "{") { // ES JSON query
		jj, err := simplejson.LoadString(queryString)
		if err != nil {
			return nil, SearchError{
				Err:   err,
				Query: queryString,
			}
		}

		jq = jj.MustMap()
	} else {
		parser := NewParser(queryString)

		if err := parser.Parse(); err != nil {
			return nil, SearchError{
				Err:   err,
				Query: queryString,
			}
		}

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

		if query.After != "" {
			after = query.After
		}

		if after != "" {
			after := decodeObject(after)
			if after == nil {
				return nil, SearchError{
					Err:   ParseError("invalid value for AFTER"),
					Query: queryString,
				}
			}

			jq["search_after"] = after
			if jq["sort"] != nil {
				jq["sort"] = append(jq["sort"].([]jmap), jmap{"_id": "asc"})
			} else {
				jq["sort"] = []jmap{jmap{"_id": "asc"}}
			}
		}

		index = query.Index
		if index == "_all" {
			index = ""
		}

		columns = query.SelectList
	}

	if Debug {
		log.Println("SEARCH", index, simplejson.MustDumpString(jq))
	}

	if strings.HasPrefix(index, "_") {
		return nil, SearchError{
			Err:   ParseError("invalid index name"),
			Query: queryString,
		}
	}

	res, err := es.client.SendRequest(es.client.Path(index+"/_search"), es.client.JsonBody(jq))
	defer res.Close()

	if err != nil {
		return nil, SearchError{
			Err:   err,
			Query: simplejson.MustDumpString(jq),
		}
	}

	if err = res.ResponseError(); err != nil {
		return nil, SearchError{
			Err:   err,
			Query: simplejson.MustDumpString(jq),
		}
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
		list := hits["hits"].(jarr)
		rows := make(jarr, 0, len(list))
		var last jobj
		for _, r := range list {
			rows = append(rows, r.(jmap)["_source"])
			last = r.(jmap)["sort"]
		}
		data["rows"] = rows
		data["total"] = int(hits["total"].(float64))
		if last != nil {
			data["last"] = encodeObject(last)
		}
		return data, nil

	case List, StringList:
		data := jmap{}
		if aggs, ok := full["aggregations"]; ok {
			data["facets"] = aggs
		}

		hits := full["hits"].(jmap)
		list := hits["hits"].(jarr)
		rows := make(jarr, 0, len(list))
		var last jobj

		if len(columns) == 0 && len(list) > 0 {
			m := list[0].(jmap)["_source"].(jmap) // assume the first row has all the names
			for k, _ := range m {
				columns = append(columns, k)
			}
			sort.Strings(columns)
		}
		l := len(columns)

		for _, r := range list {
			m := r.(jmap)["_source"].(jmap)
			last = r.(jmap)["sort"]

			if returnType == StringList {
				a := make([]string, l)
				for i, k := range columns {
					a[i] = stringify(getpath(m, k))
				}
				rows = append(rows, a)
			} else {
				a := make(jarr, l)
				for i, k := range columns {
					a[i] = getpath(m, k)
				}
				rows = append(rows, a)
			}

		}
		data["columns"] = columns
		data["rows"] = rows
		data["total"] = int(hits["total"].(float64))
		if last != nil {
			data["last"] = encodeObject(last)
		}
		return data, nil
	}

	return nil, nil
}
