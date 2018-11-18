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

func (es *ElseSearch) AllowInsecure(insecure bool) {
	es.client.AllowInsecure(insecure)
}

func nvList(lin []NameValue) (lout []jmap) {
	for _, nv := range lin {
		lout = append(lout, jmap{nv.Name: nv.Value})
	}

	return
}

func stringify(v jobj, nv string) string {
	switch vv := v.(type) {
	case string:
		return vv

	case nil:
		return nv

	case jmap:
		return strings.TrimSpace(simplejson.MustDumpString(vv))

	case jarr:
		return strings.TrimSpace(simplejson.MustDumpString(vv))
	}

	return fmt.Sprintf("%v", v)
}

func getpath(m jmap, k string) (ret jobj) {
	parts := strings.Split(k, ".")
	return getparts(m, parts)
}

func getparts(o jobj, parts []string) (ret jobj) {
	ret = o
	for pk, k := range parts {
		if mm, ismap := ret.(jmap); ismap {
			if val, ok := mm[k]; ok {
				ret = val
			} else {
				return nil
			}
		} else if aa, isarray := ret.(jarr); isarray {
			if len(aa) == 0 {
				return nil
			}

			aret := make([]jobj, len(aa))
			for i, v := range aa {
				aret[i] = getparts(v, parts[pk:])
			}
			ret = aret
		}
	}
	return
}

func parent(path string) string {
	i := strings.LastIndex(path, ".")
	if i < 1 {
		return ""
	}

	return path[0:i]
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

// Parse an ElseSQL query and return an ElasticSearch query object, the index and the list of columns to return
func ParseQuery(queryString, after string) (jq jmap, index string, columns []string, sErr error) {
	parser := NewParser(queryString)

	if err := parser.Parse(); err != nil {
		sErr = SearchError{
			Err:   err,
			Query: queryString,
		}
		return
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
				"bool": jmap{
					"must_not": jmap{
						"exists": jmap{
							"field": query.FilterExpr.GetOperand().(string),
						},
					},
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

		if jq == nil {
			jq = jmap{"query": filter}
		} else {
			jq["filter"] = filter
		}
	} else if jq == nil {
		jq = jmap{"query": jmap{"match_all": jmap{}}}
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
			sErr = SearchError{
				Err:   ParseError("invalid value for AFTER"),
				Query: queryString,
			}
			return
		}

		jq["search_after"] = after
		if jq["sort"] != nil {
			jq["sort"] = append(jq["sort"].([]jmap), jmap{"_id": "asc"})
		} else {
			jq["sort"] = []jmap{jmap{"_id": "asc"}}
		}
	}

	index = strings.Replace(query.Index, ".", "/", 1) // convert index.doc to index/doc
	if index == "_all" {
		index = ""
	}

	columns = query.SelectList
	return
}

func (es *ElseSearch) Search(queryString, after, nilValue, index string, returnType ReturnType) (jmap, error) {
	var jq jmap
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
		var err error

		jq, index, columns, err = ParseQuery(queryString, after)
		if err != nil {
			return nil, err
		}
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

		for _, r := range list {
			m := r.(jmap)["_source"].(jmap)
			last = r.(jmap)["sort"]

			a := make(jarr, len(columns))

			var l []struct {
				pos int
				arr jarr
			}

			nested := ""

			for i, k := range columns {
				res := getpath(m, k)
				if aa, ok := res.(jarr); ok {
					if returnType == StringList {
						a[i] = nilValue
					} else {
						a[i] = nil
					}
					switch len(aa) {
					case 0:
						continue
					case 1:
						res = aa[0]
						if returnType == StringList {
							res = stringify(aa[0], nilValue)
						}
						a[i] = res
						continue
					default:
						if nested != "" && parent(k) != nested {
							return nil, SearchError{
								Err:   fmt.Errorf("too many nested lists in result"),
								Query: simplejson.MustDumpString(jq),
							}
						}
					}
					nested = parent(k)
					l = append(l, struct {
						pos int
						arr jarr
					}{
						pos: i,
						arr: aa,
					})
				} else {
					if returnType == StringList {
						res = stringify(res, nilValue)
					}
					a[i] = res
				}
			}

			if nested != "" { // we have nested fields
				ll := len(l[0].arr)

				for i := 0; i < ll; i++ {
					ele := make(jarr, len(a))
					copy(ele, a)

					for _, aa := range l {
						if i < len(aa.arr) {
							if returnType == StringList {
								ele[aa.pos] = stringify(aa.arr[i], nilValue)
							} else {
								ele[aa.pos] = aa.arr[i]
							}
						}
					}

					rows = append(rows, ele)
				}
			} else {
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
