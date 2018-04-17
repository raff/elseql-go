package elseql

import (
	"log"

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

// void search(String queryString, Util.Format format, boolean streaming, boolean debug)

func (es *ElseSearch) Search(queryString string) (jmap, error) {
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
					"query":            query.WhereExpr.QueryString(),
					"default_operator": "AND",
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

	return res.Json().MustMap(), nil
}
