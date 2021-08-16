package elasticsearch

import (
    "bytes"
    "context"
    "encoding/json"
    _ "fmt"
    "github.com/elastic/go-elasticsearch/v8"
    "github.com/elastic/go-elasticsearch/v8/esapi"
    "go.k6.io/k6/js/common"
    "go.k6.io/k6/js/modules"
    "log"
)

func init() {
    modules.Register("k6/x/elasticsearch", new(Elasticsearch))
}

// Elasticsearch is the k6 extension for a Elasticsearch client.
type Elasticsearch struct{}

// Client is the Elasticsearch client wrapper.
type Client struct {
    client *elasticsearch.Client
}

// XClient represents the Client constructor (i.e. `new redis.Client()`) and
// returns a new Redis client object.
func (r *Elasticsearch) XClient(ctxPtr *context.Context, config elasticsearch.Config) interface{} {
    var client, _ = elasticsearch.NewClient(config)
    rt := common.GetRuntime(*ctxPtr)
    return common.Bind(rt, &Client{client: client}, ctxPtr)
}

// Set the document for the given index name.
func (c *Client) Set(index string, docId string, document map[string]interface{}, ) {
    go func() {
        doc, err := json.Marshal(document)
        if err != nil {
            log.Fatalf("Failed to parse document %s", err)
        }

        req := esapi.CreateRequest{
            Index: index,
            DocumentID: docId,
            Body: bytes.NewReader(doc),
        }

        res, err := req.Do(context.Background(), c.client)
        if err != nil {
            log.Fatalf("Error getting response: %s", err)
        }
        defer res.Body.Close()
    }()
}
