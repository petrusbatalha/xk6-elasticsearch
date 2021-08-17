package elasticsearch

import (
	"context"
	"fmt"
	_ "fmt"
	"github.com/olivere/elastic/v7"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"log"
)

func init() {
	modules.Register("k6/x/elasticsearch", new(Elasticsearch))
}

// Json Fields
type ElasticDoc struct {
	Doc map[string]interface{} `json:"-"` // Rest of the fields should go here.
}

// Elasticsearch is the k6 extension for a Elasticsearch client.
type Elasticsearch struct{}

// Client is the Elasticsearch client wrapper.
type Client struct {
	client *elastic.Client
}

// XClient represents the Client constructor returns a new Elasticsearch client object.
func (r *Elasticsearch) XClient(ctxPtr *context.Context, username string, password string, url string) interface{} {
	var client, err = elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(url),
		elastic.SetBasicAuth(username, password))

	if err != nil {
		fmt.Printf("elastic.NewClient() ERROR: %v\n", err)
		log.Fatalf("quiting connection..")
	}

	rt := common.GetRuntime(*ctxPtr)
	return common.Bind(rt, &Client{client: client}, ctxPtr)
}

// Set the document for the given index name.
func (c *Client) AddDocument(index string, docId string, document interface{}) {
	fmt.Printf("Index: %s, docId: %s, %s", index, docId, document);
	r, err := c.client.Index().Index(index).Id(docId).BodyJson(document).Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to index document %s", err)
	}
	fmt.Printf("Doc response %s", r.Result)
}
