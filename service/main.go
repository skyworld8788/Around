package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"net/http"      // No need web frame work like Tomcat.
	"encoding/json" // No need json parse external library
	"log"
	"reflect"
	"github.com/pborman/uuid"
	"strconv"
)

type Location struct {  // like C type define
	Lat float64 `json:"lat"` //annotation  trick: map front end json format
	Lon float64 `json:"lon"`
}
type Post struct {
	//`json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
}

const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	// Needs to update
	//PROJECT_ID = "around-xxx"
	//BT_INSTANCE = "around-post"
	// Needs to update this URL if you deploy it to cloud.
	ES_URL = "http://35.230.53.111:9200"
)
func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
                    "mappings":{
                           "post":{
                                  "properties":{
                                         "location":{
                                                "type":"geo_point"
                                         }
                                  }
                           }
                    }
             }
             `
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("started-service")
	http.HandleFunc("/post", handlerPost)   //Event Handler call back func pointer
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))// After the handlerFunc.handler: http.DefaultServeMux if nil
}

func handlerSearch(w http.ResponseWriter, r *http.Request) { // r is a pointer
	fmt.Println("Received one request for search")
	lt := r.URL.Query().Get("lat")  //remember url query format
	lat, _ := strconv.ParseFloat(lt, 64) // multi_return value, not want to use again, use _
	ln := r.URL.Query().Get("lon")
	lon, _ := strconv.ParseFloat(ln,64)
	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Println("range is ", ran)
/*	// Return a fake post
	p := &Post{
		User:"1111",
		Message:"一生必去的100个地方",
		Location: Location{
			Lat:lt,
			Lon:ln,
		},
	}
	js, err := json.Marshal(p) // Marshal: convert p to json format
	if err != nil {
		panic(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
	//fmt.Fprintf(w, "Search received: %s %s", lat, lon) // write the string to w(http.responseWriter)
	//fmt.Printf( "Search received: %f %f %s\n", lat, lon, ran) */

	// Create a client, create a connection to ES
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Define geo distance query as specified in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	// Prepare a geo based query to find posts within a geo box
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	// Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
	searchResult, err := client.Search().	// Search is the entry point for searches
		Index(INDEX).						// Index: sets the names of the indices to use for search.
		Query(q).							// Query: sets the query to perform
		Pretty(true). 					// Pretty: enables the caller to indent the JSON output
		Do()                				// Do: executes the search and returns a SearchResult.
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // instance of
		p := item.(Post) // p = (Post) item
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		// TODO(student homework): Perform filtering based on keywords such as web spam etc.
		ps = append(ps, p)

	}
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)

}

func handlerPost(w http.ResponseWriter, r *http.Request){
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil { // fancy expression: equals to: err:=decoder.Decode(&p)
												//                              if (err != nil) blabla...
		panic(err) // fatal exception
		return
	}
	//fmt.Fprintf(w, "Post received: %s\n", p.Message) // save "Pose received...." + p.Message to w
	id := uuid.New()
	// Save to ES.
	saveToES(&p, id)

}

// Save a post to ElasticSearch
func saveToES(p *Post, id string) {
	// Create a client
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}
	// Save it to index
	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		panic(err)
		return
	}
	fmt.Printf("Post is saved to Index: %s\n", p.Message)
}

