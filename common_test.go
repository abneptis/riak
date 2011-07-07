package riak

import (
	"http"
	"testing"
)


var TESTING_RIAK = http.URL{Scheme: "http", Host: "localhost:8098", Path: "/"}
var BAD_RIAK = http.URL{Scheme: "http", Host: "localhost:8098", Path: "/badriak/"}
var TESTING_BUCKET = "transientBucket"
var TESTING_MULTI_BUCKET = "transientMultiBucket"

func testClient(t *testing.T) (c Client) {
	c, err := NewClient("", TESTING_RIAK)
	if err != nil {
		t.Fatalf("Error getting client: %v", err)
	}
	return
}

func badClient(t *testing.T) (c Client) {
	c, err := NewClient("", BAD_RIAK)
	if err != nil {
		t.Fatalf("Error getting (bad) client: %v", err)
	}
	return
}


func fatalIf(t *testing.T, test bool, s string, v ...interface{}) {
	if test {
		t.Fatalf(s, v...)
	}
}
