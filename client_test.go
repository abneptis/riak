package riak

import "testing"
import "path"
import "json"

func TestPingRequest(t *testing.T) {
	c := testClient(t)
	req := pingRequest(c)
	fatalIf(t, req.ContentLength != 0, "Got a bad content length: %d", req.ContentLength)
	expPath := path.Join(TESTING_RIAK.Path)
	fatalIf(t, req.URL.Path != expPath, "Bad path in create-bucket request: %s [wanted: %s]", req.URL.Path, expPath)
	return
}


func TestCreateBucketRequest(t *testing.T) {
	c := testClient(t)
	req, err := createBucketRequest(c, TESTING_BUCKET, nil)
	fatalIf(t, err != nil, "Got an error creating bucket: %v", err)
	fatalIf(t, req.Method != "PUT", "Wrong method in create bucket", req.Method)
	fatalIf(t, req.ContentLength <= 0, "Got a bad content length: %d", req.ContentLength)
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET)
	fatalIf(t, req.URL.Path != expPath, "Bad path in create-bucket request: %s [wanted: %s]", req.URL.Path, expPath)
	fatalIf(t, req.Header.Get("Content-Type") != "application/json", "Unexpected content-type: %s", req.Header.Get("Content-Type"))
	return
}

func TestCreateMultiBucketRequest(t *testing.T) {
	c := testClient(t)
	req, err := createBucketRequest(c, TESTING_BUCKET, map[string]interface{}{"allow_mult": true})
	fatalIf(t, err != nil, "Got an error creating multi-bucket: %v", err)
	fatalIf(t, req.Method != "PUT", "Wrong method in create bucket", req.Method)
	fatalIf(t, req.ContentLength <= 0, "Got a bad content length: %d", req.ContentLength)
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET)
	fatalIf(t, req.URL.Path != expPath, "Bad path in create-bucket request: %s [wanted: %s]", req.URL.Path, expPath)
	fatalIf(t, req.Header.Get("Content-Type") != "application/json", "Unexpected content-type: %s", req.Header.Get("Content-Type"))
	exp := map[string]map[string]bool{}
	err = json.NewDecoder(req.Body).Decode(&exp)
	fatalIf(t, err != nil, "Couldn't decode body: %v", err)
	fatalIf(t, exp["props"] == nil, "Didn't encode props!", err)
	fatalIf(t, !exp["props"]["allow_mult"], "alow_mult is false!")

	return
}


func TestGetBucketRequest(t *testing.T) {
	c := testClient(t)
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET)
	// typical operation
	req := getBucketRequest(c, TESTING_BUCKET, true, false)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.URL.Path != expPath, "Bad path in create-bucket request: %s [wanted: %s]", req.URL.Path, expPath)
	fatalIf(t, req.FormValue("keys") != "", "Keys should not be set in 'false' case (got %s)", req.FormValue("keys"))
	fatalIf(t, req.FormValue("props") != "", "Props need not be set in 'true' case (got %s)", req.FormValue("props"))

	// keys-only
	req = getBucketRequest(c, TESTING_BUCKET, false, true)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.URL.Path != expPath, "Bad path in create-bucket request: %s [wanted: %s]", req.URL.Path, expPath)
	fatalIf(t, req.FormValue("keys") != "true", "Keys must be 'true' in true case [got '%s']", req.FormValue("keys"))
	fatalIf(t, req.FormValue("props") != "false", "Props must be 'false' in 'false' case [got '%s']", req.FormValue("props"))
}

func TestGetBucketsRequest(t *testing.T) {
	c := testClient(t)
	expPath := path.Join(TESTING_RIAK.Path, "riak")
	// typical operation
	req := listBucketsRequest(c)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.URL.Path != expPath, "Bad path in create-bucket request: %s [wanted: %s]", req.URL.Path, expPath)
	fatalIf(t, req.FormValue("buckets") != "true", "Didn't find buckets parameter in the request!")
}

func TestGetMultiItemRequest(t *testing.T) {
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET, "TestGetItemRequest")
	c := testClient(t)
	req := getMultiItemRequest(c, TESTING_BUCKET, "TestGetItemRequest", nil, nil)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.URL.Path != expPath, "Unexpected path: %s", expPath)
	fatalIf(t, req.URL.RawQuery != "", "Unexpected raw-query: %s", req.URL.RawQuery)
	fatalIf(t, req.Header.Get("Accept") != "multipart/mixed", "Wrong accept header: %s", req.Header.Get("Accept"))

}

func TestPutItemRequest(t *testing.T) {
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET, "TestPutItemRequest")
	c := testClient(t)
	req := putItemRequest(c, TESTING_BUCKET, "TestPutItemRequest", []byte("hello world"), nil, nil)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.Method != "PUT", "Unexpected method: %s", req.Method)
	fatalIf(t, req.URL.Path != expPath, "Unexpected path: %s", expPath)
	fatalIf(t, req.URL.RawQuery != "", "Unexpected raw-query: %s", req.URL.RawQuery)
	fatalIf(t, req.Header.Get("Content-Type") == "", "Content-type must be set: (got none)")
}

func TestDeleteItemRequest(t *testing.T) {
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET, "TestPutItemRequest")
	c := testClient(t)
	req := deleteItemRequest(c, TESTING_BUCKET, "TestPutItemRequest", nil)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.Method != "DELETE", "Unexpected method: %s", req.Method)
	fatalIf(t, req.URL.Path != expPath, "Unexpected path: %s", expPath)
	fatalIf(t, req.URL.RawQuery != "", "Unexpected raw-query: %s", req.URL.RawQuery)
}

func TestGetSingleItemRequest(t *testing.T) {
	expPath := path.Join(TESTING_RIAK.Path, "riak", TESTING_BUCKET, "TestGetSingleItemRequest")
	c := testClient(t)
	req := putItemRequest(c, TESTING_BUCKET, "TestGetSingleItemRequest", []byte("hello world"), nil, nil)
	fatalIf(t, req == nil, "Got a nil request back!")
	fatalIf(t, req.Method != "PUT", "Unexpected method: %s", req.Method)
	fatalIf(t, req.URL.Path != expPath, "Unexpected path: %s", expPath)
	fatalIf(t, req.URL.RawQuery != "", "Unexpected raw-query: %s", req.URL.RawQuery)
	fatalIf(t, req.Header.Get("Content-Type") == "", "Content-type must be set: (got none)")
}
