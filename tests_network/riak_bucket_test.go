package riak

import "testing"
import "http"
import "os"

func TestCreateRiakBucketNoParams(t *testing.T) {
	c := testClient(t)
	err := SetBucket(c, TESTING_BUCKET, DefaultProperties(), nil)
	if err != nil {
		t.Fatalf("Got an error creating bucket with default params")
	}
}

func TestCreateRiakBucketNval(t *testing.T) {
	c := testClient(t)
	err := SetBucket(c, TESTING_BUCKET, Properties{NVal: 3}, nil)
	fatalIf(t, err != nil, "Got an error creating bucket: %v", err)
}


func TestRiakListKeys(t *testing.T) {
	c := testClient(t)
	outch := make(chan string)
	keys := []string{}
	done := make(chan int)
	go func(){
		for i := range(outch){
			keys = append(keys, i)
		}
		done <- 1
	}()
	err := PutItem(c, TESTING_BUCKET, "TestRiakListKeys", []byte("hello world"), nil, nil, nil)
	fatalIf(t, err != nil, "Couldn't put a key in bucket to test: %v", err)
	err = ListKeys(c, TESTING_BUCKET, outch, nil)
	fatalIf(t, err != nil, "Got an error listing bucket: %v", err)
	<- done
	found := false
	for i := range(keys) {
		if keys[i] == "TestRiakListKeys" { found = true }
	}
	fatalIf(t, !found, "Didn't find key (got %v)", keys)

}

func TestRiakGetBucket(t *testing.T) {
	c := testClient(t)
	err := SetBucket(c, TESTING_BUCKET, DefaultProperties(), nil) 
	fatalIf(t, err != nil, "Got an error getting bucket info: %v", err)

	resp, err := GetBucket(c, TESTING_BUCKET, true, true, nil)
	fatalIf(t, err != nil, "Got an error enumerating properties: %v", err)

	fatalIf(t, resp.Props["name"].(string) != TESTING_BUCKET, "Got wrong value for bucket name??: %v", resp.Props["name"])
	fatalIf(t, resp.Props["n_val"].(float64) != 3, "Got wrong value for n_val??: %v", resp.Props["n_val"])

}

func TestRiakPing(t *testing.T) {
	c := testClient(t)
	err := Ping(c, nil)
	fatalIf(t, err != nil, "Couldn't ping!")
}

func TestRiakPingBad(t *testing.T) {
	c := badClient(t)
	err := Ping(c, nil)
	fatalIf(t, err == nil, "Unexpected good response from BAD_RIAK ping!")
}

func TestRiakListBuckets(t *testing.T) {
	c := testClient(t)
	// a key must be present to be counted
	err := PutItem(c, TESTING_BUCKET, "TestRiakListBucket", []byte("hello world"), nil, nil, nil)
	fatalIf(t, err != nil, "Got an error putting item: %v", err)
	names, err := ListBuckets(c, nil)
	fatalIf(t, err != nil, "Error listing buckets: %v", err)
	var found bool
	for i := range names {
		if names[i] == TESTING_BUCKET {
			found = true
			break
		}
	}
	if !found {
		t.Logf("Didn't find testing bucket (did find %v)", names)
	}
	testDeleteItem(c, t, TESTING_BUCKET, "TestRiakListBucket")
}


func TestPutItem(t *testing.T) {
	c := testClient(t)
	err := PutItem(c, TESTING_BUCKET, "TestPutSingleItem", []byte("hello world"), nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error putting item to bucket: %v", err)
	testDeleteItem(c, t, TESTING_BUCKET, "TestPutSingleItem")
}

func TestDeleteUnknownItem(t *testing.T) {
	c := testClient(t)
	err := DeleteItem(c, TESTING_BUCKET, "TestGetUnknownItem", nil, nil)
	fatalIf(t, err != ErrUnknownKey, "expected 'unknown key' deleting item from bucket: %v", err)
}

func testDeleteItem(c Client, t *testing.T, bucket, key string) {
	err := DeleteItem(c, bucket, key, nil, nil)
	fatalIf(t, err != nil, "Unexpected error deleting item from bucket: %v", err)
}


// MULTI-VALUE funcs
func TestCreateRiakMulti(t *testing.T) {
	c := testClient(t)
	isTrue := true
	err := SetBucket(c, TESTING_MULTI_BUCKET, Properties{AllowMulti: &isTrue}, nil)
	fatalIf(t, err != nil, "Got an error creating multi-bucket: %v", err)
}

func TestGetUnknownMultiItem(t *testing.T) {
	c := testClient(t)
	err := GetMultiItem(c, TESTING_MULTI_BUCKET, "TestGetUnknownItem", nil, nil, make(chan *http.Response), nil)
	fatalIf(t, err != ErrUnknownKey, "Unexpected error getting missing-item from buckets: %v", err)
}

func TestPutMultiItems(t *testing.T) {
	c := testClient(t)
	err := PutItem(c, TESTING_MULTI_BUCKET, "TestPutMultiItem", []byte("hello world"), nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error putting new item into bucket: %v", err)
	err = PutItem(c, TESTING_MULTI_BUCKET, "TestPutMultiItem", []byte("hello new world"), nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error putting new item into bucket: %v", err)
	testDeleteItem(c, t, TESTING_MULTI_BUCKET, "TestPutMultiItem")
}

func TestGetMultiItems(t *testing.T) {
	c := testClient(t)
	DeleteItem(c, TESTING_MULTI_BUCKET, "TestGetMultiItem", nil, nil)
	err := PutItem(c, TESTING_MULTI_BUCKET, "TestGetMultiItem", []byte("hello world"), nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error putting new item into bucket: %v", err)
	err = PutItem(c, TESTING_MULTI_BUCKET, "TestGetMultiItem", []byte("hello new world"), nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error putting new item into bucket: %v", err)

	mych := make(chan *http.Response)
	done := make(chan int)

	go func() {
		hw_found := 0
		hnw_found := 0
		for i := range mych {
			buff := make([]byte, 8192)
			n, err := i.Body.Read(buff)
			fatalIf(t, i.Header.Get("X-Riak-Vclock") == "", "No vclock in the individual pages")

			fatalIf(t, err != nil && err != os.EOF, "Error reading body: %v", err)
			if string(buff[0:n]) == "hello world" {
				hw_found++
			}
			if string(buff[0:n]) == "hello new world" {
				hnw_found++
			}
		}
		fatalIf(t, hw_found != 1, "Wrong number of 'hello world's found: %d'", hw_found)
		fatalIf(t, hnw_found != 1, "Wrong number of 'hello new world's found: %d'", hnw_found)
		done <- 1
	}()

	err = GetMultiItem(c, TESTING_MULTI_BUCKET, "TestGetMultiItem", nil, nil, mych, nil)
	fatalIf(t, err != nil, "Unexpected error getting multiitem from bucket: %v", err)
	<-done
	testDeleteItem(c, t, TESTING_MULTI_BUCKET, "TestGetMultiItem")
}


// This is actually a fairly long test to achieve failure.
// Riak considers it an error to requiest an unvaried item via a multipart/mixed message
func TestGetOneMultiItems(t *testing.T) {
	c := testClient(t)
	DeleteItem(c, TESTING_MULTI_BUCKET, "TestGetOneMultiItem", nil, nil)
	err := PutItem(c, TESTING_MULTI_BUCKET, "TestGetOneMultiItem", []byte("hello world"), nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error putting new item into bucket: %v", err)

	mych := make(chan *http.Response)
	done := make(chan int)

	go func() {
		hw_found := 0
		hnw_found := 0
		for i := range mych {
			buff := make([]byte, 8192)
			n, err := i.Body.Read(buff)
			fatalIf(t, i.Header.Get("X-Riak-Vclock") == "", "No vclock in the individual pages")

			fatalIf(t, err != nil && err != os.EOF, "Error reading body: %v", err)
			i.Body.Close()
			if string(buff[0:n]) == "hello world" {
				hw_found++
			}
			if string(buff[0:n]) == "hello new world" {
				hnw_found++
			}
		}
		fatalIf(t, hw_found != 0, "Wrong number of 'hello world's found: %d'", hw_found)
		fatalIf(t, hnw_found != 0, "Wrong number of 'hello new world's found: %d'", hnw_found)
		done <- 1
	}()

	err = GetMultiItem(c, TESTING_MULTI_BUCKET, "TestGetOneMultiItem", nil, nil, mych, nil)
	fatalIf(t, err != ErrUnacceptable, "Unexpected error getting multiitem from bucket: %v", err)
	_, err = GetItem(c, TESTING_MULTI_BUCKET, "TestGetOneMultiItem", nil, nil, nil)
	fatalIf(t, err != nil, "Unexpected error getting one item from multiitem bucket: %v", err)
	<-done
}
