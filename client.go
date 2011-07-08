package riak

import (
	"bytes"
	"io/ioutil"
	"http"
	"fmt"
	"path"
	"os"
	"io"
	"json"
	"strconv"
	"net"
	"crypto/tls"
	"mime/multipart"
	"mime"
)

type Client struct {
	ClientId string
	RootURL  http.URL
}

var ErrUnknownKey = os.NewError("Unknown key")
var ErrPreconditionFailed = os.NewError("Precondition failed")
var ErrServiceUnavailable = os.NewError("Service unavailable")
var ErrBadRequest = os.NewError("Bad request")
var ErrUnacceptable = os.NewError("That's unacceptable!")

// If id is nil, os.Hostname().os.GetPid is used.  if os.Hostname returns an error,
// this error will be returned.
//
// (This is the only error that can be returned, so if a client-id is passed, no error will
// return)
func NewClient(id string, rooturl http.URL) (c Client, err os.Error) {
	c = Client{RootURL: rooturl, ClientId: id}
	if c.ClientId == "" {
		c.ClientId, err = os.Hostname()
		if err == nil {
			c.ClientId += "." + strconv.Itoa(os.Getpid())
		}
	}
	return
}

func (self Client) request(method string, p string, hdrs http.Header, vals http.Values) (req *http.Request) {
	req = &http.Request{
		Method: method,
		Host:   self.RootURL.Host,
		URL:    &http.URL{Path: path.Join(self.RootURL.Path, p)},
		Header: hdrs,
	}
	if method == "GET" {
		req.URL.RawQuery = vals.Encode()
	}
	// TODO: Deletes?
	if method == "POST" || method == "PUT" {
		req.Header.Set("X-Riak-ClientId", self.ClientId)
	}
	return
}

func (self Client) bucketPath(name string) string {
	return path.Join(self.RootURL.Path, "riak", name)
}

func (self Client) keyPath(bucket, name string) string {
	return path.Join(self.RootURL.Path, "riak", bucket, name)
}


// http://wiki.basho.com/HTTP-Get-Bucket-Properties.html
// http://wiki.basho.com/HTTP-List-Buckets.html
// See notes on HTTP-List-Buckets about usage of getkeys (e.g., don't do it in production)

func getBucketRequest(c Client, name string, getprops, getkeys bool) (req *http.Request) {
	hdrs := http.Header{"Content-Type": []string{"application/json"}}
	qry := http.Values{}
	if !getprops {
		qry.Set("props", "false")
	}
	if getkeys {
		qry.Set("keys", "true")
	}

	req = c.request("GET", c.bucketPath(name), hdrs, qry)
	return
}


type BucketDetails struct {
	Props map[string]interface{}
	Keys  []string
}

// NB: Using getkeys on a bucket with a large numer of keys will fail if you've not
// tweaked your riak install.  See ListKeys to use the streaming interface.
// ListKeys is generally discouraged in production (see http://wiki.basho.com/HTTP-List-Keys.html)
func GetBucket(c Client, name string, getprops, getkeys bool, cc *http.ClientConn) (br BucketDetails, err os.Error) {
	req := getBucketRequest(c, name, getprops, getkeys)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		200: func(r *http.Response) (err os.Error) {
			err = json.NewDecoder(r.Body).Decode(&br)
			return
		},
		-1: failf("Server refused enumeration"),
	})
	return
}


func setBucketRequest(c Client, name string, props Properties) (req *http.Request, err os.Error) {
	hdrs := http.Header{}
	hdrs.Set("Content-Type", "application/json")

	req = c.request("PUT", c.bucketPath(name), hdrs, nil)

	ob, err := json.Marshal(map[string]interface{}{"props": props})

	if err == nil {
		req.ContentLength = int64(len(ob))
		req.Body = ioutil.NopCloser(bytes.NewBuffer(ob))
	}
	return
}

func failf(s string, v ...interface{}) func(*http.Response) os.Error {
	return func(*http.Response) os.Error {
		return os.NewError(fmt.Sprintf(s, v...))
	}
}

func debugFailf(out io.Writer, body bool, s string, v ...interface{}) func(*http.Response) os.Error {
	return func(in *http.Response) os.Error {
		ob, _ := http.DumpResponse(in, body)
		out.Write(ob)
		return os.NewError(fmt.Sprintf(s, v...))
	}
}

func okf(*http.Response) os.Error {
	return nil
}

func pingRequest(c Client) (req *http.Request) {
	// note there's no /riak/ on a PING
	req = c.request("GET", "/", nil, nil)
	return
}

func SetBucket(c Client, name string, props Properties, cc *http.ClientConn) (err os.Error) {
	req, err := setBucketRequest(c, name, props)
	if err == nil {
		err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
			204: func(*http.Response) os.Error { return nil },
			-1:  failf("Server refused creation"),
		})
	}
	return
}

func dialHTTP(hoststring string, scheme string) (cc *http.ClientConn, err os.Error) {
	host, port, err := net.SplitHostPort(hoststring)
	if err != nil {
		return
	}
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "80"
		case "riak":
			port = "8098"
		default:
			err = os.NewError("Unknown scheme")
		}
	}
	if err != nil {
		return
	}
	var c net.Conn
	switch scheme {
	case "https":
		c, err = tls.Dial("tcp", host+":"+port, nil)
	default:
		c, err = net.Dial("tcp", host+":"+port)
	}
	if err == nil {
		cc = http.NewClientConn(c, nil)
	}
	return
}

func Ping(c Client, cc *http.ClientConn) (err os.Error) {
	// note there's no /riak/ on a PING
	req := pingRequest(c)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		200: okf,
		-1:  failf("Unexpected response from ping"),
	})
	return
}


func listBucketsRequest(c Client) (req *http.Request) {
	reqvals := http.Values{"buckets": []string{"true"}}
	req = c.request("GET", "riak", nil, reqvals)
	return
}

type listResponse struct {
	Buckets []string
}

func ListBuckets(c Client, cc *http.ClientConn) (names []string, err os.Error) {
	req := listBucketsRequest(c)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		200: func(r *http.Response) (err os.Error) {
			lr := listResponse{}
			err = json.NewDecoder(r.Body).Decode(&lr)
			if err == nil {
				names = lr.Buckets
			}
			return
		},
		-1: failf("ListBuckets failed"),
	})
	return
}

// ListKeys is generally discouraged in production (see http://wiki.basho.com/HTTP-List-Keys.html)
func ListKeys(c Client, b string, outch chan<- string, cc *http.ClientConn)(err os.Error){
	resp, err := GetItem(c, b, "",http.Header{"Accept":[]string{"application/json"}}, 
									http.Values{
										"keys":[]string{"stream"},
										"props":[]string{"false"},
									}, cc)
	if resp != nil && resp.StatusCode == 200 {
		

		dec := json.NewDecoder(resp.Body)
		bi := BucketDetails{}
		for err = dec.Decode(&bi); err == nil ; err = dec.Decode(&bi){
			for i := range(bi.Keys){
				outch <- bi.Keys[i]
			}
		}
		if err == os.EOF { err = nil }
	} else {
		return debugFailf(os.Stdout, true, "Unexpected response: %s", resp.Status)(resp)
	}
	close(outch)
	return

}

// for hdrs, only include headers listed as optional from 'http://wiki.basho.com/HTTP-Fetch-Object.html'
func getItemRequest(c Client, bucket, key string, hdrs http.Header, parms http.Values) (req *http.Request) {
	req = c.request("GET", c.keyPath(bucket, key), hdrs, parms)
	return
}

func GetItem(c Client, bucket, key string, hdrs http.Header, parms http.Values, cc *http.ClientConn) (resp *http.Response, err os.Error) {
	req := getItemRequest(c, bucket, key, hdrs, parms)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		-1:  debugFailf(os.Stdout, true, "GetItem failed"+req.URL.String()),
		404: func(*http.Response) os.Error { return ErrUnknownKey },
		200: func(rresp *http.Response) (err os.Error) {
			// This doesn't actually happen unless someone else has resolved the item for us, but we'll
			// take it if it happens.
			resp = rresp
			return
		},
	})
	return
}


// for hdrs, only include headers listed as optional from 'http://wiki.basho.com/HTTP-Fetch-Object.html'
func getMultiItemRequest(c Client, bucket, key string, hdrs http.Header, parms http.Values) (req *http.Request) {
	if hdrs == nil {
		hdrs = http.Header{}
	}
	if hdrs.Get("Accept") == "" {
		hdrs.Set("Accept", "multipart/mixed")
	}
	req = c.request("GET", c.keyPath(bucket, key), hdrs, parms)
	return
}


// for hdrs, see 'http://wiki.basho.com/HTTP-Fetch-Object.html'
// NB: If no accept is set, we will choose multipart/mixed.
func GetMultiItem(c Client, bucket, key string, hdrs http.Header, parms http.Values, respch chan<- *http.Response, cc *http.ClientConn) (err os.Error) {
	req := getMultiItemRequest(c, bucket, key, hdrs, parms)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		-1:  debugFailf(os.Stdout, true, "GetMultiItem failed"),
		400: func(*http.Response) os.Error { return ErrBadRequest },
		404: func(*http.Response) os.Error { return ErrUnknownKey },
		406: func(*http.Response) os.Error { return ErrUnacceptable },
		503: func(*http.Response) os.Error { return ErrServiceUnavailable },
		200: func(resp *http.Response) (err os.Error) {
			// This doesn't actually happen unless someone else has resolved the item for us, but we'll
			// take it if it happens.
			respch <- resp
			return
		},
		300: func(resp *http.Response) (err os.Error) {
			mtype, mparms := mime.ParseMediaType(resp.Header.Get("Content-Type"))
			if mtype != "multipart/mixed" {
				return debugFailf(os.Stdout, true, "Server gave us a 300, but not a multipart/mixed message\t"+mtype)(resp)
			}
			if err == nil && mparms["boundary"] == "" {
				err = os.NewError("No boundry name found in content-type")
			}
			if err == nil {
				mpart := multipart.NewReader(io.LimitReader(resp.Body, resp.ContentLength), mparms["boundary"])
				var part *multipart.Part
				for part, err = mpart.NextPart(); err == nil; part, err = mpart.NextPart() {
					// if we don't swallow the reader now, the caller may not get their bits (multipart closes when we call NextPart()).
					buff := bytes.NewBuffer(nil)
					n, _ := buff.ReadFrom(part)

					rr := &http.Response{
						Body:          ioutil.NopCloser(buff),
						Header:        http.Header(part.Header),
						ContentLength: int64(n),
					}
					// Riak doesn't include a vclock in the sub-headers, and readers may want to use them.
					rr.Header.Set("X-Riak-Vclock", resp.Header.Get("X-Riak-Vclock"))
					respch <- rr
				}
				if err == os.EOF {
					err = nil
				}
			}
			return
		},
	})
	close(respch)

	return
}


func putItemRequest(c Client, bucket, key string, body []byte, hdrs http.Header, parms http.Values) (req *http.Request) {
	if hdrs == nil {
		hdrs = http.Header{"Content-Type": []string{"application/binary"}}
	}
	req = c.request("PUT", c.keyPath(bucket, key), hdrs, parms)
	req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	req.ContentLength = int64(len(body))
	return
}

// for hdrs, you can include any header (see 'http://wiki.basho.com/HTTP-Fetch-Object.html' for riak specific headers)
func PutItem(c Client, bucket, key string, body []byte, hdrs http.Header, parms http.Values, cc *http.ClientConn) (err os.Error) {
	req := putItemRequest(c, bucket, key, body, hdrs, parms)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		-1: debugFailf(os.Stdout, true, "PutItem	failed:"+req.URL.String()),
		412: func(*http.Response) os.Error { return ErrPreconditionFailed },
		204: okf,
	})
	return
}

func deleteItemRequest(c Client, bucket, key string, parms http.Values) (req *http.Request) {
	req = c.request("DELETE", c.keyPath(bucket, key), nil, parms)
	return
}


func DeleteItem(c Client, bucket, key string, parms http.Values, cc *http.ClientConn) (err os.Error) {
	req := deleteItemRequest(c, bucket, key, parms)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		-1:  debugFailf(os.Stdout, true, "DeleteItem failed"),
		404: func(*http.Response) os.Error { return ErrUnknownKey },
		204: okf,
	})
	return
}
