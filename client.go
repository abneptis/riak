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

func GetBucket(c Client, name string, getprops, getkeys bool, cc *http.ClientConn) (br BucketDetails, err os.Error) {
	req := getBucketRequest(c, name, getprops, getkeys)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		200: func(r *http.Response) (err os.Error) {
			err = json.NewDecoder(r.Body).Decode(&br)
			return
		},
		-1: Failf("Server refused enumeration"),
	})
	return
}


func createBucketRequest(c Client, name string, props map[string]interface{}) (req *http.Request, err os.Error) {
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

func Failf(s string, v ...interface{}) func(*http.Response) os.Error {
	return func(*http.Response) os.Error {
		return os.NewError(fmt.Sprintf(s, v...))
	}
}

func DebugFailf(out io.Writer, body bool, s string, v ...interface{}) func(*http.Response) os.Error {
	return func(in *http.Response) os.Error {
		ob, _ := http.DumpResponse(in, body)
		out.Write(ob)
		return os.NewError(fmt.Sprintf(s, v...))
	}
}

func Okf(*http.Response) os.Error {
	return nil
}

func pingRequest(c Client) (req *http.Request) {
	// note there's no /riak/ on a PING
	req = c.request("GET", "/", nil, nil)
	return
}

func CreateBucket(c Client, name string, props map[string]interface{}, cc *http.ClientConn) (err os.Error) {
	req, err := createBucketRequest(c, name, props)
	if err == nil {
		err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
			204: func(*http.Response) os.Error { return nil },
			-1:  Failf("Server refused creation"),
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


func dispatchRequest(cc *http.ClientConn, request *http.Request, rc map[int]func(*http.Response) os.Error) (err os.Error) {
	if cc == nil {
		cc, err = dialHTTP(request.Host, request.URL.Scheme)
	}
	if err != nil {
		return
	}
	resp, err := cc.Do(request)
	if err == nil {
		if rcf, ok := rc[resp.StatusCode]; ok {
			return rcf(resp)
		}
		if rcf, ok := rc[-1]; ok {
			return rcf(resp)
		}
		err = os.NewError("No response handler for code")
	}
	return
}

func Ping(c Client, cc *http.ClientConn) (err os.Error) {
	// note there's no /riak/ on a PING
	req := pingRequest(c)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		200: Okf,
		-1:  Failf("Unexpected response from ping"),
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
		-1: Failf("ListBuckets failed"),
	})
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
		-1:  DebugFailf(os.Stdout, true, "GetItem failed" + req.URL.String()),
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
	hdrs.Set("Accept", "multipart/mixed")
	req = c.request("GET", c.keyPath(bucket, key), hdrs, parms)
	return
}


// for hdrs, only include headers listed as optional from 'http://wiki.basho.com/HTTP-Fetch-Object.html'
func GetMultiItem(c Client, bucket, key string, hdrs http.Header, parms http.Values, respch chan<- *http.Response, cc *http.ClientConn) (err os.Error) {
	req := getMultiItemRequest(c, bucket, key, hdrs, parms)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		-1:  DebugFailf(os.Stdout, true, "GetMultiItem failed"),
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
				return DebugFailf(os.Stdout, true, "Server gave us a 300, but not a multipart/mixed message\t"+mtype)(resp)
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

// for hdrs, only include headers listed as optional from 'http://wiki.basho.com/HTTP-Fetch-Object.html'
func putItemRequest(c Client, bucket, key string, body []byte, hdrs http.Header, parms http.Values) (req *http.Request) {
	if hdrs == nil {
		hdrs = http.Header{"Content-Type": []string{"application/binary"}}
	}
	req = c.request("PUT", c.keyPath(bucket, key), hdrs, parms)
	req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	req.ContentLength = int64(len(body))
	return
}

// for hdrs, only include headers listed as optional from 'http://wiki.basho.com/HTTP-Fetch-Object.html'
func PutItem(c Client, bucket, key string, body []byte, hdrs http.Header, parms http.Values, cc *http.ClientConn) (err os.Error) {
	req := putItemRequest(c, bucket, key, body, hdrs, parms)
	err = dispatchRequest(cc, req, map[int]func(*http.Response) os.Error{
		-1:  DebugFailf(os.Stdout, true, "PutItem	failed:" + req.URL.String()),
		412: func(*http.Response) os.Error { return ErrPreconditionFailed },
		204: Okf,
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
		-1:  DebugFailf(os.Stdout, true, "DeleteItem failed"),
		404: func(*http.Response) os.Error { return ErrUnknownKey },
		204: Okf,
	})
	return
}
