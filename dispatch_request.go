package riak

import (
	"http"
	"os"
)

// TODO: This function could use some dedicated testing


func dispatchRequest(cc *http.ClientConn, request *http.Request, rc map[int]func(*http.Response) os.Error) (err os.Error) {
  if cc == nil {
    cc, err = dialHTTP(request.Host, request.URL.Scheme)
  }
  if err != nil {
    return
  }
  resp, err := cc.Do(request)
  if resp !=	nil && (err == nil || err == http.ErrPersistEOF ) {
    if rcf, ok := rc[resp.StatusCode]; ok {
      return rcf(resp)
    }
    if rcf, ok := rc[-1]; ok {
      return rcf(resp)
    }
		// If you don't handle your errors, you won't be able to spot a persistant connection closing!
    err = os.NewError("No response handler for code")
  }
  return
}

