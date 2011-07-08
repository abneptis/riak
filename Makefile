include $(GOROOT)/src/Make.inc

TARG=github.com/abneptis/riak
GOFILES=\
		client.go\
		dispatch_request.go\
		props.go\

DEPS=\

CLEANFILES+=\

include $(GOROOT)/src/Make.pkg

