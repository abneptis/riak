include $(GOROOT)/src/Make.inc

TARG=github.com/abneptis/riak
GOFILES=\
		client.go\

DEPS=\
		../event/

CLEANFILES+=\

include $(GOROOT)/src/Make.pkg

