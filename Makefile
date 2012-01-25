include $(GOROOT)/src/Make.inc
TARG=	donut
GOFILES= 	util.go cluster.go listener.go

include $(GOROOT)/src/Make.pkg
