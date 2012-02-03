include $(GOROOT)/src/Make.inc
TARG=	donut
GOFILES= 	util.go cluster.go listener.go balancer.go

include $(GOROOT)/src/Make.pkg
