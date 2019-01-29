package log

// FmtLogger is a trivial interface for passing a logger to
// a cluster. This is not something to use outside of donut.
type FmtLogger interface {
	Printf(format string, v ...interface{})
}
