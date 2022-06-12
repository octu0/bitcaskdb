package context

var (
	defaultContext = createDefaultContext()
)

type Context struct {
	buf *Buffer
}

func (c *Context) Buffer() *Buffer {
	return c.buf
}

func Default() *Context {
	return defaultContext
}

func createDefaultContext() *Context {
	return &Context{
		buf: createDefaultBuffer(),
	}
}
