package runtime

var (
	_ Context = (*defaultContext)(nil)
)

var (
	ctx = createDefaultContext()
)

type Context interface {
	Buffer() Buffer
}

type defaultContext struct {
	buf *defaultBuffer
}

func (c *defaultContext) Buffer() Buffer {
	return c.buf
}

func DefaultContext() Context {
	return ctx
}

func createDefaultContext() *defaultContext {
	return &defaultContext{
		buf: createDefaultBuffer(),
	}
}
