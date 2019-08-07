package bitcask

func Example() {
	_, _ = Open("path/to/db")
}

func Example_withOptions() {
	opts := []Option{
		WithMaxKeySize(1024),
		WithMaxValueSize(4096),
	}
	_, _ = Open("path/to/db", opts...)
}
