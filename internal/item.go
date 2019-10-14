package internal

// Item represents the location of the value on disk. This is used by the
// internal Adaptive Radix Tree to hold an in-memory structure mapping keys to
// locations on disk of where the value(s) can be read from.
type Item struct {
	FileID int   `json:"fileid"`
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}
