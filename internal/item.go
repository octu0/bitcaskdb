package internal

type Item struct {
	FileID int   `json:"fileid"`
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}
