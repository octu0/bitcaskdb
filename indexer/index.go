package indexer

import (
	art "github.com/plar/go-adaptive-radix-tree"
)

// Indexer is an interface for loading and saving the index (an Adaptive Radix Tree)
type Indexer interface {
	Load(path string) (art.Tree, bool, error)
	Save(t art.Tree, path string) error
}
