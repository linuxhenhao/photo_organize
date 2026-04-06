package hasher

import (
	"math/bits"
	"sync"
)

// HammingDistance computes the Hamming distance between two uint64 values.
func HammingDistance(a, b uint64) int {
	return bits.OnesCount64(a ^ b)
}

// BKNode represents a single node in the BK-Tree.
type BKNode struct {
	Hash     uint64
	Path     string
	Size     int64
	Children map[int]*BKNode
}

// BKTree structure with read-write mutex for concurrency.
type BKTree struct {
	Root  *BKNode
	mutex sync.RWMutex
}

// NewBKTree initializes a new BK-Tree.
func NewBKTree() *BKTree {
	return &BKTree{}
}

// Add safely inserts a new hash, path, and size into the BK-Tree.
func (t *BKTree) Add(hash uint64, path string, size int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.Root == nil {
		t.Root = &BKNode{
			Hash:     hash,
			Path:     path,
			Size:     size,
			Children: make(map[int]*BKNode),
		}
		return
	}

	curr := t.Root
	for {
		dist := HammingDistance(curr.Hash, hash)
		if dist == 0 && curr.Hash == hash && curr.Path == path {
			return // exact duplicate entry, ignore
		}

		// If a child with this distance does not exist, insert it here
		if child, exists := curr.Children[dist]; !exists {
			curr.Children[dist] = &BKNode{
				Hash:     hash,
				Path:     path,
				Size:     size,
				Children: make(map[int]*BKNode),
			}
			break
		} else {
			// Traverse down
			curr = child
		}
	}
}

// MatchResult holds a match from the BK-Tree search.
type MatchResult struct {
	Distance int
	Hash     uint64
	Path     string
	Size     int64
}

// Search allows a query hash to find matches within a maxDistance.
func (t *BKTree) Search(query uint64, maxDistance int) []MatchResult {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var results []MatchResult
	if t.Root == nil {
		return results
	}

	var search func(node *BKNode)
	search = func(node *BKNode) {
		if node == nil {
			return
		}

		dist := HammingDistance(node.Hash, query)
		if dist <= maxDistance {
			results = append(results, MatchResult{
				Distance: dist,
				Hash:     node.Hash,
				Path:     node.Path,
				Size:     node.Size,
			})
		}

		minDist := dist - maxDistance
		maxDist := dist + maxDistance

		for childDist, child := range node.Children {
			if childDist >= minDist && childDist <= maxDist {
				search(child)
			}
		}
	}

	search(t.Root)
	return results
}
