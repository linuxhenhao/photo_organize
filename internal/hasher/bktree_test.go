package hasher

import (
	"testing"
)

func TestHammingDistance(t *testing.T) {
	tests := []struct {
		a, b     uint64
		expected int
	}{
		{0, 0, 0},
		{0, 1, 1},
		{1, 1, 0},
		{0xFF, 0x00, 8},
		{0xF0, 0x0F, 8},
	}

	for _, tt := range tests {
		got := HammingDistance(tt.a, tt.b)
		if got != tt.expected {
			t.Errorf("HammingDistance(%x, %x) = %d; want %d", tt.a, tt.b, got, tt.expected)
		}
	}
}

func TestBKTree_AddAndSearch(t *testing.T) {
	tree := NewBKTree()

	// 1. Initial empty search
	results := tree.Search(0x1234, 5)
	if len(results) != 0 {
		t.Errorf("Empty tree returned %d results", len(results))
	}

	// 2. Add elements
	tree.Add(0x1000, "path1", 100)
	tree.Add(0x1001, "path2", 200) // distance 1 from 0x1000
	tree.Add(0x2000, "path3", 300) // distance 2 from 0x1000 (0x1000 ^ 0x2000 = 0x3000 -> 2 bits)
	tree.Add(0xFFFF, "path4", 400) // far away

	// 3. Search exact
	results = tree.Search(0x1000, 0)
	if len(results) != 1 || results[0].Path != "path1" {
		t.Errorf("Exact search failed, got %+v", results)
	}

	// 4. Search within distance 1
	results = tree.Search(0x1000, 1)
	if len(results) != 2 {
		t.Errorf("Search distance 1 failed, got %d results", len(results))
	}

	// 5. Search within distance 2
	results = tree.Search(0x1000, 2)
	// 0x1000 ^ 0x2000 = 0x3000 (0011 0000 0000 0000) -> 2 bits set
	if len(results) != 3 {
		t.Errorf("Search distance 2 failed, got %d results", len(results))
	}

	// 6. Test duplicate add (should be ignored)
	tree.Add(0x1000, "path1", 100)
	results = tree.Search(0x1000, 0)
	if len(results) != 1 {
		t.Errorf("Duplicate add increased result count: %d", len(results))
	}
}
