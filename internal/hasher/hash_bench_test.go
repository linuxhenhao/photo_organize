package hasher

import (
	"os"
	"path/filepath"
	"testing"
)

func benchmarkFixturePath(t *testing.B, parts ...string) string {
	t.Helper()

	path := filepath.Join(parts...)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture missing: %s: %v", path, err)
	}
	return path
}

func BenchmarkCalculateDHashARWFixture(b *testing.B) {
	path := benchmarkFixturePath(b, "..", "..", "test_data", "source", "DSC00903.ARW")
	if _, err := CalculateDHash(path); err != nil {
		b.Fatalf("warm-up CalculateDHash failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := CalculateDHash(path); err != nil {
			b.Fatalf("CalculateDHash failed: %v", err)
		}
	}
}

func BenchmarkCalculateFullPerceptionHashARWFixture(b *testing.B) {
	path := benchmarkFixturePath(b, "..", "..", "test_data", "source", "DSC00903.ARW")
	if _, err := CalculateFullPerceptionHash(path); err != nil {
		b.Fatalf("warm-up CalculateFullPerceptionHash failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := CalculateFullPerceptionHash(path); err != nil {
			b.Fatalf("CalculateFullPerceptionHash failed: %v", err)
		}
	}
}

func BenchmarkCalculateDHashJPEGFixture(b *testing.B) {
	path := benchmarkFixturePath(b, "..", "..", "test_data", "source_mock", "img_2023_05_01.jpg")
	if _, err := CalculateDHash(path); err != nil {
		b.Fatalf("warm-up CalculateDHash failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := CalculateDHash(path); err != nil {
			b.Fatalf("CalculateDHash failed: %v", err)
		}
	}
}

func BenchmarkCalculateFullPerceptionHashJPEGFixture(b *testing.B) {
	path := benchmarkFixturePath(b, "..", "..", "test_data", "source_mock", "img_2023_05_01.jpg")
	if _, err := CalculateFullPerceptionHash(path); err != nil {
		b.Fatalf("warm-up CalculateFullPerceptionHash failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := CalculateFullPerceptionHash(path); err != nil {
			b.Fatalf("CalculateFullPerceptionHash failed: %v", err)
		}
	}
}
