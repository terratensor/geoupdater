// internal/core/domain/document_test.go
package domain

import (
	"strings"
	"testing"
)

func TestNewGeoUpdateData(t *testing.T) {
	tests := []struct {
		name        string
		docID       uint64
		strings     []string
		uint64s     []uint64 // меняем с []int64 на []uint64
		expectError bool
	}{
		{
			name:        "valid data",
			docID:       123,
			strings:     []string{"abc", "def"},
			uint64s:     []uint64{123, 456}, // uint64 литералы
			expectError: false,
		},
		{
			name:        "empty doc_id",
			docID:       0,
			strings:     []string{"abc"},
			uint64s:     []uint64{123},
			expectError: true,
		},
		{
			name:        "count mismatch",
			docID:       123,
			strings:     []string{"abc", "def"},
			uint64s:     []uint64{123},
			expectError: true,
		},
		{
			name:        "empty string geohash",
			docID:       123,
			strings:     []string{"abc", ""},
			uint64s:     []uint64{123, 456},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := NewGeoUpdateData(tt.docID, tt.strings, tt.uint64s)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectError && data == nil {
				t.Error("expected data but got nil")
			}
		})
	}
}

func TestDocumentMergeReplace(t *testing.T) {
	doc := &Document{
		ID:              123,
		GeohashesString: "abc, def",
		GeohashesUint64: []uint64{123, 456}, // uint64
	}

	data := &GeoUpdateData{
		DocID:           123,
		GeohashesString: []string{"xyz", "uvw"},
		GeohashesUint64: []uint64{789, 101}, // uint64
	}

	err := doc.Merge(data, ModeReplace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Проверяем замену строк
	expected := "uvw, xyz"
	if doc.GeohashesString != expected {
		t.Errorf("expected %q, got %q", expected, doc.GeohashesString)
	}

	// Проверяем замену uint64
	if len(doc.GeohashesUint64) != 2 {
		t.Errorf("expected 2 uint64, got %d", len(doc.GeohashesUint64))
	}
	if doc.GeohashesUint64[0] != 101 && doc.GeohashesUint64[1] != 789 {
		t.Errorf("unexpected uint64 values: %v", doc.GeohashesUint64)
	}
}

func TestDocumentMergeMerge(t *testing.T) {
	doc := &Document{
		ID:              123,
		GeohashesString: "abc, def",
		GeohashesUint64: []uint64{123, 456},
	}

	data := &GeoUpdateData{
		DocID:           123,
		GeohashesString: []string{"def", "xyz"}, // def дублируется
		GeohashesUint64: []uint64{456, 789},     // 456 дублируется
	}

	err := doc.Merge(data, ModeMerge)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Проверяем уникальность строк
	parts := strings.Split(doc.GeohashesString, ", ")
	if len(parts) != 3 { // abc, def, xyz
		t.Errorf("expected 3 unique strings, got %d: %v", len(parts), parts)
	}

	// Проверяем уникальность uint64
	if len(doc.GeohashesUint64) != 3 { // 123, 456, 789
		t.Errorf("expected 3 unique uint64, got %d: %v", len(doc.GeohashesUint64), doc.GeohashesUint64)
	}

	// Проверяем сортировку
	for i := 1; i < len(doc.GeohashesUint64); i++ {
		if doc.GeohashesUint64[i] < doc.GeohashesUint64[i-1] {
			t.Errorf("uint64 slice not sorted: %v", doc.GeohashesUint64)
		}
	}
}

func TestDocumentGetGeohashCount(t *testing.T) {
	tests := []struct {
		name     string
		doc      Document
		expected int
	}{
		{
			name: "empty",
			doc: Document{
				GeohashesString: "",
			},
			expected: 0,
		},
		{
			name: "single",
			doc: Document{
				GeohashesString: "abc",
			},
			expected: 1,
		},
		{
			name: "multiple",
			doc: Document{
				GeohashesString: "abc, def, xyz",
			},
			expected: 3,
		},
		{
			name: "with spaces",
			doc: Document{
				GeohashesString: "abc, def,  xyz  ",
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.doc.GetGeohashCount(); got != tt.expected {
				t.Errorf("GetGeohashCount() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestDocumentIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		doc      Document
		expected bool
	}{
		{
			name: "completely empty",
			doc: Document{
				GeohashesString: "",
				GeohashesUint64: nil,
			},
			expected: true,
		},
		{
			name: "only string",
			doc: Document{
				GeohashesString: "abc",
				GeohashesUint64: nil,
			},
			expected: false,
		},
		{
			name: "only uint64",
			doc: Document{
				GeohashesString: "",
				GeohashesUint64: []uint64{123},
			},
			expected: false,
		},
		{
			name: "both",
			doc: Document{
				GeohashesString: "abc",
				GeohashesUint64: []uint64{123},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.doc.IsEmpty(); got != tt.expected {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.expected)
			}
		})
	}
}
