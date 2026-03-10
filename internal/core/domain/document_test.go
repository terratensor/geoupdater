// internal/core/domain/document_test.go
package domain

import (
	"strings"
	"testing"
)

func TestNewGeoUpdateData(t *testing.T) {
	tests := []struct {
		name        string
		docID       string
		strings     []string
		uint64s     []int64
		expectError bool
	}{
		{
			name:        "valid data",
			docID:       "123",
			strings:     []string{"abc", "def"},
			uint64s:     []int64{123, 456},
			expectError: false,
		},
		{
			name:        "empty doc_id",
			docID:       "",
			strings:     []string{"abc"},
			uint64s:     []int64{123},
			expectError: true,
		},
		{
			name:        "count mismatch",
			docID:       "123",
			strings:     []string{"abc", "def"},
			uint64s:     []int64{123},
			expectError: true,
		},
		{
			name:        "empty string geohash",
			docID:       "123",
			strings:     []string{"abc", ""},
			uint64s:     []int64{123, 456},
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
		ID:              "123",
		GeohashesString: "abc, def",
		GeohashesUint64: []int64{123, 456},
	}

	data := &GeoUpdateData{
		DocID:           "123",
		GeohashesString: []string{"xyz", "uvw"},
		GeohashesUint64: []int64{789, 101},
	}

	err := doc.Merge(data, ModeReplace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Проверяем замену строк
	expected := "uvw, xyz" // отсортировано
	if doc.GeohashesString != expected {
		t.Errorf("expected %q, got %q", expected, doc.GeohashesString)
	}

	// Проверяем замену uint64
	if len(doc.GeohashesUint64) != 2 {
		t.Errorf("expected 2 uint64, got %d", len(doc.GeohashesUint64))
	}
}

func TestDocumentMergeMerge(t *testing.T) {
	doc := &Document{
		ID:              "123",
		GeohashesString: "abc, def",
		GeohashesUint64: []int64{123, 456},
	}

	data := &GeoUpdateData{
		DocID:           "123",
		GeohashesString: []string{"def", "xyz"}, // def дублируется
		GeohashesUint64: []int64{456, 789},      // 456 дублируется
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
		t.Errorf("expected 3 unique uint64, got %d", len(doc.GeohashesUint64))
	}
}
