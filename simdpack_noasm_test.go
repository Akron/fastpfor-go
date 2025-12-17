//go:build !amd64 || noasm

package fastpfor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDeltaDecodeWithOverflowScalar tests the scalar implementation
// of delta decode with overflow detection.
func TestDeltaDecodeWithOverflowScalar(t *testing.T) {
	tests := []struct {
		name        string
		deltas      []uint32
		wantValues  []uint32
		wantOverPos uint8
	}{
		{
			name:        "no overflow - small deltas",
			deltas:      []uint32{1, 2, 3, 4, 5},
			wantValues:  []uint32{1, 3, 6, 10, 15},
			wantOverPos: 0,
		},
		{
			name:        "overflow at index 1",
			deltas:      []uint32{0xFFFFFFFF, 1},
			wantValues:  []uint32{0xFFFFFFFF, 0}, // wraps around
			wantOverPos: 1,
		},
		{
			name:        "overflow at index 3",
			deltas:      []uint32{0xFFFFFFF0, 5, 5, 10, 5},
			wantValues:  []uint32{0xFFFFFFF0, 0xFFFFFFF5, 0xFFFFFFFA, 4, 9},
			wantOverPos: 3,
		},
		{
			name:        "no overflow - large values",
			deltas:      []uint32{0x80000000, 0x7FFFFFFF},
			wantValues:  []uint32{0x80000000, 0xFFFFFFFF},
			wantOverPos: 0,
		},
		{
			name:        "full block no overflow",
			deltas:      make([]uint32, 128),
			wantValues:  make([]uint32, 128),
			wantOverPos: 0,
		},
	}

	// Initialize full block test data
	for i := range tests[4].deltas {
		tests[4].deltas[i] = 1
		if i == 0 {
			tests[4].wantValues[i] = 1
		} else {
			tests[4].wantValues[i] = tests[4].wantValues[i-1] + 1
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := make([]uint32, len(tt.deltas))
			overflowPos := deltaDecodeWithOverflowScalar(dst, tt.deltas, false)

			if overflowPos != tt.wantOverPos {
				t.Errorf("overflow position = %d, want %d", overflowPos, tt.wantOverPos)
			}

			for i, want := range tt.wantValues {
				if dst[i] != want {
					t.Errorf("dst[%d] = 0x%X, want 0x%X", i, dst[i], want)
				}
			}
		})
	}
}

// TestDeltaDecodeWithOverflowScalarFullBlock tests overflow at the last position (127).
func TestDeltaDecodeWithOverflowScalarFullBlock(t *testing.T) {
	// Create a full block (128 elements) where overflow happens at index 127
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = 0x02000000
	}
	// At index 127: 127 * 0x02000000 = 0xFE000000
	// Adding another 0x02000000 = 0x100000000 = overflow!

	dst := make([]uint32, 128)
	overflowPos := deltaDecodeWithOverflowScalar(dst, deltas, false)

	assert.Equal(t, uint8(127), overflowPos, "overflow position")
}

// BenchmarkDeltaDecodeWithOverflowScalarNoasm benchmarks the scalar implementation.
func BenchmarkDeltaDecodeWithOverflowScalarNoasm(b *testing.B) {
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = uint32(i + 1)
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowScalar(dst, deltas, false)
	}
}
