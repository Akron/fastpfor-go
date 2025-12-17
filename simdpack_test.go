//go:build amd64 && !noasm

package fastpfor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSIMDPackRoundTrip(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD disabled")
	}
	for bitWidth := 1; bitWidth <= 16; bitWidth += 5 {
		values := make([]uint32, 128)
		mask := uint32((1 << bitWidth) - 1)
		for i := range values {
			values[i] = uint32(i) & mask
		}
		payload := make([]byte, bitWidth*16)
		if !simdPack(payload, values, bitWidth) {
			assert.Failf(t, "PackUint32", "returned false at width %d", bitWidth)
		}
		got := make([]uint32, len(values))
		if !simdUnpack(got, payload, bitWidth, len(values)) {
			assert.Failf(t, "Unpack", "returned false at width %d", bitWidth)
		}
		for i, v := range values {
			assert.Equalf(t, v, got[i], "width %d index %d", bitWidth, i)
		}
	}
}

func TestSIMDPackRoundTripShort(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD disabled")
	}
	values := []uint32{1, 2, 3, 0, 1}
	const bitWidth = 3
	payload := make([]byte, bitWidth*16)
	if !simdPack(payload, values, bitWidth) {
		assert.Fail(t, "PackUint32 returned false")
	}
	got := make([]uint32, len(values))
	if !simdUnpack(got, payload, bitWidth, len(values)) {
		assert.Fail(t, "Unpack returned false")
	}
	for i, v := range values {
		assert.Equalf(t, v, got[i], "index %d", i)
	}
}

// TestDeltaDecodeWithOverflowSIMDAsm tests the SIMD assembly implementation
// of delta decode with overflow detection.
func TestDeltaDecodeWithOverflowSIMDAsm(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD disabled")
	}

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
			overflowPos := deltaDecodeWithOverflowSIMD(dst, tt.deltas, false)

			assert.Equalf(t, tt.wantOverPos, overflowPos, "overflow position")

			for i, want := range tt.wantValues {
				assert.Equalf(t, want, dst[i], "dst[%d]", i)
			}
		})
	}
}

// TestDeltaDecodeWithOverflowSIMDAsmFullBlock tests overflow at the last position (127).
func TestDeltaDecodeWithOverflowSIMDAsmFullBlock(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD disabled")
	}

	// Create a full block (128 elements) where overflow happens at index 127
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = 0x02000000
	}
	// At index 127: 127 * 0x02000000 = 0xFE000000
	// Adding another 0x02000000 = 0x100000000 = overflow!

	dst := make([]uint32, 128)
	overflowPos := deltaDecodeWithOverflowSIMD(dst, deltas, false)

	assert.Equal(t, uint8(127), overflowPos, "overflow position")
}

// TestDeltaDecodeWithOverflowSIMDAsmMatchesScalar verifies SIMD matches scalar implementation.
func TestDeltaDecodeWithOverflowSIMDAsmMatchesScalar(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD disabled")
	}

	testCases := [][]uint32{
		{1, 2, 3, 4, 5, 6, 7, 8},
		{0xFFFFFFFF, 1, 2, 3},
		{0x80000000, 0x40000000, 0x40000001},
		{0xFFFFFFF0, 5, 5, 10, 5},
		make([]uint32, 128),
	}

	// Initialize last test case
	for i := range testCases[4] {
		testCases[4][i] = uint32(i + 1)
	}

	for _, deltas := range testCases {
		dstSIMD := make([]uint32, len(deltas))
		dstScalar := make([]uint32, len(deltas))

		overflowSIMD := deltaDecodeWithOverflowSIMD(dstSIMD, deltas, false)
		overflowScalar := deltaDecodeWithOverflowScalar(dstScalar, deltas, false)

		assert.Equalf(t, overflowScalar, overflowSIMD, "deltas=%v overflow", deltas[:min(len(deltas), 5)])

		for i := range dstSIMD {
			assert.Equalf(t, dstScalar[i], dstSIMD[i], "deltas=%v dst[%d]", deltas[:min(len(deltas), 5)], i)
		}
	}
}

// BenchmarkDeltaDecodeWithOverflow_SIMD benchmarks the SIMD implementation.
// Note: This function is only called when overflow WILL occur (flag is set in header).
func BenchmarkDeltaDecodeWithOverflow_SIMD(b *testing.B) {
	if !IsSIMDavailable() {
		b.Skip("SIMD disabled")
	}

	// Create deltas that will overflow at index 64 (realistic case)
	// 64 * 0x02000000 = 0x80000000, 65th causes overflow
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = 0x02000000
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowSIMD(dst, deltas, false)
	}
}

// BenchmarkDeltaDecodeWithOverflow_Scalar benchmarks the scalar implementation for comparison.
// Note: This function is only called when overflow WILL occur (flag is set in header).
func BenchmarkDeltaDecodeWithOverflow_Scalar(b *testing.B) {
	// Create deltas that will overflow at index 64 (realistic case)
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = 0x02000000
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowScalar(dst, deltas, false)
	}
}

// BenchmarkDeltaDecodeWithOverflow_SIMD_EarlyOverflow benchmarks SIMD with overflow at index 1.
func BenchmarkDeltaDecodeWithOverflow_SIMD_EarlyOverflow(b *testing.B) {
	if !IsSIMDavailable() {
		b.Skip("SIMD disabled")
	}

	// Overflow at index 1 (earliest possible)
	deltas := make([]uint32, 128)
	deltas[0] = 0xFFFFFFFF
	deltas[1] = 1
	for i := 2; i < 128; i++ {
		deltas[i] = 1
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowSIMD(dst, deltas, false)
	}
}

// BenchmarkDeltaDecodeWithOverflow_Scalar_EarlyOverflow benchmarks scalar with overflow at index 1.
func BenchmarkDeltaDecodeWithOverflow_Scalar_EarlyOverflow(b *testing.B) {
	// Overflow at index 1 (earliest possible)
	deltas := make([]uint32, 128)
	deltas[0] = 0xFFFFFFFF
	deltas[1] = 1
	for i := 2; i < 128; i++ {
		deltas[i] = 1
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowScalar(dst, deltas, false)
	}
}

// BenchmarkDeltaDecodeWithOverflow_SIMD_LateOverflow benchmarks SIMD with overflow at index 127.
func BenchmarkDeltaDecodeWithOverflow_SIMD_LateOverflow(b *testing.B) {
	if !IsSIMDavailable() {
		b.Skip("SIMD disabled")
	}

	// Overflow at index 127 (latest possible in full block)
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = 0x02000000
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowSIMD(dst, deltas, false)
	}
}

// BenchmarkDeltaDecodeWithOverflow_Scalar_LateOverflow benchmarks scalar with overflow at index 127.
func BenchmarkDeltaDecodeWithOverflow_Scalar_LateOverflow(b *testing.B) {
	// Overflow at index 127 (latest possible in full block)
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = 0x02000000
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeWithOverflowScalar(dst, deltas, false)
	}
}

// ==================== Regular Delta Decode Benchmarks (no overflow) ====================

// BenchmarkDeltaDecode_SIMD benchmarks SIMD delta decode (no overflow detection).
func BenchmarkDeltaDecode_SIMD(b *testing.B) {
	if !IsSIMDavailable() {
		b.Skip("SIMD disabled")
	}

	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = uint32(i + 1)
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeSIMD(dst, deltas, false)
	}
}

// BenchmarkDeltaDecode_Scalar benchmarks scalar delta decode (no overflow detection).
func BenchmarkDeltaDecode_Scalar(b *testing.B) {
	deltas := make([]uint32, 128)
	for i := range deltas {
		deltas[i] = uint32(i + 1)
	}
	dst := make([]uint32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deltaDecodeScalar(dst, deltas, false)
	}
}
