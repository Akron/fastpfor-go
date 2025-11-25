//go:build amd64 && !purego

package fastpfor

import "testing"

func TestSIMDPackRoundTrip(t *testing.T) {
	if !Available() {
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
			t.Fatalf("Pack returned false at width %d", bitWidth)
		}
		got := make([]uint32, len(values))
		if !simdUnpack(got, payload, bitWidth, len(values)) {
			t.Fatalf("Unpack returned false at width %d", bitWidth)
		}
		for i, v := range values {
			if got[i] != v {
				t.Fatalf("width %d index %d: got %d want %d", bitWidth, i, got[i], v)
			}
		}
	}
}

func TestSIMDPackRoundTripShort(t *testing.T) {
	if !Available() {
		t.Skip("SIMD disabled")
	}
	values := []uint32{1, 2, 3, 0, 1}
	const bitWidth = 3
	payload := make([]byte, bitWidth*16)
	if !simdPack(payload, values, bitWidth) {
		t.Fatalf("Pack returned false")
	}
	got := make([]uint32, len(values))
	if !simdUnpack(got, payload, bitWidth, len(values)) {
		t.Fatalf("Unpack returned false")
	}
	for i, v := range values {
		if got[i] != v {
			t.Fatalf("index %d: got %d want %d", i, got[i], v)
		}
	}
}
