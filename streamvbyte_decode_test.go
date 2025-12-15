package fastpfor

import (
	"fmt"
	"testing"

	"github.com/mhr3/streamvbyte"
	"github.com/stretchr/testify/assert"
)

// svbCursor provides efficient sequential iteration through StreamVByte data.
// This is defined here for testing purposes - not used in production code.
type svbCursor struct {
	controlBytes []byte
	dataBytes    []byte
	count        int
	dataOffset   int
	blockIndex   int
	posInBlock   int
	currentCtrl  byte
	intraOffset  int
}

func svbNewCursor(svbData []byte, count int) svbCursor {
	numControlBytes := (count + 3) >> 2
	c := svbCursor{
		controlBytes: svbData[:numControlBytes],
		dataBytes:    svbData[numControlBytes:],
		count:        count,
	}
	if len(c.controlBytes) > 0 {
		c.currentCtrl = c.controlBytes[0]
	}
	return c
}

func (c *svbCursor) svbSeekTo(index int) {
	targetBlock := index >> 2
	targetPos := index & 0x03

	if targetBlock < c.blockIndex || (targetBlock == c.blockIndex && targetPos < c.posInBlock) {
		c.blockIndex = 0
		c.posInBlock = 0
		c.dataOffset = 0
		c.intraOffset = 0
		if len(c.controlBytes) > 0 {
			c.currentCtrl = c.controlBytes[0]
		}
	}

	for c.blockIndex < targetBlock {
		c.dataOffset += svbControlBlockSize(c.controlBytes[c.blockIndex])
		c.blockIndex++
		c.posInBlock = 0
		c.intraOffset = 0
	}

	if c.blockIndex < len(c.controlBytes) {
		c.currentCtrl = c.controlBytes[c.blockIndex]
	}

	for c.posInBlock < targetPos {
		code := (c.currentCtrl >> (c.posInBlock * 2)) & 0x03
		c.intraOffset += int(code) + 1
		c.posInBlock++
	}
}

func (c *svbCursor) svbReadCurrent() uint32 {
	code := (c.currentCtrl >> (c.posInBlock * 2)) & 0x03
	byteLen := int(code) + 1
	return svbReadValue(c.dataBytes[c.dataOffset+c.intraOffset:], byteLen)
}

func (c *svbCursor) svbAdvance() {
	code := (c.currentCtrl >> (c.posInBlock * 2)) & 0x03
	c.intraOffset += int(code) + 1
	c.posInBlock++

	if c.posInBlock >= 4 {
		c.dataOffset += c.intraOffset
		c.blockIndex++
		c.posInBlock = 0
		c.intraOffset = 0
		if c.blockIndex < len(c.controlBytes) {
			c.currentCtrl = c.controlBytes[c.blockIndex]
		}
	}
}

func (c *svbCursor) svbCurrentIndex() int {
	return c.blockIndex*4 + c.posInBlock
}

// TestSvbControlBlockSize tests the control block size calculation.
func TestSvbControlBlockSize(t *testing.T) {
	testCases := []struct {
		ctrl     byte
		expected int
	}{
		{0x00, 4},  // codes: 0,0,0,0 → 1+1+1+1 = 4
		{0xFF, 16}, // codes: 3,3,3,3 → 4+4+4+4 = 16
		{0x55, 8},  // codes: 1,1,1,1 → 2+2+2+2 = 8
		{0xAA, 12}, // codes: 2,2,2,2 → 3+3+3+3 = 12
		{0x01, 5},  // codes: 1,0,0,0 → 2+1+1+1 = 5
		{0x04, 5},  // codes: 0,1,0,0 → 1+2+1+1 = 5
		{0x10, 5},  // codes: 0,0,1,0 → 1+1+2+1 = 5
		{0x40, 5},  // codes: 0,0,0,1 → 1+1+1+2 = 5
	}

	for _, tc := range testCases {
		got := svbControlBlockSize(tc.ctrl)
		assert.Equal(t, tc.expected, got, "svbControlBlockSize(0x%02X)", tc.ctrl)
	}
}

// TestSvbDecodeOne tests single-value decoding against the reference implementation.
func TestSvbDecodeOne(t *testing.T) {
	testCases := []struct {
		name   string
		values []uint32
	}{
		{"small_values", []uint32{1, 2, 3, 4, 5, 6, 7, 8}},
		{"mixed_sizes", []uint32{1, 256, 65536, 16777216, 2, 512, 131072}},
		{"all_1byte", []uint32{0, 127, 255, 1, 100, 200, 50, 150}},
		{"all_4byte", []uint32{1 << 24, 1 << 25, 1 << 26, 1 << 27}},
		{"single_value", []uint32{42}},
		{"large_count", func() []uint32 {
			v := make([]uint32, 64)
			for i := range v {
				v[i] = uint32(i * 1000)
			}
			return v
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode using reference implementation
			encoded := streamvbyte.EncodeUint32(tc.values, nil)

			// Decode each value individually and compare
			for i, want := range tc.values {
				got := svbDecodeOne(encoded, len(tc.values), i)
				assert.Equal(t, want, got, "svbDecodeOne(data, %d, %d)", len(tc.values), i)
			}
		})
	}
}

// TestSvbReadValue tests individual value reading at different byte lengths.
func TestSvbReadValue(t *testing.T) {
	testCases := []struct {
		data     []byte
		byteLen  int
		expected uint32
	}{
		{[]byte{0x42}, 1, 0x42},
		{[]byte{0x34, 0x12}, 2, 0x1234},
		{[]byte{0x56, 0x34, 0x12}, 3, 0x123456},
		{[]byte{0x78, 0x56, 0x34, 0x12}, 4, 0x12345678},
	}

	for _, tc := range testCases {
		got := svbReadValue(tc.data, tc.byteLen)
		assert.Equal(t, tc.expected, got, "svbReadValue(%v, %d)", tc.data, tc.byteLen)
	}
}

// TestSvbCursor tests the cursor-based sequential decoding.
func TestSvbCursor(t *testing.T) {
	values := []uint32{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200}
	encoded := streamvbyte.EncodeUint32(values, nil)

	cursor := svbNewCursor(encoded, len(values))

	// Read values sequentially
	for i, want := range values {
		cursor.svbSeekTo(i)
		got := cursor.svbReadCurrent()
		assert.Equal(t, want, got, "cursor at %d", i)
	}
}

// TestSvbCursorSeekBackwards tests cursor seeking backwards.
func TestSvbCursorSeekBackwards(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30, 40, 50, 60, 70, 80}
	encoded := streamvbyte.EncodeUint32(values, nil)

	cursor := svbNewCursor(encoded, len(values))

	// Seek to end
	cursor.svbSeekTo(7)
	assert.Equal(uint32(80), cursor.svbReadCurrent(), "at pos 7")

	// Seek back to beginning
	cursor.svbSeekTo(0)
	assert.Equal(uint32(10), cursor.svbReadCurrent(), "at pos 0")

	// Seek to middle
	cursor.svbSeekTo(4)
	assert.Equal(uint32(50), cursor.svbReadCurrent(), "at pos 4")
}

// TestSvbCursorAdvance tests cursor advance functionality.
func TestSvbCursorAdvance(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	encoded := streamvbyte.EncodeUint32(values, nil)

	cursor := svbNewCursor(encoded, len(values))

	for i, want := range values {
		assert.Equal(i, cursor.svbCurrentIndex(), "currentIndex")
		assert.Equal(want, cursor.svbReadCurrent(), "at index %d", i)
		if i < len(values)-1 {
			cursor.svbAdvance()
		}
	}
}

// TestSvbCursorMixedSizes tests cursor with values of different byte sizes.
func TestSvbCursorMixedSizes(t *testing.T) {
	// Values that require different byte sizes:
	// 1-byte: 0-255
	// 2-byte: 256-65535
	// 3-byte: 65536-16777215
	// 4-byte: 16777216+
	values := []uint32{
		1,        // 1 byte
		256,      // 2 bytes
		65536,    // 3 bytes
		16777216, // 4 bytes
		2,        // 1 byte
		512,      // 2 bytes
		100000,   // 3 bytes
		50000000, // 4 bytes
	}
	encoded := streamvbyte.EncodeUint32(values, nil)

	cursor := svbNewCursor(encoded, len(values))

	// Test sequential read
	for i, want := range values {
		cursor.svbSeekTo(i)
		got := cursor.svbReadCurrent()
		assert.Equal(t, want, got, "at index %d", i)
	}

	// Test random access
	testOrder := []int{7, 0, 4, 2, 6, 1, 5, 3}
	for _, idx := range testOrder {
		cursor.svbSeekTo(idx)
		got := cursor.svbReadCurrent()
		assert.Equal(t, values[idx], got, "random access at %d", idx)
	}
}

// TestSvbDecodeOneVsReference compares our implementation with the reference.
func TestSvbDecodeOneVsReference(t *testing.T) {
	// Test with various counts that cross block boundaries
	counts := []int{1, 2, 3, 4, 5, 7, 8, 9, 12, 15, 16, 17, 31, 32, 33, 64, 100}

	for _, count := range counts {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			values := make([]uint32, count)
			for i := range values {
				values[i] = uint32(i*123 + 456)
			}

			encoded := streamvbyte.EncodeUint32(values, nil)
			reference := streamvbyte.DecodeUint32(encoded, count, nil)

			for i := 0; i < count; i++ {
				got := svbDecodeOne(encoded, count, i)
				assert.Equal(t, reference[i], got, "at index %d", i)
			}
		})
	}
}

// BenchmarkSvbDecodeOne benchmarks single-value decoding.
func BenchmarkSvbDecodeOne(b *testing.B) {
	values := make([]uint32, 64)
	for i := range values {
		values[i] = uint32(i * 1000)
	}
	encoded := streamvbyte.EncodeUint32(values, nil)
	count := len(values)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = svbDecodeOne(encoded, count, i%count)
	}
}

// BenchmarkSvbDecodeOneVsFullDecode compares single-value vs full decode.
func BenchmarkSvbDecodeOneVsFullDecode(b *testing.B) {
	values := make([]uint32, 64)
	for i := range values {
		values[i] = uint32(i * 1000)
	}
	encoded := streamvbyte.EncodeUint32(values, nil)
	count := len(values)

	b.Run("SingleValue", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = svbDecodeOne(encoded, count, 32) // Middle value
		}
	})

	b.Run("FullDecode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = streamvbyte.DecodeUint32(encoded, count, nil)
		}
	})
}

// BenchmarkSvbCursor benchmarks cursor-based sequential access.
func BenchmarkSvbCursor(b *testing.B) {
	values := make([]uint32, 64)
	for i := range values {
		values[i] = uint32(i * 1000)
	}
	encoded := streamvbyte.EncodeUint32(values, nil)
	count := len(values)

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		cursor := svbNewCursor(encoded, count)
		for i := 0; i < b.N; i++ {
			idx := i % count
			if idx == 0 {
				cursor = svbNewCursor(encoded, count)
			}
			_ = cursor.svbReadCurrent()
			cursor.svbAdvance()
		}
	})

	b.Run("RandomSeek", func(b *testing.B) {
		b.ReportAllocs()
		cursor := svbNewCursor(encoded, count)
		for i := 0; i < b.N; i++ {
			cursor.svbSeekTo((i * 7) % count)
			_ = cursor.svbReadCurrent()
		}
	})
}
