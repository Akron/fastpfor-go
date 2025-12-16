package fastpfor

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackUint16Basic(t *testing.T) {
	assert := assert.New(t)

	values := []uint16{1, 2, 3, 4, 5, 100, 1000, 10000, 65535}
	buf := PackUint16(nil, values)

	// Verify the header has IntTypeUint16 marker
	header := bo.Uint32(buf[:headerBytes])
	intType := int((header >> headerTypeShift) & headerTypeMask)
	assert.Equal(IntTypeUint16, intType, "expected IntTypeUint16 marker")

	// Verify count
	count := int(header & headerCountMask)
	assert.Equal(len(values), count, "count mismatch")

	// Unpack using UnpackUint32 (should work since format is compatible)
	unpacked, err := UnpackUint32(nil, buf)
	assert.NoError(err)
	assert.Len(unpacked, len(values))

	for i, v := range values {
		assert.Equal(uint32(v), unpacked[i], "value[%d] mismatch", i)
	}
}

func TestPackDeltaUint16Basic(t *testing.T) {
	assert := assert.New(t)

	// Monotonic sequence - should not use zigzag
	values := []uint16{100, 200, 300, 400, 500, 600, 700, 800}
	original := make([]uint16, len(values))
	copy(original, values)

	buf := PackDeltaUint16(nil, values)

	// Verify original values are NOT mutated (unlike PackDeltaUint32)
	assert.Equal(original, values, "input should not be mutated")

	// Verify the header has IntTypeUint16 and delta flag
	header := bo.Uint32(buf[:headerBytes])
	intType := int((header >> headerTypeShift) & headerTypeMask)
	assert.Equal(IntTypeUint16, intType, "expected IntTypeUint16 marker")

	hasDelta := header&headerDeltaFlag != 0
	assert.True(hasDelta, "expected delta flag to be set")

	// Unpack and verify values
	unpacked, err := UnpackUint32(nil, buf)
	assert.NoError(err)

	for i, v := range values {
		assert.Equal(uint32(v), unpacked[i], "value[%d] mismatch", i)
	}
}

func TestPackDeltaUint16Zigzag(t *testing.T) {
	assert := assert.New(t)

	// Non-monotonic sequence - should use zigzag
	values := []uint16{500, 400, 600, 300, 700, 200}
	buf := PackDeltaUint16(nil, values)

	// Verify the header has zigzag flag
	header := bo.Uint32(buf[:headerBytes])
	hasZigZag := header&headerZigZagFlag != 0
	assert.True(hasZigZag, "expected zigzag flag for non-monotonic data")

	// Unpack and verify values
	unpacked, err := UnpackUint32(nil, buf)
	assert.NoError(err)

	for i, v := range values {
		assert.Equal(uint32(v), unpacked[i], "value[%d] mismatch", i)
	}
}

func TestPackUint16FullBlock(t *testing.T) {
	assert := assert.New(t)

	values := make([]uint16, blockSize)
	for i := range values {
		values[i] = uint16(rand.IntN(65536))
	}

	buf := PackUint16(nil, values)
	unpacked, err := UnpackUint32(nil, buf)
	assert.NoError(err)
	assert.Len(unpacked, len(values))

	for i, v := range values {
		assert.Equal(uint32(v), unpacked[i], "value[%d] mismatch", i)
	}
}

func TestPackUint16Empty(t *testing.T) {
	assert := assert.New(t)

	buf := PackUint16(nil, nil)

	header := bo.Uint32(buf[:headerBytes])
	count := int(header & headerCountMask)
	assert.Equal(0, count, "expected count 0")

	intType := int((header >> headerTypeShift) & headerTypeMask)
	assert.Equal(IntTypeUint16, intType, "expected IntTypeUint16 marker")
}

func TestPackUint16SmallValues(t *testing.T) {
	assert := assert.New(t)

	// All values fit in 7 bits - should use optimal bit width
	values := make([]uint16, blockSize)
	for i := range values {
		values[i] = uint16(i) // 0-127, max is 127 which needs 7 bits
	}

	buf := PackUint16(nil, values)

	// Verify bit width is 7 (max value is 127 = 0b1111111)
	header := bo.Uint32(buf[:headerBytes])
	bitWidth := int((header >> headerWidthShift) & headerWidthMask)
	assert.Equal(7, bitWidth, "expected bit width 7")
}

func TestPackDeltaUint16Empty(t *testing.T) {
	assert := assert.New(t)

	buf := PackDeltaUint16(nil, nil)

	header := bo.Uint32(buf[:headerBytes])
	count := int(header & headerCountMask)
	assert.Equal(0, count, "expected count 0")

	hasDelta := header&headerDeltaFlag != 0
	assert.True(hasDelta, "expected delta flag even for empty input")
}
