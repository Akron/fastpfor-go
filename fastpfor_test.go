package fastpfor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/mhr3/streamvbyte"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// Non-delta round-trip tests
// -----------------------------------------------------------------------------

// TestMaxBlockSizeUint32 verifies the exported MaxBlockSizeUint32 constant matches internal logic.
func TestMaxBlockSizeUint32(t *testing.T) {
	// MaxBlockSizeUint32 = headerBytes (4) + blockSize (128) * 4 bytes/int = 516
	assert.Equal(t, 516, MaxBlockSizeUint32())
}

// TestBlockLength verifies BlockLength() matches the encoded size.
func TestBlockLength(t *testing.T) {
	assert := assert.New(t)

	t.Run("noExceptions", func(t *testing.T) {
		buf := PackUint32(nil, genSequential(blockSize))
		got, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(len(buf), got)
	})

	t.Run("withExceptions", func(t *testing.T) {
		buf := PackUint32(nil, genDataWithSmallExceptions())
		got, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(len(buf), got)
	})

	t.Run("truncatedHeader", func(t *testing.T) {
		_, err := BlockLength([]byte{0x00, 0x01})
		assert.Error(err)
	})

	t.Run("truncatedExceptionsMeta", func(t *testing.T) {
		buf := PackUint32(nil, genDataWithSmallExceptions())
		header := bo.Uint32(buf[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		if !hasExceptions {
			t.Skip("test data unexpectedly produced no exceptions")
		}
		minLen := headerBytes + payloadBytes(bitWidth)
		_, err := BlockLength(buf[:minLen+1])
		assert.Error(err)
	})
}

// TestIntTypeConstants verifies the integer type constants have expected values.
func TestIntTypeConstants(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(1, IntTypeUint16, "IntTypeUint16 should be 1")
	assert.Equal(2, IntTypeUint32, "IntTypeUint32 should be 2")
}

// TestEncodeDecodeHeader verifies header encode/decode round-trip with all fields.
func TestEncodeDecodeHeader(t *testing.T) {
	assert := assert.New(t)

	// Create a header with specific values
	count := 50
	bitWidth := 12
	flags := headerTypeUint16Flag | headerDeltaFlag | headerZigZagFlag

	header := encodeHeader(count, bitWidth, flags)

	// Decode and verify
	gotCount, gotBitWidth, gotIntType, hasExc, hasDelta, hasZigZag, willOverflow := decodeHeader(header)

	assert.Equal(count, gotCount, "count mismatch")
	assert.Equal(bitWidth, gotBitWidth, "bitWidth mismatch")
	assert.Equal(IntTypeUint16, gotIntType, "intType mismatch")
	assert.False(hasExc, "expected no exception flag")
	assert.True(hasDelta, "expected delta flag")
	assert.True(hasZigZag, "expected zigzag flag")
	assert.False(willOverflow, "expected no will-overflow flag")
}

// TestPackUint32HasCorrectIntType verifies PackUint32 uses IntTypeUint32 in header.
func TestPackUint32HasCorrectIntType(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{1, 2, 3, 4, 5}
	buf := PackUint32(nil, values)

	header := bo.Uint32(buf[:headerBytes])
	intType := int((header >> headerTypeShift) & headerTypeMask)
	assert.Equal(IntTypeUint32, intType, "PackUint32 should use IntTypeUint32")
}

// TestPackLengthValidation ensures PackUint32 accepts inputs that exceed blockSize (performance optimization).
func TestPackLengthValidation(t *testing.T) {
	assert := assert.New(t)
	// No longer panics for performance - caller is responsible for valid input
	buf := PackUint32(nil, make([]uint32, blockSize+1))
	assert.NotNil(buf, "should still produce output even with oversized input")
}

// TestPackUnpackEmpty verifies we can round-trip an empty slice.
func TestPackUnpackEmpty(t *testing.T) {
	assertRoundTrip(t, nil)
}

// TestPackUnpackSingleValue covers the simplest non-empty payload.
func TestPackUnpackSingleValue(t *testing.T) {
	assertRoundTrip(t, []uint32{123456})
}

// TestPackUnpackShortBlock shows we do not require a full block of data.
func TestPackUnpackShortBlock(t *testing.T) {
	assertRoundTrip(t, []uint32{0, 1, 1, 2, 3, 5, 8, 13, 21})
	assertRoundTrip(t, []uint32{33, 22, 12, 14, 0, 3, 1, 8, 9})
}

// TestPackUnpackFullBlock validates the happy path for a sequential block.
func TestPackUnpackFullBlock(t *testing.T) {
	assertRoundTrip(t, genSequential(blockSize))
}

// TestPackUnpackBitWidth32 checks that maximum-width values survive a round trip.
func TestPackUnpackBitWidth32(t *testing.T) {
	max := ^uint32(0)

	// The zero can be represented in 0 bits, that's why its index isn't in the exception list
	buf := assertRoundTrip(t, []uint32{max, 0, max - 1, 1234567890, 42, max})
	assert.Equal(t, 5, getExceptionCount(buf))
	assert.Equal(t, 0, getBitWidth(buf))
}

// TestPackUnpackRandomData inspects header stats for unstructured inputs.
func TestPackUnpackRandomData(t *testing.T) {
	src := make([]uint32, blockSize)
	rng := rand.New(rand.NewSource(42))
	for i := range src {
		src[i] = rng.Uint32()
	}
	buf := assertRoundTrip(t, src)

	// Slightly larger than uncompressed:
	assert.Equal(t, 516, len(buf))
	assert.Equal(t, 32, getBitWidth(buf))
	assert.Equal(t, 0, getExceptionCount(buf))
}

// TestPackFullBlockSequentialCompression confirms predictable sizing for sequential values.
func TestPackFullBlockSequentialCompression(t *testing.T) {
	assert := assert.New(t)
	src := genSequential(blockSize)
	buf := assertRoundTrip(t, src)
	assert.Equal(116, len(buf))
	assert.Equal(7, getBitWidth(buf))
	assert.Equal(0, getExceptionCount(buf))
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

// TestPackFullBlockRandom16BitCompression ensures 16-bit noise still compresses below raw size.
func TestPackFullBlockRandom16BitCompression(t *testing.T) {
	src := make([]uint32, blockSize)
	rng := rand.New(rand.NewSource(2025))
	for i := range src {
		src[i] = uint32(rng.Intn(1 << 16))
	}
	buf := assertRoundTrip(t, src)
	assert.Equal(t, 0, getExceptionCount(buf))
	assert.Equal(t, 260, len(buf))
	assert.Equal(t, 16, getBitWidth(buf))
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

// TestPackBitWidthCoverage walks widths 2..32 and confirms deterministic payloads.
func TestPackBitWidthCoverage(t *testing.T) {
	buf := make([]byte, 0, headerBytes+payloadBytes(32))
	dst := make([]uint32, 2*blockSize) // cap >= 256 for zero-allocation

	// Note: the width 0 is not supported, so we start at 2
	for width := 2; width <= 32; width++ {
		width := width
		t.Run(fmt.Sprintf("width_%02d", width), func(t *testing.T) {
			assert := assert.New(t)
			src := genValuesForBitWidth(width)

			encoded := PackUint32(buf[:0], src)
			if len(encoded) > 0 {
				assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array")
			}

			header := binary.LittleEndian.Uint32(encoded[:headerBytes])
			_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)

			assert.False(hasExceptions, "unexpected exceptions for width %d", width)
			assert.Equal(width, bitWidth, "header stored wrong bit width")
			assert.Equal(headerBytes+payloadBytes(width), len(encoded), "payload size mismatch")

			decoded, err := UnpackUint32(dst[:0], encoded)
			assert.NoError(err)
			if len(decoded) > 0 {
				assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array")
			}
			assert.Equal(src, decoded, "round trip mismatch")
		})
	}
}

// TestPackBitWidthExamples documents concrete width scenarios without exceptions.
func TestPackBitWidthExamples(t *testing.T) {
	assert := assert.New(t)
	buf := make([]byte, 0, headerBytes+payloadBytes(32))
	dst := make([]uint32, 2*blockSize) // cap >= 256 for zero-allocation

	// Width 2 (no exceptions): simple repeating pattern that fits in two bits.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32(i % 4)
		}
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 2")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(2, bitWidth, "width 2 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 2")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 2")
		assert.Equal(src, decoded, "width 2 round trip mismatch")
	}

	// Width 5 (no exceptions): Fibonacci-like sequence staying within five bits.
	{
		src := make([]uint32, blockSize)
		fibLike := []uint32{0, 1, 1, 2, 3, 5, 8, 13, 21, 31}
		for i := range src {
			src[i] = fibLike[i%len(fibLike)]
		}
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 5")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(5, bitWidth, "width 5 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 5")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 5")
		assert.Equal(src, decoded, "width 5 round trip mismatch")
	}

	// Width 13 (no exceptions): quadratic sequence capped at 13 bits.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32((i * i * 17) & ((1 << 13) - 1))
		}
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 13")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(13, bitWidth, "width 13 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 13")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 13")
		assert.Equal(src, decoded, "width 13 round trip mismatch")
	}

	// Width 24 (no exceptions): multiplicative pattern covering 24 bits.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32((i * 123456) & ((1 << 24) - 1))
		}
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 24")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(24, bitWidth, "width 24 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 24")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 24")
		assert.Equal(src, decoded, "width 24 round trip mismatch")
	}

	// Width 32 (no exceptions): alternating max values forcing 32-bit packing.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			if i%2 == 0 {
				src[i] = mathMaxUint32
			} else {
				src[i] = mathMaxUint32 - 1
			}
		}
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 32")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(32, bitWidth, "width 32 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 32")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 32")
		assert.Equal(src, decoded, "width 32 round trip mismatch")
	}

}

// -----------------------------------------------------------------------------
// Delta tests
// -----------------------------------------------------------------------------

// TestPackUnpackDeltaEmpty verifies delta packing a nil slice is safe.
func TestPackUnpackDeltaEmpty(t *testing.T) {
	buf := assertDeltaRoundTrip(t, nil)
	assert.Equal(t, 0, getExceptionCount(buf))
	assert.Equal(t, 0, getBitWidth(buf))
}

// TestPackUnpackDeltaMonotonic ensures monotonic deltas survive a delta round trip.
func TestPackUnpackDeltaMonotonic(t *testing.T) {
	buf := assertDeltaRoundTrip(t, genMonotonic(blockSize))
	assertCompressionBelowRaw(t, buf, blockSize*4)
	assert.Equal(t, 0, getExceptionCount(buf))
	assert.Equal(t, 3, getBitWidth(buf))
}

// TestPackUnpackDeltaMixed covers noisy data that still round-trips.
func TestPackUnpackDeltaMixed(t *testing.T) {
	buf := assertDeltaRoundTrip(t, genMixed(blockSize))
	assertCompressionBelowRaw(t, buf, blockSize*4)
	assert.Equal(t, 1, getExceptionCount(buf))
	assert.Equal(t, 13, getBitWidth(buf))
}

// TestPackDeltaHandlesMixedLargeDiffs ensures big positive/negative deltas decode.
func TestPackDeltaHandlesMixedLargeDiffs(t *testing.T) {
	values := []uint32{0x30303030, 0x00303030, 0x81303030}
	buf := assertDeltaRoundTrip(t, values)
	assert.Equal(t, 3, getExceptionCount(buf))
	assert.Equal(t, 0, getBitWidth(buf))
	assertValidEncoding(t, buf)
}

// TestUnpackUint32AutoDeltaDecode verifies that UnpackUint32 auto-detects delta flag
// and decodes delta-encoded data without needing to call UnpackDeltaUint32.
func TestUnpackUint32AutoDeltaDecode(t *testing.T) {
	assert := assert.New(t)

	// Test with monotonic data (no zigzag)
	monotonic := genMonotonic(blockSize)
	src := slices.Clone(monotonic)
	buf := PackDeltaUint32(nil, src)

	// Verify delta flag is set
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, _, hasDelta, _, _ := decodeHeader(header)
	assert.True(hasDelta, "delta flag should be set")

	// UnpackUint32 should auto-detect and decode delta
	result, err := UnpackUint32(nil, buf)
	assert.NoError(err)
	assert.Equal(monotonic, result, "UnpackUint32 should auto-decode delta-encoded data")

	// Test with non-monotonic data (with zigzag)
	nonMonotonic := []uint32{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901}
	src2 := slices.Clone(nonMonotonic)
	buf2 := PackDeltaUint32(nil, src2)

	// Verify both flags are set
	header2 := binary.LittleEndian.Uint32(buf2[:headerBytes])
	_, _, _, _, hasDelta2, hasZigZag2, _ := decodeHeader(header2)
	assert.True(hasDelta2, "delta flag should be set")
	assert.True(hasZigZag2, "zigzag flag should be set for non-monotonic data")

	// UnpackUint32 should auto-detect and decode delta+zigzag
	result2, err := UnpackUint32(nil, buf2)
	assert.NoError(err)
	assert.Equal(nonMonotonic, result2, "UnpackUint32 should auto-decode delta+zigzag data")
}

// TestPackDeltaMonotonicDoesNotSetZigZag ensures monotonic deltas skip zigzag flag.
func TestPackDeltaMonotonicDoesNotSetZigZag(t *testing.T) {
	assert := assert.New(t)
	src := slices.Clone(genMonotonic(32))
	buf := PackDeltaUint32(nil, src)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, _, hasDelta, hasZigZag, _ := decodeHeader(header)
	assert.True(hasDelta, "delta flag should be set")
	assert.False(hasZigZag, "monotonic data should not require zigzag encoding")
	assert.Equal(0, getExceptionCount(buf))
	assert.Equal(3, getBitWidth(buf))
}

// -----------------------------------------------------------------------------
// Delta with ZigZag
// -----------------------------------------------------------------------------

// TestPackUnpackDeltaZigZagHeader checks that negative deltas toggle zigzag encoding.
func TestPackUnpackDeltaZigZagHeader(t *testing.T) {
	assert := assert.New(t)
	original := []uint32{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901}
	src := slices.Clone(original)
	buf := PackDeltaUint32(nil, src)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, _, hasDelta, hasZigZag, _ := decodeHeader(header)
	assert.True(hasDelta, "expected delta flag to be set")
	assert.True(hasZigZag, "expected zigzag flag for negative deltas")

	got, err := UnpackUint32(nil, buf)
	assert.NoError(err)
	assert.Equal(original, got, "zigzag delta round-trip mismatch")
	// Even though this block only stores 9 logical values, the lane layout would still
	// serialize a full 4×32 payload if bitWidth > 0. It's therefore cheaper to set the
	// width to zero and spill every non-zero value into the exception table.
	assert.Equal(9, getExceptionCount(buf))
	assert.Equal(0, getBitWidth(buf))
}

// TestPackUnpackDeltaZigZagWithExceptions verifies zigzagged data can still patch outliers.
func TestPackUnpackDeltaZigZagWithExceptions(t *testing.T) {
	assert := assert.New(t)
	original := make([]uint32, 64)
	value := uint32(1 << 20)
	for i := range original {
		switch i {
		case 0:
			original[i] = value
		case 20:
			value -= 5000
			original[i] = value
		case 40:
			value += 1 << 24
			original[i] = value
		default:
			value++
			original[i] = value
		}
	}

	src := slices.Clone(original)
	buf := PackDeltaUint32(nil, src)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, hasExceptions, hasDelta, hasZigZag, _ := decodeHeader(header)
	assert.True(hasDelta, "expected delta flag to be set")
	assert.True(hasZigZag, "expected zigzag flag when negative delta present")
	assert.True(hasExceptions, "expected exceptions due to large zigzagged delta")
	assert.Equal(3, getExceptionCount(buf))
	assert.Equal(2, getBitWidth(buf))
	got, err := UnpackUint32(nil, buf)
	assert.NoError(err)
	assert.Equal(original, got, "zigzag delta with exceptions round-trip mismatch")
}

// -----------------------------------------------------------------------------
// Exception-focused tests
// -----------------------------------------------------------------------------

// TestPackUnpackWithExceptions spikes a few values and observes patched output.
func TestPackUnpackWithExceptions(t *testing.T) {
	src := make([]uint32, blockSize)
	for i := range src {
		src[i] = uint32(i % 7)
	}
	src[5] = 1 << 30
	src[9] = 1<<29 + 123
	buf := assertRoundTrip(t, src)
	// With StreamVByte exceptions, the exact size depends on encoded high bits
	assert.Equal(t, 2, getExceptionCount(buf))
	assert.LessOrEqual(t, getBitWidth(buf), 3, "bit width should be at most 3")
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

// TestPackBitWidthExceptionExamples shows how spikes trigger exception metadata.
func TestPackBitWidthExceptionExamples(t *testing.T) {
	assert := assert.New(t)
	buf := make([]byte, 0, headerBytes+payloadBytes(32))
	dst := make([]uint32, 2*blockSize) // cap >= 256 for zero-allocation

	// Width 5 (with exceptions): low values plus a few spikes that trigger patches.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32(16 + (i % 16))
		}
		src[10] = 1<<18 | 7
		src[77] = 1<<20 | 5
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 5 exceptions")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(5, bitWidth, "width 5 exception header mismatch")
		assert.True(hasExceptions, "expected exceptions for width 5 case")
		assert.Equal(2, getExceptionCount(encoded), "width 5 exception count mismatch")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 5 exceptions")
		assert.Equal(src, decoded, "width 5 exceptions round trip mismatch")
	}

	// Width 13 (with exceptions): mostly 13-bit values with high outliers.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32(4096 + (i % 4096))
		}
		src[5] = 1<<20 | 3
		src[48] = 1<<22 | 11
		src[97] = 1<<21 | 17
		encoded := PackUint32(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected PackUint32 to reuse dst backing array for width 13 exceptions")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
		assert.Equal(13, bitWidth, "width 13 exception header mismatch")
		assert.True(hasExceptions, "expected exceptions for width 13 case")
		assert.Equal(3, getExceptionCount(encoded), "width 13 exception count mismatch")
		decoded, err := UnpackUint32(dst[:0], encoded)
		assert.NoError(err)
		assert.Equal(&dst[0], &decoded[0], "expected UnpackUint32 to reuse dst backing array for width 13 exceptions")
		assert.Equal(src, decoded, "width 13 exceptions round trip mismatch")
	}
}

// TestPackWritesExceptionMetadata ensures patched blocks write the extra payload.
func TestPackWritesExceptionMetadata(t *testing.T) {
	assert := assert.New(t)
	data := make([]uint32, blockSize)
	for i := range data {
		data[i] = uint32(i & 15)
	}
	data[0] = 1 << 28
	data[63] = 1<<29 + 7

	buf := PackUint32(nil, data)
	assert.Equal(2, getExceptionCount(buf))
	assert.Equal(4, getBitWidth(buf))
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	assert.True(header&headerExceptionFlag != 0, "expected exception flag set")
	width := int((header >> headerWidthShift) & headerWidthMask)
	payload := payloadBytes(width)
	// With StreamVByte, the patch size is variable. Verify the buffer is valid and well-formed.
	excCount := int(buf[headerBytes+payload])
	assert.Equal(2, excCount, "expected 2 exceptions")
	// Verify we can decode the buffer correctly
	decoded, err := UnpackUint32(nil, buf)
	assert.NoError(err)
	assert.Equal(data, decoded, "round trip mismatch with exceptions")
}

// -----------------------------------------------------------------------------
// Edge cases
// -----------------------------------------------------------------------------

// TestPackAppendsInPlace ensures PackUint32 can reuse the caller's capacity and that the
// caller can still decode the appended block by slicing off the already-written prefix.
func TestPackAppendsInPlace(t *testing.T) {
	assert := assert.New(t)
	prefix := make([]byte, 8, 128)
	for i := range prefix {
		prefix[i] = byte(i)
	}
	values := []uint32{11, 22}
	buf := PackUint32(prefix, values)
	assert.Equal(&prefix[0], &buf[0], "expected PackUint32 to reuse dst capacity")
	assert.Equal(prefix, buf[:len(prefix)], "prefix corrupted")
	decoded, err := UnpackUint32(nil, buf[len(prefix):])
	assert.NoError(err)
	assert.Equal(values, decoded, "round trip mismatch for appended block")
	// Verify the block is well-formed (size depends on optimal width + exception encoding)
	header := binary.LittleEndian.Uint32(buf[len(prefix) : len(prefix)+headerBytes])
	count, width, _, hasExc, _, _, _ := decodeHeader(header)
	assert.Equal(len(values), count, "header count mismatch")
	assert.LessOrEqual(width, 32, "bit width should be at most 32")
	// The actual size depends on whether exceptions are used and StreamVByte encoding
	minSize := len(prefix) + headerBytes + payloadBytes(width)
	assert.GreaterOrEqual(len(buf), minSize, "buffer should at least have header + payload")
	_ = hasExc // exception presence depends on optimal width selection
}

// TestUnpackReusesDst ensures UnpackUint32 writes back into the provided buffer.
func TestUnpackReusesDst(t *testing.T) {
	assert := assert.New(t)
	input := []uint32{5, 6, 7, 8}
	buf := PackUint32(nil, input)
	dst := make([]uint32, 2*blockSize) // cap >= 256 for zero-allocation
	out, err := UnpackUint32(dst[:0], buf)
	assert.NoError(err)
	assert.Equal(len(input), len(out), "length mismatch")
	if len(out) > 0 {
		assert.Equal(&dst[0], &out[0], "expected UnpackUint32 to reuse dst backing array")
	}
	assert.Equal(input, out)
}

// TestUnpackRejectsShortBuffer guards against truncated buffers.
func TestUnpackRejectsShortBuffer(t *testing.T) {
	header := encodeHeader(4, 5, 0)
	buf := make([]byte, headerBytes)
	binary.LittleEndian.PutUint32(buf, header)
	_, err := UnpackUint32(nil, buf)
	assert.ErrorIs(t, err, ErrInvalidBuffer)
}

// -----------------------------------------------------------------------------
// Internal helper tests
// -----------------------------------------------------------------------------

// TestRequiredBitWidthScalar exercises the internal width calculator directly so
// we keep coverage even when SIMD overrides the function pointer at runtime.
func TestRequiredBitWidthScalar(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(0, requiredBitWidthScalar(nil), "nil slice should require zero width")

	zeros := make([]uint32, 10)
	assert.Equal(0, requiredBitWidthScalar(zeros), "all-zero slice should require zero width")

	small := []uint32{0, 1, 2, 3, 7}
	assert.Equal(3, requiredBitWidthScalar(small), "expected three bits for %v", small)

	mixed := []uint32{15, 16, 31, 1024}
	assert.Equal(11, requiredBitWidthScalar(mixed), "expected eleven bits for %v", mixed)

	max := []uint32{mathMaxUint32}
	assert.Equal(32, requiredBitWidthScalar(max), "max uint32 forces 32 bits")
}

// TestValidateBlockLengthDirect ensures the guard returns errors for invalid lengths
// without going through PackUint32/UnpackUint32.
func TestValidateBlockLengthDirect(t *testing.T) {
	assert := assert.New(t)
	assert.NoError(validateBlockLength(0))
	assert.NoError(validateBlockLength(blockSize))

	err := validateBlockLength(-1)
	assert.ErrorIs(err, ErrInvalidBlockLength)
	assert.Contains(err.Error(), "cannot be negative")

	err = validateBlockLength(blockSize + 1)
	assert.ErrorIs(err, ErrInvalidBlockLength)
	assert.Contains(err.Error(), fmt.Sprintf("exceeds maximum %d", blockSize))
}

// TestSIMDScalarFormatCompatibility verifies that SIMD and scalar implementations produce
// identical binary output for the same input. This is critical for wire format compatibility.
func TestSIMDScalarFormatCompatibility(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD not available")
	}

	for bitWidth := 1; bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth_%d", bitWidth), func(t *testing.T) {
			// Create test input with known values
			values := make([]uint32, blockSize)
			mask := uint32(0xFFFFFFFF)
			if bitWidth < 32 {
				mask = (1 << bitWidth) - 1
			}
			for i := range values {
				values[i] = uint32(i*7+3) & mask
			}

			payloadLen := payloadBytes(bitWidth)

			// Pack with SIMD
			simdPayload := make([]byte, payloadLen)
			ok := simdPack(simdPayload, values, bitWidth)
			if !ok {
				t.Fatalf("simdPack failed for bitWidth %d", bitWidth)
			}

			// Pack with scalar
			scalarPayload := make([]byte, payloadLen)
			packLanesScalar(scalarPayload, values, bitWidth)

			// Compare byte-by-byte
			if !bytes.Equal(simdPayload, scalarPayload) {
				t.Errorf("SIMD and scalar produced different output for bitWidth %d", bitWidth)
				// Show first difference
				for i := range simdPayload {
					if simdPayload[i] != scalarPayload[i] {
						t.Errorf("First difference at byte %d: SIMD=0x%02x, scalar=0x%02x", i, simdPayload[i], scalarPayload[i])
						break
					}
				}
			}

			// Also verify unpacking compatibility:
			// Data packed by SIMD should unpack correctly by scalar
			scalarUnpacked := make([]uint32, blockSize)
			unpackLanesScalar(scalarUnpacked, simdPayload, blockSize, bitWidth)
			for i := range values {
				if scalarUnpacked[i] != values[i] {
					t.Errorf("Scalar unpack of SIMD data failed at index %d: got %d, want %d", i, scalarUnpacked[i], values[i])
					break
				}
			}

			// Data packed by scalar should unpack correctly by SIMD
			simdUnpacked := make([]uint32, blockSize)
			ok = simdUnpack(simdUnpacked, scalarPayload, bitWidth, blockSize)
			if !ok {
				t.Fatalf("simdUnpack failed for bitWidth %d", bitWidth)
			}
			for i := range values {
				if simdUnpacked[i] != values[i] {
					t.Errorf("SIMD unpack of scalar data failed at index %d: got %d, want %d", i, simdUnpacked[i], values[i])
					break
				}
			}
		})
	}
}

// TestSIMDPackDirectly verifies simdPack succeeds
func TestSIMDPackDirectly(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD not available")
	}

	data := make([]uint32, 128)
	for i := range data {
		data[i] = uint32(i)
	}

	const bitWidth = 7
	dst := make([]byte, bitWidth*16)

	ok := simdPack(dst, data, bitWidth)
	t.Logf("simdPack returned: %v", ok)

	if !ok {
		t.Error("simdPack returned false unexpectedly")
	}
}

// TestPackUnpackLanesScalar covers the scalar lane helpers regardless of SIMD availability.
func TestPackUnpackLanesScalar(t *testing.T) {
	t.Run("zeroWidthNoop", func(t *testing.T) {
		assert := assert.New(t)
		src := []uint32{1, 2, 3}
		// Should not panic even though payload is empty.
		packLanesScalar(nil, src, 0)

		dst := []uint32{10, 11, 12}
		unpackLanesScalar(dst, nil, len(dst), 0)
		for i, v := range dst {
			assert.Equalf(uint32(0), v, "dst[%d] should be zero after bitWidth=0 unpack", i)
		}
	})

	t.Run("roundTripSubset", func(t *testing.T) {
		assert := assert.New(t)
		const width = 11
		values := make([]uint32, 57) // short of full block to exercise padding paths
		mask := uint32((1 << width) - 1)
		for i := range values {
			values[i] = (uint32(i*97) + 13) & mask
		}

		payload := make([]byte, payloadBytes(width))
		packLanesScalar(payload, values, width)

		dst := make([]uint32, 2*blockSize) // cap >= 256 for zero-allocation
		for i := range dst {
			dst[i] = mathMaxUint32 // sentinel to ensure untouched slots stay intact
		}
		unpackLanesScalar(dst, payload, len(values), width)

		assert.Equal(values, dst[:len(values)], "scalar lane round trip mismatch")
		for i, v := range dst[len(values):] {
			assert.Equalf(mathMaxUint32, v, "dst tail overwritten at %d", len(values)+i)
		}
	})
}

// TestApplyExceptionsBehavior validates both the successful patch path and the guard rails.
func TestApplyExceptionsBehavior(t *testing.T) {
	assert := assert.New(t)

	t.Run("patchesValues", func(t *testing.T) {
		dst := []uint32{1, 2, 3, 4}
		scratch := make([]uint32, blockSize)
		positions := []byte{1, 3}
		highBits := []uint32{5, 2}
		buf := buildExceptionBuf(positions, highBits)

		patchBytes, err := applyExceptions(dst, buf, 0, len(dst), 3, scratch)
		assert.NoError(err)
		assert.Equal(len(buf), patchBytes, "patch bytes should match buffer length")
		assert.Equal(uint32(2)|(5<<3), dst[1], "unexpected patch at index 1")
		assert.Equal(uint32(4)|(2<<3), dst[3], "unexpected patch at index 3")
	})

	t.Run("errorOnOutOfRange", func(t *testing.T) {
		dst := make([]uint32, 4)
		scratch := make([]uint32, blockSize)
		positions := []byte{byte(len(dst))} // index 4 is out of range for 4-element slice
		buf := buildExceptionBuf(positions, []uint32{1})
		_, err := applyExceptions(dst, buf, 0, len(dst), 5, scratch)
		assert.Error(err)
		assert.Contains(err.Error(), fmt.Sprintf("exception index %d out of range", len(dst)))
	})

	t.Run("errorOnTruncatedBuffer", func(t *testing.T) {
		dst := make([]uint32, 4)
		scratch := make([]uint32, blockSize)
		_, err := applyExceptions(dst, []byte{}, 0, len(dst), 5, scratch)
		assert.Error(err)
		assert.Contains(err.Error(), "missing exception count byte")
	})
}

// TestUnpackUint32WithBuffer validates the caller-provided scratch buffer path.
func TestUnpackUint32WithBuffer(t *testing.T) {
	assert := assert.New(t)

	// Test with data that triggers exceptions
	data := genDataWithSmallExceptions()
	buf := PackUint32(nil, data)

	dst := make([]uint32, 0, blockSize)
	scratch := make([]uint32, blockSize)

	result, err := UnpackUint32WithBuffer(dst, scratch, buf)
	assert.NoError(err)
	assert.Equal(data, result)

	// Test with no exceptions
	data2 := genSequential(blockSize)
	buf2 := PackUint32(nil, data2)

	dst2 := make([]uint32, 0, blockSize)
	scratch2 := make([]uint32, blockSize)

	result2, err := UnpackUint32WithBuffer(dst2, scratch2, buf2)
	assert.NoError(err)
	assert.Equal(data2, result2)

	// Test empty
	buf3 := PackUint32(nil, nil)
	scratch3 := make([]uint32, blockSize)

	result3, err := UnpackUint32WithBuffer(nil, scratch3, buf3)
	assert.NoError(err)
	assert.Nil(result3)
}

// TestUnpackUint32WithBufferErrors covers invalid scratch buffers.
func TestUnpackUint32WithBufferErrors(t *testing.T) {
	assert := assert.New(t)

	validBuf := PackUint32(nil, genSequential(blockSize))

	// Test insufficient scratch capacity
	dst := make([]uint32, 0, blockSize)
	scratch := make([]uint32, 64) // Too small
	_, err := UnpackUint32WithBuffer(dst, scratch, validBuf)
	assert.Error(err)
	assert.Contains(err.Error(), "scratch capacity too small")
}

// TestUnpackUint32WithLength validates the WithLength variant returns correct values and byte count.
func TestUnpackUint32WithLength(t *testing.T) {
	assert := assert.New(t)

	t.Run("noExceptions", func(t *testing.T) {
		data := genSequential(blockSize)
		buf := PackUint32(nil, data)

		result, consumed, err := UnpackUint32WithLength(nil, buf)
		assert.NoError(err)
		assert.Equal(data, result)
		assert.Equal(len(buf), consumed)

		blockLen, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(blockLen, consumed)
	})

	t.Run("withExceptions", func(t *testing.T) {
		data := genDataWithSmallExceptions()
		buf := PackUint32(nil, data)

		result, consumed, err := UnpackUint32WithLength(nil, buf)
		assert.NoError(err)
		assert.Equal(data, result)
		assert.Equal(len(buf), consumed)

		blockLen, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(blockLen, consumed)
	})

	t.Run("empty", func(t *testing.T) {
		buf := PackUint32(nil, nil)

		result, consumed, err := UnpackUint32WithLength(nil, buf)
		assert.NoError(err)
		assert.Nil(result)
		assert.Equal(len(buf), consumed)
	})

	t.Run("singleValue", func(t *testing.T) {
		data := []uint32{42}
		buf := PackUint32(nil, data)

		result, consumed, err := UnpackUint32WithLength(nil, buf)
		assert.NoError(err)
		assert.Equal(data, result)
		assert.Equal(len(buf), consumed)
	})

	t.Run("reusesDst", func(t *testing.T) {
		data := []uint32{5, 6, 7, 8}
		buf := PackUint32(nil, data)
		dst := make([]uint32, 2*blockSize)

		out, consumed, err := UnpackUint32WithLength(dst[:0], buf)
		assert.NoError(err)
		assert.Equal(data, out)
		assert.Equal(len(buf), consumed)
		if len(out) > 0 {
			assert.Equal(&dst[0], &out[0], "expected to reuse dst backing array")
		}
	})

	t.Run("rejectsShortBuffer", func(t *testing.T) {
		header := encodeHeader(4, 5, 0)
		buf := make([]byte, headerBytes)
		binary.LittleEndian.PutUint32(buf, header)
		_, _, err := UnpackUint32WithLength(nil, buf)
		assert.ErrorIs(err, ErrInvalidBuffer)
	})
}

// TestUnpackUint32WithBufferAndLength validates the WithBufferAndLength variant.
func TestUnpackUint32WithBufferAndLength(t *testing.T) {
	assert := assert.New(t)

	t.Run("withExceptions", func(t *testing.T) {
		data := genDataWithSmallExceptions()
		buf := PackUint32(nil, data)

		dst := make([]uint32, 0, blockSize)
		scratch := make([]uint32, blockSize)

		result, consumed, err := UnpackUint32WithBufferAndLength(dst, scratch, buf)
		assert.NoError(err)
		assert.Equal(data, result)
		assert.Equal(len(buf), consumed)

		blockLen, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(blockLen, consumed)
	})

	t.Run("noExceptions", func(t *testing.T) {
		data := genSequential(blockSize)
		buf := PackUint32(nil, data)

		dst := make([]uint32, 0, blockSize)
		scratch := make([]uint32, blockSize)

		result, consumed, err := UnpackUint32WithBufferAndLength(dst, scratch, buf)
		assert.NoError(err)
		assert.Equal(data, result)
		assert.Equal(len(buf), consumed)

		blockLen, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(blockLen, consumed)
	})

	t.Run("empty", func(t *testing.T) {
		buf := PackUint32(nil, nil)
		scratch := make([]uint32, blockSize)

		result, consumed, err := UnpackUint32WithBufferAndLength(nil, scratch, buf)
		assert.NoError(err)
		assert.Nil(result)
		assert.Equal(len(buf), consumed)
	})

	t.Run("scratchTooSmall", func(t *testing.T) {
		validBuf := PackUint32(nil, genSequential(blockSize))
		dst := make([]uint32, 0, blockSize)
		scratch := make([]uint32, 64)
		_, _, err := UnpackUint32WithBufferAndLength(dst, scratch, validBuf)
		assert.Error(err)
		assert.Contains(err.Error(), "scratch capacity too small")
	})
}

// TestWithLengthConsumedMatchesBlockLength verifies all unpack variants and
// BlockLength agree on byte counts across various data patterns.
func TestWithLengthConsumedMatchesBlockLength(t *testing.T) {
	assert := assert.New(t)

	cases := []struct {
		name string
		src  []uint32
	}{
		{"nil", nil},
		{"sequential", genSequential(blockSize)},
		{"smallExceptions", genDataWithSmallExceptions()},
		{"largeExceptions", genDataWithLargeExceptions()},
		{"shortBlock", []uint32{0, 1, 1, 2, 3, 5, 8, 13, 21}},
		{"singleValue", []uint32{123456}},
		{"maxValues", func() []uint32 {
			max := ^uint32(0)
			return []uint32{max, 0, max - 1, 1234567890, 42, max}
		}()},
	}

	scratch := make([]uint32, blockSize)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := PackUint32(nil, tc.src)

			blockLen, err := BlockLength(buf)
			assert.NoError(err)
			assert.Equal(len(buf), blockLen)

			// UnpackUint32 (original, no byte count)
			decodedA, err := UnpackUint32(nil, buf)
			assert.NoError(err)
			assert.Equal(tc.src, decodedA)

			// UnpackUint32WithBuffer (original, no byte count)
			decodedB, err := UnpackUint32WithBuffer(nil, scratch, buf)
			assert.NoError(err)
			assert.Equal(tc.src, decodedB)

			// UnpackUint32WithLength (new)
			decodedC, consumedC, err := UnpackUint32WithLength(nil, buf)
			assert.NoError(err)
			assert.Equal(tc.src, decodedC)
			assert.Equal(blockLen, consumedC, "WithLength consumed mismatch")

			// UnpackUint32WithBufferAndLength (new)
			decodedD, consumedD, err := UnpackUint32WithBufferAndLength(nil, scratch, buf)
			assert.NoError(err)
			assert.Equal(tc.src, decodedD)
			assert.Equal(blockLen, consumedD, "WithBufferAndLength consumed mismatch")
		})
	}
}

// TestWithLengthDeltaEncoded verifies the WithLength variants with delta-encoded data.
func TestWithLengthDeltaEncoded(t *testing.T) {
	assert := assert.New(t)

	t.Run("monotonic", func(t *testing.T) {
		original := genMonotonic(blockSize)
		src := slices.Clone(original)
		buf := PackDeltaUint32(nil, src)

		result, consumed, err := UnpackUint32WithLength(nil, buf)
		assert.NoError(err)
		assert.Equal(original, result)
		assert.Equal(len(buf), consumed)

		blockLen, err := BlockLength(buf)
		assert.NoError(err)
		assert.Equal(blockLen, consumed)
	})

	t.Run("zigzag", func(t *testing.T) {
		original := []uint32{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901}
		src := slices.Clone(original)
		buf := PackDeltaUint32(nil, src)

		result, consumed, err := UnpackUint32WithLength(nil, buf)
		assert.NoError(err)
		assert.Equal(original, result)
		assert.Equal(len(buf), consumed)
	})

	t.Run("overflowReported", func(t *testing.T) {
		deltas := []uint32{0xFFFFFFFF, 1}
		buf := PackAlreadyDeltaUint32(nil, deltas)

		_, consumed, err := UnpackUint32WithLength(nil, buf)
		var overflow *ErrOverflow
		assert.True(errors.As(err, &overflow), "overflow error should be returned")
		assert.Equal(uint8(1), overflow.Position)
		assert.Equal(len(buf), consumed, "bytesConsumed should be valid even on overflow")
	})
}

// TestWithLengthMultiBlock demonstrates iterating over consecutive blocks
// using the WithLength variants (the primary use case from Issue #1).
func TestWithLengthMultiBlock(t *testing.T) {
	assert := assert.New(t)

	blocks := [][]uint32{
		genSequential(blockSize),
		genDataWithSmallExceptions(),
		{42, 100, 200},
		genMonotonic(64),
	}

	// Pack all blocks into a single buffer
	var combined []byte
	for _, block := range blocks {
		combined = PackUint32(combined, block)
	}

	// Iterate using UnpackUint32WithLength
	offset := 0
	for i, expected := range blocks {
		result, consumed, err := UnpackUint32WithLength(nil, combined[offset:])
		assert.NoError(err, "block %d", i)
		assert.Equal(expected, result, "block %d", i)
		assert.Greater(consumed, 0, "block %d must consume bytes", i)
		offset += consumed
	}
	assert.Equal(len(combined), offset, "should consume entire buffer")

	// Same iteration using UnpackUint32WithBufferAndLength
	offset = 0
	scratch := make([]uint32, blockSize)
	for i, expected := range blocks {
		result, consumed, err := UnpackUint32WithBufferAndLength(nil, scratch, combined[offset:])
		assert.NoError(err, "block %d", i)
		assert.Equal(expected, result, "block %d", i)
		offset += consumed
	}
	assert.Equal(len(combined), offset, "should consume entire buffer")
}

// buildExceptionBuf creates a properly formatted exception buffer for testing.
// Layout: count(1) + svb_len(2) + positions(N) + StreamVByte(M)
func buildExceptionBuf(positions []byte, highBits []uint32) []byte {
	svbData := encodeHighBitsForTest(highBits)
	buf := make([]byte, 1+2+len(positions)+len(svbData))
	buf[0] = byte(len(positions))
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(svbData)))
	copy(buf[3:], positions)
	copy(buf[3+len(positions):], svbData)
	return buf
}

// TestDeltaEncodeDecodeScalar directly exercises the scalar delta helpers to
// keep coverage regardless of SIMD selection.
func TestDeltaEncodeDecodeScalar(t *testing.T) {
	assert := assert.New(t)
	monotonic := []uint32{3, 5, 8, 13, 21}
	buf := make([]uint32, len(monotonic))
	useZigZag := deltaEncodeScalar(buf, monotonic)
	assert.False(useZigZag, "expected monotonic deltas to skip zigzag encoding")
	recovered := make([]uint32, len(monotonic))
	deltaDecodeScalar(recovered, buf, false)
	assert.Equal(monotonic, recovered, "deltaDecodeScalar mismatch for monotonic input")

	nonMonotonic := []uint32{100, 90, 95, 80, 200}
	buf = make([]uint32, len(nonMonotonic))
	useZigZag = deltaEncodeScalar(buf, nonMonotonic)
	assert.True(useZigZag, "expected zigzag encoding for negative deltas")
	recovered = make([]uint32, len(nonMonotonic))
	deltaDecodeScalar(recovered, buf, true)
	assert.Equal(nonMonotonic, recovered, "zigzag deltaDecodeScalar mismatch")
}

// TestZigZagEncodeDecode32 confirms round-trip correctness for notable signed values.
func TestZigZagEncodeDecode32(t *testing.T) {
	assert := assert.New(t)
	values := []int32{0, 1, -1, 2, -2, 123456, -123456, 1<<30 - 1, -(1 << 30)}
	for _, v := range values {
		encoded := zigzagEncode32(v)
		decoded := zigzagDecode32(encoded)
		assert.Equalf(v, decoded, "zigzag round trip mismatch for %d (encoded=%d)", v, encoded)
	}
}

// TestDeltaDecodeSIMDInPlaceZigZag verifies that deltaDecodeSIMD correctly handles
// the case where dst and deltas are the same slice and useZigZag is true.
// This is a regression test for a bug where zigzagDecodeSIMDAsm mutated the input
// slice in place, corrupting deltas before the prefix-sum completed.
func TestDeltaDecodeSIMDInPlaceZigZag(t *testing.T) {
	assert := assert.New(t)

	// Log whether SIMD is available
	t.Logf("SIMD available: %v", IsSIMDavailable())

	// Create test values with negative deltas (requires zigzag encoding)
	original := []uint32{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901}

	// Encode deltas with zigzag
	deltas := make([]uint32, len(original))
	useZigZag := deltaEncodeScalar(deltas, original)
	assert.True(useZigZag, "expected zigzag encoding for negative deltas")
	t.Logf("Encoded deltas: %v", deltas)

	// Test scalar version first (should always work)
	scalarResult := make([]uint32, len(original))
	copy(scalarResult, deltas)
	deltaDecodeScalar(scalarResult, scalarResult, useZigZag)
	assert.Equal(original, scalarResult, "scalar decode with same dst/src should work")

	// Test the current deltaDecode function pointer (may be SIMD or scalar)
	// This is the actual code path used by UnpackDelta
	simdResult := make([]uint32, len(original))
	copy(simdResult, deltas)
	deltaDecode(simdResult, simdResult, useZigZag) // This is what UnpackDelta does
	assert.Equal(original, simdResult, "deltaDecode with same dst/src and zigzag should work")

	// Verify SIMD matches scalar exactly
	assert.Equal(scalarResult, simdResult, "SIMD and scalar results should match")
}

// TestDeltaDecodeSIMDvScalarComparison directly compares SIMD and scalar
// implementations to ensure they produce identical results for various inputs.
func TestDeltaDecodeSIMDvScalarComparison(t *testing.T) {
	if !IsSIMDavailable() {
		t.Skip("SIMD not available")
	}

	testCases := []struct {
		name   string
		values []uint32
	}{
		{"short_zigzag", []uint32{1000, 900, 950, 800, 1200}},
		{"medium_zigzag", genMixed(64)},
		{"full_block_zigzag", genMixed(128)},
		{"alternating", func() []uint32 {
			v := make([]uint32, 64)
			for i := range v {
				if i%2 == 0 {
					v[i] = uint32(1000 + i*10)
				} else {
					v[i] = uint32(500 + i*5)
				}
			}
			return v
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			// Encode with zigzag
			deltas := make([]uint32, len(tc.values))
			useZigZag := deltaEncodeScalar(deltas, tc.values)
			if !useZigZag {
				t.Skip("test case doesn't require zigzag")
			}

			// Decode with scalar (using separate buffers)
			scalarDeltas := make([]uint32, len(deltas))
			copy(scalarDeltas, deltas)
			scalarResult := make([]uint32, len(tc.values))
			deltaDecodeScalar(scalarResult, scalarDeltas, useZigZag)

			// Decode with deltaDecode (which uses SIMD when available)
			// Using same slice for dst and deltas (the typical UnpackDelta case)
			simdResult := make([]uint32, len(deltas))
			copy(simdResult, deltas)
			deltaDecode(simdResult, simdResult, useZigZag)

			// Compare
			assert.Equal(tc.values, scalarResult, "scalar failed to recover original")
			assert.Equal(tc.values, simdResult, "SIMD failed to recover original")
			assert.Equal(scalarResult, simdResult, "SIMD and scalar results differ")
		})
	}
}

// TestDeltaDecodeDoesNotMutateInput verifies that deltaDecode does not mutate
// the input deltas slice when dst and deltas are different slices.
// This is a regression test for a bug where the SIMD path mutated deltas in-place.
func TestDeltaDecodeDoesNotMutateInput(t *testing.T) {
	assert := assert.New(t)

	// Create test values with negative deltas (requires zigzag encoding)
	original := []uint32{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901}

	// Encode deltas with zigzag
	deltas := make([]uint32, len(original))
	useZigZag := deltaEncodeScalar(deltas, original)
	assert.True(useZigZag, "expected zigzag encoding for negative deltas")

	// Make a copy to verify deltas aren't mutated
	deltasCopy := make([]uint32, len(deltas))
	copy(deltasCopy, deltas)

	// Decode with dst != deltas
	dst := make([]uint32, len(original))
	deltaDecode(dst, deltas, useZigZag)

	// Verify the result is correct
	assert.Equal(original, dst, "decode failed")

	// Verify deltas was not mutated
	assert.Equal(deltasCopy, deltas, "deltaDecode mutated input deltas slice")
}

// TestUnpackDeltaZigZagRegression is an end-to-end test for the zigzag+delta
// decoding bug where SIMD path corrupted data when dst==deltas.
func TestUnpackDeltaZigZagRegression(t *testing.T) {
	assert := assert.New(t)

	// Values that trigger zigzag encoding (has negative deltas)
	testCases := [][]uint32{
		{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901},
		{100, 50, 75, 25, 200, 150, 300, 100},
		genMixed(64),  // Random fluctuations
		genMixed(128), // Full block with fluctuations
	}

	for i, original := range testCases {
		t.Run(fmt.Sprintf("case_%d_len_%d", i, len(original)), func(t *testing.T) {
			src := slices.Clone(original)
			buf := PackDeltaUint32(nil, src)

			// Verify zigzag flag is set (confirms we're testing the right path)
			header := binary.LittleEndian.Uint32(buf[:headerBytes])
			_, _, _, _, _, hasZigZag, _ := decodeHeader(header)
			if !hasZigZag {
				t.Skip("test case doesn't trigger zigzag encoding")
			}

			// UnpackDelta uses deltaDecode(dst, dst, useZigZag)
			result, err := UnpackUint32(nil, buf)
			assert.NoError(err)
			assert.Equal(original, result, "UnpackDelta zigzag round-trip failed")
		})
	}
}

// TestDeltaDecodeInPlaceAliasing explicitly tests that deltaDecode handles
// the dst==deltas aliasing case correctly with zigzag encoding.
// This uses PackDelta/UnpackDelta end-to-end to ensure proper handling.
func TestDeltaDecodeInPlaceAliasing(t *testing.T) {
	assert := assert.New(t)
	t.Logf("SIMD available: %v", IsSIMDavailable())

	// Test cases with various sizes to exercise different code paths
	sizes := []int{8, 16, 32, 64, 128}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Create data with negative deltas to trigger zigzag
			original := make([]uint32, size)
			val := uint32(10000)
			for i := range original {
				if i%3 == 0 {
					val -= uint32(100 + i*7)
				} else {
					val += uint32(50 + i*3)
				}
				original[i] = val
			}

			// Use PackDelta/UnpackDelta end-to-end
			src := slices.Clone(original)
			buf := PackDeltaUint32(nil, src)

			// Verify zigzag flag is set
			header := binary.LittleEndian.Uint32(buf[:headerBytes])
			_, _, _, _, _, hasZigZag, _ := decodeHeader(header)
			if !hasZigZag {
				t.Skip("couldn't create data that triggers zigzag")
			}

			// UnpackDelta uses deltaDecode(dst, dst, useZigZag)
			result, err := UnpackUint32(nil, buf)
			assert.NoError(err)
			assert.Equal(original, result, "zigzag delta round-trip failed for size %d", size)
		})
	}
}

// -----------------------------------------------------------------------------
// Overflow handling tests
// -----------------------------------------------------------------------------

// TestPackAlreadyDeltaUint32SetsOverflowFlagOnlyWhenNeeded verifies PackAlreadyDeltaUint32 only sets
// the will-overflow flag when the deltas would actually overflow during decode.
func TestPackAlreadyDeltaUint32SetsOverflowFlagOnlyWhenNeeded(t *testing.T) {
	assert := assert.New(t)

	// Case 1: Small deltas that won't overflow - flag should NOT be set
	smallDeltas := []uint32{100, 5, 7, 3, 10, 8}
	buf1 := PackAlreadyDeltaUint32(nil, smallDeltas)
	header1 := bo.Uint32(buf1[:headerBytes])
	_, _, _, _, hasDelta1, _, hasOverflow1 := decodeHeader(header1)
	assert.True(hasDelta1, "PackAlreadyDeltaUint32 should set delta flag")
	assert.False(hasOverflow1, "small deltas should NOT set overflow flag")

	// Case 2: Deltas that will overflow - flag SHOULD be set
	overflowDeltas := []uint32{0xFFFFFFFF, 1} // will overflow at index 1
	buf2 := PackAlreadyDeltaUint32(nil, overflowDeltas)
	header2 := bo.Uint32(buf2[:headerBytes])
	_, _, _, _, hasDelta2, _, hasOverflow2 := decodeHeader(header2)
	assert.True(hasDelta2, "PackAlreadyDeltaUint32 should set delta flag")
	assert.True(hasOverflow2, "overflow deltas SHOULD set overflow flag")

	// Verify actual overflow position
	_, err := UnpackUint32(nil, buf2)
	var overflow *ErrOverflow
	assert.True(errors.As(err, &overflow), "overflow error should be returned")
	assert.Equal(uint8(1), overflow.Position, "overflow should be at index 1")

	// Case 3: Large but non-overflowing deltas - flag should NOT be set
	largeDeltas := []uint32{0x80000000, 0x7FFFFFFF} // sum = 0xFFFFFFFF, no overflow
	buf3 := PackAlreadyDeltaUint32(nil, largeDeltas)
	header3 := bo.Uint32(buf3[:headerBytes])
	_, _, _, _, hasDelta3, _, hasOverflow3 := decodeHeader(header3)
	assert.True(hasDelta3, "PackAlreadyDeltaUint32 should set delta flag")
	assert.False(hasOverflow3, "large but non-overflow deltas should NOT set overflow flag")
}

// TestPackDeltaUint32DoesNotSetMayOverflowFlag verifies that PackDeltaUint32 does NOT set the will-overflow flag.
func TestPackDeltaUint32DoesNotSetMayOverflowFlag(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{100, 105, 112, 118, 125}
	buf := PackDeltaUint32(nil, values)

	header := bo.Uint32(buf[:headerBytes])
	_, _, _, _, hasDelta, _, willOverflow := decodeHeader(header)

	assert.True(hasDelta, "PackDeltaUint32 should set delta flag")
	assert.False(willOverflow, "PackDeltaUint32 should NOT set will-overflow flag")
}

// TestUnpackUint32DeltaNoOverflow verifies that no overflow is reported when deltas don't overflow.
func TestUnpackUint32DeltaNoOverflow(t *testing.T) {
	assert := assert.New(t)

	// Small deltas that won't overflow
	deltas := []uint32{100, 50, 30, 20, 10}
	buf := PackAlreadyDeltaUint32(nil, deltas)

	decoded, err := UnpackUint32(nil, buf)
	assert.NoError(err, "no overflow should occur")

	// Verify prefix sum: 100, 150, 180, 200, 210
	expected := []uint32{100, 150, 180, 200, 210}
	assert.Equal(expected, decoded)
}

// TestUnpackUint32DeltaWithOverflow verifies overflow detection at various positions.
func TestUnpackUint32DeltaWithOverflow(t *testing.T) {
	assert := assert.New(t)

	// Create deltas that will overflow at index 3
	// Starting from 0xFFFFFFF0, adding more will overflow
	deltas := []uint32{0xFFFFFFF0, 5, 5, 10, 5} // overflow at index 3 (0xFFFFFFF0 + 5 + 5 + 10 overflows)
	buf := PackAlreadyDeltaUint32(nil, deltas)

	decoded, err := UnpackUint32(nil, buf)
	var overflow *ErrOverflow
	assert.True(errors.As(err, &overflow), "overflow error should be returned")
	assert.Equal(uint8(3), overflow.Position, "overflow should be at index 3")

	// Values before overflow should still be decoded (with wraparound after)
	assert.Equal(uint32(0xFFFFFFF0), decoded[0])
	assert.Equal(uint32(0xFFFFFFF5), decoded[1])
	assert.Equal(uint32(0xFFFFFFFA), decoded[2])
	// Index 3 causes overflow: 0xFFFFFFFA + 10 = overflow
}

// TestUnpackUint32DeltaOverflowAtIndex1 verifies overflow at the earliest possible position.
func TestUnpackUint32DeltaOverflowAtIndex1(t *testing.T) {
	assert := assert.New(t)

	// Overflow at index 1 (second element) - the earliest possible
	deltas := []uint32{0xFFFFFFFF, 1} // 0xFFFFFFFF + 1 = overflow
	buf := PackAlreadyDeltaUint32(nil, deltas)

	_, err := UnpackUint32(nil, buf)
	var overflow *ErrOverflow
	assert.True(errors.As(err, &overflow), "overflow error should be returned")
	assert.Equal(uint8(1), overflow.Position, "overflow should be at index 1 (0-based)")
}

// TestPackDeltaUint32AlwaysReturnsNoOverflow verifies the fast path returns no overflow.
func TestPackDeltaUint32AlwaysReturnsNoOverflow(t *testing.T) {
	assert := assert.New(t)

	// Even with large values, PackDeltaUint32 computes deltas, so no overflow
	values := []uint32{1000000, 1000005, 1000010, 1000015}
	buf := PackDeltaUint32(nil, values)

	_, err := UnpackUint32(nil, buf)
	assert.NoError(err, "PackDeltaUint32 blocks should never report overflow")
}

// TestReaderOverflowPos verifies Reader correctly reports overflow position.
func TestReaderOverflowPos(t *testing.T) {
	assert := assert.New(t)

	// No overflow case
	deltas1 := []uint32{100, 50, 30}
	buf1 := PackAlreadyDeltaUint32(nil, deltas1)
	reader1 := NewReader()
	err := reader1.Load(buf1)
	assert.NoError(err)
	assert.False(reader1.HasOverflow(), "HasOverflow should be false when no overflow occurred")
	assert.Equal(uint8(0), reader1.OverflowPos(), "OverflowPos should be 0 when no overflow")

	// Overflow case
	deltas2 := []uint32{0xFFFFFFFF, 1}
	buf2 := PackAlreadyDeltaUint32(nil, deltas2)
	reader2 := NewReader()
	err = reader2.Load(buf2)
	assert.NoError(err) // Load succeeds, overflow is detected but stored
	assert.True(reader2.HasOverflow(), "HasOverflow should be true when overflow occurred")
	assert.Equal(uint8(1), reader2.OverflowPos(), "OverflowPos should be 1 (0-based index)")
}

// TestSlimReaderOverflowPosIteration verifies SlimReader detects overflow during Next() iteration.
func TestSlimReaderOverflowPosIteration(t *testing.T) {
	assert := assert.New(t)

	// Overflow case
	deltas := []uint32{0xFFFFFFF0, 5, 5, 20}
	buf := PackAlreadyDeltaUint32(nil, deltas)

	reader := NewSlimReader()
	err := reader.Load(buf)
	assert.NoError(err)
	assert.True(reader.HasOverflow(), "HasOverflow should be true when flag is set")
	assert.Equal(uint8(0), reader.OverflowPos(), "OverflowPos should be 0 before iteration")

	// Iterate through all values
	for {
		_, _, ok := reader.Next()
		if !ok {
			break
		}
	}

	assert.True(reader.HasOverflow(), "HasOverflow should still be true")
	assert.Equal(uint8(3), reader.OverflowPos(), "OverflowPos should be 3 (0xFFFFFFF0 + 5 + 5 + 20 overflows)")
}

// TestSlimReaderOverflowPosDecode verifies SlimReader detects overflow during Decode().
func TestSlimReaderOverflowPosDecode(t *testing.T) {
	assert := assert.New(t)

	deltas := []uint32{0xFFFFFFFF, 1}
	buf := PackAlreadyDeltaUint32(nil, deltas)

	reader := NewSlimReader()
	err := reader.Load(buf)
	assert.NoError(err)

	_ = reader.Decode(nil) // This should trigger overflow detection

	assert.True(reader.HasOverflow(), "HasOverflow should be true when flag is set")
	assert.Equal(uint8(1), reader.OverflowPos(), "OverflowPos should be 1 (0-based index)")
}

// TestPackAlreadyDeltaUint32RoundTrip verifies the full round-trip without overflow.
func TestPackAlreadyDeltaUint32RoundTrip(t *testing.T) {
	assert := assert.New(t)

	// Create deltas that represent differences between values
	// Simulating: 1000, 1010, 1025, 1040, 1060 → deltas: 1000, 10, 15, 15, 20
	deltas := []uint32{1000, 10, 15, 15, 20}
	buf := PackAlreadyDeltaUint32(nil, deltas)

	decoded, err := UnpackUint32(nil, buf)
	assert.NoError(err)

	// Expected prefix sums: 1000, 1010, 1025, 1040, 1060
	expected := []uint32{1000, 1010, 1025, 1040, 1060}
	assert.Equal(expected, decoded)
}

// TestErrOverflowError verifies the ErrOverflow error type and its Error() method.
func TestErrOverflowError(t *testing.T) {
	assert := assert.New(t)

	deltas := []uint32{0xFFFFFFFF, 1}
	buf := PackAlreadyDeltaUint32(nil, deltas)

	_, err := UnpackUint32(nil, buf)

	// Verify it's an error
	assert.Error(err)

	// Verify error message contains useful info
	assert.Contains(err.Error(), "overflow")
	assert.Contains(err.Error(), "1") // position

	// Verify errors.As works
	var overflow *ErrOverflow
	assert.True(errors.As(err, &overflow))
	assert.Equal(uint8(1), overflow.Position)

	// Verify errors.Is does NOT match (it's a pointer type, not sentinel)
	assert.False(errors.Is(err, &ErrOverflow{}))
}

// TestOverflowAtPosition127 verifies overflow detection at the last possible position (index 127).
func TestOverflowAtPosition127(t *testing.T) {
	assert := assert.New(t)

	// Create a full block (128 elements) where overflow happens at index 127
	deltas := make([]uint32, 128)
	// All small values except the last one triggers overflow
	for i := range deltas {
		deltas[i] = 0x02000000 // Each adds ~33 million
	}
	// After 127 additions: 127 * 0x02000000 = 0xFE000000
	// At index 127: 0xFE000000 + 0x02000000 = 0x100000000 = overflow!

	buf := PackAlreadyDeltaUint32(nil, deltas)

	decoded, err := UnpackUint32(nil, buf)
	var overflow *ErrOverflow
	assert.True(errors.As(err, &overflow), "overflow error should be returned")
	assert.Equal(uint8(127), overflow.Position, "overflow should be at index 127")

	// Verify values up to overflow are correct
	var expected uint32
	for i := 0; i < 127; i++ {
		expected += 0x02000000
		assert.Equal(expected, decoded[i], "value at index %d", i)
	}

	// Test with Reader
	reader := NewReader()
	err = reader.Load(buf)
	assert.NoError(err)
	assert.True(reader.HasOverflow())
	assert.Equal(uint8(127), reader.OverflowPos())

	// Test with SlimReader iteration
	slimReader := NewSlimReader()
	err = slimReader.Load(buf)
	assert.NoError(err)
	for {
		_, _, ok := slimReader.Next()
		if !ok {
			break
		}
	}
	assert.True(slimReader.HasOverflow())
	assert.Equal(uint8(127), slimReader.OverflowPos())
}

// TestInvalidFlagCombinationZigZagAndWillOverflow verifies that zigzag + willOverflow flags are accepted (performance optimization).
func TestInvalidFlagCombinationZigZagAndWillOverflow(t *testing.T) {
	assert := assert.New(t)

	// Create values with negative deltas so PackDeltaUint32 uses zigzag encoding
	// (300 → 200 → 100 has negative deltas)
	values := []uint32{300, 200, 100}
	buf := PackDeltaUint32(nil, values)

	// Verify zigzag flag is set
	header := bo.Uint32(buf[:headerBytes])
	_, _, _, _, _, hasZigZag, _ := decodeHeader(header)
	assert.True(hasZigZag, "test setup: zigzag flag should be set for negative deltas")

	// Manually set the will-overflow flag in the header (bit 16)
	header |= headerWillOverflowFlag
	bo.PutUint32(buf[:headerBytes], header)

	// UnpackUint32 now accepts this combination for performance - no longer validates flag combinations
	_, err := UnpackUint32(nil, buf)
	assert.NoError(err, "should accept zigzag + will-overflow combination (performance optimization)")

	// SlimReader should also accept this combination
	reader := NewSlimReader()
	err = reader.Load(buf)
	assert.NoError(err, "SlimReader should accept invalid flag combination (performance optimization)")
}

// -----------------------------------------------------------------------------
// StreamVByte exception compression tests
// -----------------------------------------------------------------------------

// TestStreamVByteExceptionCompression demonstrates that StreamVByte reduces
// storage for small exception high bits compared to fixed 4-byte encoding.
func TestStreamVByteExceptionCompression(t *testing.T) {
	assert := assert.New(t)

	// Scenario: mostly 8-bit values with a few larger values that have small high bits
	src := make([]uint32, blockSize)
	for i := range src {
		src[i] = uint32(i % 256) // 8-bit values
	}
	// Add some exceptions with small high bits (9-bit values)
	src[10] = 256 + 1   // 9 bits, high bits = 1 (1 byte in StreamVByte)
	src[20] = 256 + 2   // 9 bits, high bits = 1 (1 byte in StreamVByte)
	src[30] = 256 + 100 // 9 bits, high bits = 1 (1 byte in StreamVByte)
	src[40] = 256 + 255 // 9 bits, high bits = 1 (1 byte in StreamVByte)
	src[50] = 512 + 50  // 10 bits, high bits = 2 (1 byte in StreamVByte)

	buf := assertRoundTrip(t, src)
	excCount := getExceptionCount(buf)
	assert.Equal(5, excCount, "expected 5 exceptions")

	// Old format would be: 1 + 5 + 5*4 = 26 bytes for exception area
	// StreamVByte format: 1 + 5 + 2 + ~5 bytes = ~13 bytes (high bits are all small)
	payloadLen := payloadBytes(getBitWidth(buf))
	excAreaLen := len(buf) - headerBytes - payloadLen
	oldExcSize := 1 + excCount + excCount*4 // Old format size

	t.Logf("Exception area: actual=%d bytes, old format would be=%d bytes (%.1f%% reduction)",
		excAreaLen, oldExcSize, 100*(1-float64(excAreaLen)/float64(oldExcSize)))
	assert.Less(excAreaLen, oldExcSize, "StreamVByte should reduce exception storage for small high bits")
}

// TestStreamVByteMaxCompressionBenefit shows best-case compression with single-byte exceptions.
func TestStreamVByteMaxCompressionBenefit(t *testing.T) {
	// All zeros except one large value forces many exceptions with high bits = 0 or small
	src := make([]uint32, blockSize)
	// Set all values to 1 (requires 1 bit)
	for i := range src {
		src[i] = 1
	}
	// Add one large value forcing exceptions on the 1-bit values
	// Actually, let's create a scenario where bitWidth=0 is chosen with many exceptions
	for i := range src {
		src[i] = uint32(i%4 + 1) // Values 1-4, all fit in 3 bits
	}
	// Add outliers that force exceptions
	for i := range 10 {
		src[i*12] = uint32(8 + i) // 4-bit values, will be exceptions with 3-bit width
	}

	buf := assertRoundTrip(t, src)
	excCount := getExceptionCount(buf)
	if excCount > 0 {
		payloadLen := payloadBytes(getBitWidth(buf))
		excAreaLen := len(buf) - headerBytes - payloadLen
		oldExcSize := 1 + excCount + excCount*4

		t.Logf("Exceptions=%d: actual=%d bytes, old=%d bytes (%.1f%% reduction)",
			excCount, excAreaLen, oldExcSize, 100*(1-float64(excAreaLen)/float64(oldExcSize)))
	}
}

// TestStreamVByteWorstCaseOverhead shows worst-case where StreamVByte adds overhead.
func TestStreamVByteWorstCaseOverhead(t *testing.T) {
	assert := assert.New(t)

	// Scenario: many exceptions with large high bits (32-bit values)
	// StreamVByte will use 4 bytes per value plus control bytes
	src := make([]uint32, blockSize)
	for i := range src {
		src[i] = mathMaxUint32 - uint32(i) // All 32-bit values
	}

	buf := assertRoundTrip(t, src)
	excCount := getExceptionCount(buf)

	// With all max values, there should be no exceptions (bitWidth=32)
	// Let's force exceptions by having some zeros
	src2 := make([]uint32, blockSize)
	for i := range src2 {
		if i%10 == 0 {
			src2[i] = mathMaxUint32 // Large outliers
		} else {
			src2[i] = 0 // Zeros that fit in 0 bits
		}
	}

	buf2 := assertRoundTrip(t, src2)
	excCount2 := getExceptionCount(buf2)
	if excCount2 > 0 {
		width := getBitWidth(buf2)
		payloadLen := payloadBytes(width)
		excAreaLen := len(buf2) - headerBytes - payloadLen
		oldExcSize := 1 + excCount2 + excCount2*4

		// StreamVByte overhead: 2 bytes for length + ~1 control byte per 4 values
		// For max uint32 high bits, each value needs 4 bytes in StreamVByte
		t.Logf("Large exceptions=%d (width=%d): actual=%d bytes, old=%d bytes (%.1f%% change)",
			excCount2, width, excAreaLen, oldExcSize, 100*(float64(excAreaLen)/float64(oldExcSize)-1))
	}
	_ = excCount      // Use first result
	assert.True(true) // Test documents behavior rather than asserting specific values
}

// -----------------------------------------------------------------------------
// Fuzzy failures
// -----------------------------------------------------------------------------

// FuzzPackRoundTrip stresses PackUint32/UnpackUint32 with randomized raw inputs.
func FuzzPackRoundTrip(f *testing.F) {
	corpus := [][]uint32{
		nil,
		{0},
		{mathMaxUint32},
		{1, 2, 3, 4, 5},
		genSequential(blockSize),
		genMixed(32),
	}
	for _, seed := range corpus {
		if len(seed) == 0 {
			f.Add([]byte{})
			continue
		}
		f.Add(encodeValuesSeed(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		values := fuzzBytesToValues(data)
		buf := assertRoundTrip(t, values)
		assertValidEncoding(t, buf)
	})
}

// FuzzPackDeltaRoundTrip stresses PackDelta/UnpackDelta with randomized inputs.
func FuzzPackDeltaRoundTrip(f *testing.F) {
	corpus := [][]uint32{
		nil,
		genMonotonic(8),
		genMixed(8),
		genMixed(blockSize),
	}
	for _, seed := range corpus {
		if len(seed) == 0 {
			f.Add([]byte{})
			continue
		}
		f.Add(encodeValuesSeed(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		values := fuzzBytesToValues(data)
		buf := assertDeltaRoundTrip(t, values)
		assertValidEncoding(t, buf)
	})
}

// FuzzSIMDScalarByteCompatibility verifies that SIMD and scalar implementations
// produce byte-identical packed output for arbitrary inputs.
func FuzzSIMDScalarByteCompatibility(f *testing.F) {
	if !IsSIMDavailable() {
		f.Skip("SIMD not available")
	}

	// Seed with various test cases
	corpus := []struct {
		bitWidth int
		values   []uint32
	}{
		{1, genSequential(blockSize)},
		{8, genSequential(blockSize)},
		{16, genSequential(blockSize)},
		{32, genSequential(blockSize)},
		{7, genMixed(blockSize)},
		{13, genMixed(blockSize)},
	}
	for _, seed := range corpus {
		// Encode as: bitWidth (1 byte) + values (4 bytes each)
		data := make([]byte, 1+len(seed.values)*4)
		data[0] = byte(seed.bitWidth)
		for i, v := range seed.values {
			binary.LittleEndian.PutUint32(data[1+i*4:], v)
		}
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 1 {
			return
		}

		// Extract bit width (1-32)
		bitWidth := int(data[0])%32 + 1

		// Extract values (up to blockSize)
		numValues := (len(data) - 1) / 4
		if numValues > blockSize {
			numValues = blockSize
		}
		if numValues == 0 {
			return
		}

		values := make([]uint32, numValues)
		mask := uint32(0xFFFFFFFF)
		if bitWidth < 32 {
			mask = (1 << bitWidth) - 1
		}
		for i := range values {
			if 1+i*4+4 <= len(data) {
				values[i] = binary.LittleEndian.Uint32(data[1+i*4:]) & mask
			}
		}

		// Pad to blockSize for full block test
		fullValues := make([]uint32, blockSize)
		copy(fullValues, values)

		payloadLen := payloadBytes(bitWidth)

		// Pack with SIMD
		simdPayload := make([]byte, payloadLen)
		if !simdPack(simdPayload, fullValues, bitWidth) {
			t.Fatal("simdPack failed")
		}

		// Pack with scalar
		scalarPayload := make([]byte, payloadLen)
		packLanesScalar(scalarPayload, fullValues, bitWidth)

		// Compare byte-by-byte
		if !bytes.Equal(simdPayload, scalarPayload) {
			t.Errorf("SIMD and scalar produced different output for bitWidth %d", bitWidth)
			for i := range simdPayload {
				if simdPayload[i] != scalarPayload[i] {
					t.Errorf("First difference at byte %d: SIMD=0x%02x, scalar=0x%02x", i, simdPayload[i], scalarPayload[i])
					break
				}
			}
		}

		// Verify cross-unpacking works
		simdUnpacked := make([]uint32, blockSize)
		scalarUnpacked := make([]uint32, blockSize)

		unpackLanesScalar(scalarUnpacked, simdPayload, blockSize, bitWidth)
		if !simdUnpack(simdUnpacked, scalarPayload, bitWidth, blockSize) {
			t.Fatal("simdUnpack failed")
		}

		for i := range fullValues {
			if scalarUnpacked[i] != fullValues[i] {
				t.Errorf("Scalar unpack of SIMD data failed at index %d: got %d, want %d", i, scalarUnpacked[i], fullValues[i])
				break
			}
			if simdUnpacked[i] != fullValues[i] {
				t.Errorf("SIMD unpack of scalar data failed at index %d: got %d, want %d", i, simdUnpacked[i], fullValues[i])
				break
			}
		}
	})
}

var (
	resultBytes []byte
	resultU32   []uint32
)

func BenchmarkPackUint32(b *testing.B) {
	data := genSequential(blockSize)
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for range b.N {
		dst = PackUint32(dst[:0], data)
	}
	resultBytes = dst
}

func BenchmarkUnpackUint32(b *testing.B) {
	buf := PackUint32(nil, genSequential(blockSize))
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst, _ = UnpackUint32(dst[:0], buf)
	}
	resultU32 = dst
}

func BenchmarkPackDeltaUint32(b *testing.B) {
	source := genMonotonic(blockSize)
	data := make([]uint32, blockSize)
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for range b.N {
		copy(data, source)
		dst = PackDeltaUint32(dst[:0], data)
	}
	resultBytes = dst
}

func BenchmarkUnpackDeltaUint32(b *testing.B) {
	source := slices.Clone(genMonotonic(blockSize))
	buf := PackDeltaUint32(nil, source)
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst, _ = UnpackUint32(dst[:0], buf)
	}
	resultU32 = dst
}

func BenchmarkPackDeltaMixed(b *testing.B) {
	source := genMixed(blockSize)
	data := make([]uint32, blockSize, 2*blockSize) // cap >= 256 for zero-alloc
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for range b.N {
		copy(data, source)
		dst = PackDeltaUint32(dst[:0], data)
	}
	resultBytes = dst
}

func BenchmarkUnpackDeltaMixed(b *testing.B) {
	source := slices.Clone(genMixed(blockSize))
	buf := PackDeltaUint32(nil, source)
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst, _ = UnpackUint32(dst[:0], buf)
	}
	resultU32 = dst
}

// BenchmarkPackWithExceptions measures encoding with StreamVByte exception handling.
func BenchmarkPackWithExceptions(b *testing.B) {
	data := genDataWithSmallExceptions()
	dst := make([]byte, 0, headerBytes+payloadBytes(16)+patchBytesMax(10))
	b.ReportAllocs()
	for range b.N {
		dst = PackUint32(dst[:0], data)
	}
	resultBytes = dst
}

// BenchmarkUnpackWithExceptions measures decoding with StreamVByte exception handling.
func BenchmarkUnpackWithExceptions(b *testing.B) {
	buf := PackUint32(nil, genDataWithSmallExceptions())
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst, _ = UnpackUint32(dst[:0], buf)
	}
	resultU32 = dst
}

// BenchmarkPackWithLargeExceptions measures encoding with large exception high bits.
func BenchmarkPackWithLargeExceptions(b *testing.B) {
	data := genDataWithLargeExceptions()
	dst := make([]byte, 0, headerBytes+payloadBytes(32)+patchBytesMax(20))
	b.ReportAllocs()
	for range b.N {
		dst = PackUint32(dst[:0], data)
	}
	resultBytes = dst
}

// BenchmarkUnpackWithLargeExceptions measures decoding with large exception high bits.
func BenchmarkUnpackWithLargeExceptions(b *testing.B) {
	buf := PackUint32(nil, genDataWithLargeExceptions())
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst, _ = UnpackUint32(dst[:0], buf)
	}
	resultU32 = dst
}

// BenchmarkUnpackStackVsHeapBuffer compares stack allocation vs heap buffer reuse.
func BenchmarkUnpackStackVsHeapBuffer(b *testing.B) {
	b.Run("WithExceptions", func(b *testing.B) {
		data := genDataWithSmallExceptions()
		buf := PackUint32(nil, data)

		b.Run("StackAllocated", func(b *testing.B) {
			dst := make([]uint32, 0, blockSize)
			b.ReportAllocs()
			for range b.N {
				var err error
				dst, err = UnpackUint32(dst[:0], buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			resultU32 = dst
		})

		b.Run("HeapBufferReuse", func(b *testing.B) {
			dst := make([]uint32, 0, blockSize)
			scratch := make([]uint32, blockSize)
			b.ReportAllocs()
			for range b.N {
				var err error
				dst, err = UnpackUint32WithBuffer(dst[:0], scratch, buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			resultU32 = dst
		})

		b.Run("HeapBufferFresh", func(b *testing.B) {
			dst := make([]uint32, 0, blockSize)
			b.ReportAllocs()
			for range b.N {
				scratch := make([]uint32, blockSize)
				var err error
				dst, err = UnpackUint32WithBuffer(dst[:0], scratch, buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			resultU32 = dst
		})
	})

	b.Run("NoExceptions", func(b *testing.B) {
		data := genSequential(blockSize)
		buf := PackUint32(nil, data)

		b.Run("StackAllocated", func(b *testing.B) {
			dst := make([]uint32, 0, blockSize)
			b.ReportAllocs()
			for range b.N {
				var err error
				dst, err = UnpackUint32(dst[:0], buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			resultU32 = dst
		})

		b.Run("HeapBufferReuse", func(b *testing.B) {
			dst := make([]uint32, 0, blockSize)
			scratch := make([]uint32, blockSize)
			b.ReportAllocs()
			for range b.N {
				var err error
				dst, err = UnpackUint32WithBuffer(dst[:0], scratch, buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			resultU32 = dst
		})

		b.Run("HeapBufferFresh", func(b *testing.B) {
			dst := make([]uint32, 0, blockSize)
			b.ReportAllocs()
			for range b.N {
				scratch := make([]uint32, blockSize)
				var err error
				dst, err = UnpackUint32WithBuffer(dst[:0], scratch, buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			resultU32 = dst
		})
	})
}

// BenchmarkUnpackUint32WithLength measures decoding + byte length for sequential data.
func BenchmarkUnpackUint32WithLength(b *testing.B) {
	buf := PackUint32(nil, genSequential(blockSize))
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	var consumed int
	for range b.N {
		dst, consumed, _ = UnpackUint32WithLength(dst[:0], buf)
	}
	resultU32 = dst
	_ = consumed
}

// BenchmarkUnpackUint32WithBufferAndLength measures decoding + byte length with buffer reuse.
func BenchmarkUnpackUint32WithBufferAndLength(b *testing.B) {
	buf := PackUint32(nil, genSequential(blockSize))
	dst := make([]uint32, 0, blockSize)
	scratch := make([]uint32, blockSize)
	b.ReportAllocs()
	var consumed int
	for range b.N {
		dst, consumed, _ = UnpackUint32WithBufferAndLength(dst[:0], scratch, buf)
	}
	resultU32 = dst
	_ = consumed
}

// BenchmarkUnpackWithLengthExceptions measures WithLength decoding for data with exceptions.
func BenchmarkUnpackWithLengthExceptions(b *testing.B) {
	buf := PackUint32(nil, genDataWithSmallExceptions())
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	var consumed int
	for range b.N {
		dst, consumed, _ = UnpackUint32WithLength(dst[:0], buf)
	}
	resultU32 = dst
	_ = consumed
}

// BenchmarkUnpackWithBufferAndLengthExceptions measures WithBufferAndLength for data with exceptions.
func BenchmarkUnpackWithBufferAndLengthExceptions(b *testing.B) {
	buf := PackUint32(nil, genDataWithSmallExceptions())
	dst := make([]uint32, 0, blockSize)
	scratch := make([]uint32, blockSize)
	b.ReportAllocs()
	var consumed int
	for range b.N {
		dst, consumed, _ = UnpackUint32WithBufferAndLength(dst[:0], scratch, buf)
	}
	resultU32 = dst
	_ = consumed
}

// BenchmarkUnpackHotPathCompare compares decode-only, decode+length, and
// separate BlockLength+decode workflows for hot-path tuning.
func BenchmarkUnpackHotPathCompare(b *testing.B) {
	cases := []struct {
		name string
		src  []uint32
	}{
		{"NoExceptions", genSequential(blockSize)},
		{"WithExceptions", genDataWithSmallExceptions()},
	}

	for _, tc := range cases {
		buf := PackUint32(nil, tc.src)

		b.Run(tc.name, func(b *testing.B) {
			b.Run("UnpackWithBuffer", func(b *testing.B) {
				dst := make([]uint32, 0, blockSize)
				scratch := make([]uint32, blockSize)
				b.ReportAllocs()
				for range b.N {
					var err error
					dst, err = UnpackUint32WithBuffer(dst[:0], scratch, buf)
					if err != nil {
						b.Fatal(err)
					}
				}
				resultU32 = dst
			})

			b.Run("UnpackWithBufferAndLength", func(b *testing.B) {
				dst := make([]uint32, 0, blockSize)
				scratch := make([]uint32, blockSize)
				var consumed int
				b.ReportAllocs()
				for range b.N {
					var err error
					dst, consumed, err = UnpackUint32WithBufferAndLength(dst[:0], scratch, buf)
					if err != nil {
						b.Fatal(err)
					}
				}
				resultU32 = dst
				_ = consumed
			})

			b.Run("BlockLengthThenUnpackWithBuffer", func(b *testing.B) {
				dst := make([]uint32, 0, blockSize)
				scratch := make([]uint32, blockSize)
				var consumed int
				b.ReportAllocs()
				for range b.N {
					var err error
					consumed, err = BlockLength(buf)
					if err != nil {
						b.Fatal(err)
					}
					dst, err = UnpackUint32WithBuffer(dst[:0], scratch, buf[:consumed])
					if err != nil {
						b.Fatal(err)
					}
				}
				resultU32 = dst
				_ = consumed
			})
		})
	}
}

// BenchmarkBlockLength measures standalone block-size scanning throughput.
func BenchmarkBlockLength(b *testing.B) {
	cases := []struct {
		name string
		src  []uint32
	}{
		{"NoExceptions", genSequential(blockSize)},
		{"WithExceptions", genDataWithSmallExceptions()},
	}

	for _, tc := range cases {
		buf := PackUint32(nil, tc.src)

		b.Run(tc.name, func(b *testing.B) {
			var consumed int
			b.ReportAllocs()
			for range b.N {
				var err error
				consumed, err = BlockLength(buf)
				if err != nil {
					b.Fatal(err)
				}
			}
			_ = consumed
		})
	}
}

// blockLengthInlineForBench mirrors BlockLength but computes the exception size
// inline (without calling blockBytesConsumed). Used to measure helper-call overhead.
func blockLengthInlineForBench(buf []byte) (int, error) {
	if len(buf) < headerBytes {
		return 0, ErrInvalidBuffer
	}
	count, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(bo.Uint32(buf[:headerBytes]))
	if count > blockSize {
		return 0, ErrInvalidBuffer
	}

	payloadEnd := headerBytes + payloadBytes(bitWidth)
	if !hasExceptions {
		return payloadEnd, nil
	}

	minExcMeta := payloadEnd + 1 + 2
	if len(buf) < minExcMeta {
		return 0, ErrInvalidBuffer
	}
	excCount := int(buf[payloadEnd])
	if excCount > blockSize {
		return 0, ErrInvalidBuffer
	}
	svbLen := int(bo.Uint16(buf[payloadEnd+1 : payloadEnd+3]))
	return payloadEnd + 1 + 2 + excCount + svbLen, nil
}

// BenchmarkBlockLengthHelperOverhead compares BlockLength against an inlined
// equivalent to quantify any overhead from using blockBytesConsumed as a helper.
func BenchmarkBlockLengthHelperOverhead(b *testing.B) {
	cases := []struct {
		name string
		src  []uint32
	}{
		{"NoExceptions", genSequential(blockSize)},
		{"WithExceptions", genDataWithSmallExceptions()},
	}

	for _, tc := range cases {
		buf := PackUint32(nil, tc.src)

		b.Run(tc.name, func(b *testing.B) {
			b.Run("BlockLength", func(b *testing.B) {
				var consumed int
				b.ReportAllocs()
				for range b.N {
					var err error
					consumed, err = BlockLength(buf)
					if err != nil {
						b.Fatal(err)
					}
				}
				_ = consumed
			})

			b.Run("InlineEquivalent", func(b *testing.B) {
				var consumed int
				b.ReportAllocs()
				for range b.N {
					var err error
					consumed, err = blockLengthInlineForBench(buf)
					if err != nil {
						b.Fatal(err)
					}
				}
				_ = consumed
			})
		})
	}
}

// ----------------------------------------------------------------------------
// Helper functions for generating test data
// ----------------------------------------------------------------------------

// Generate a sequence of n integers sequentiallystarting from 0
func genSequential(n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i)
	}
	return out
}

// Generate a sequence of n integers monotonically increasing
func genMonotonic(n int) []uint32 {
	out := make([]uint32, n)
	var acc uint32
	for i := range out {
		acc += uint32(i%7 + 1)
		out[i] = acc
	}
	return out
}

// Generate a sequence of n integers with random fluctuations
func genMixed(n int) []uint32 {
	out := make([]uint32, n)
	rng := rand.New(rand.NewSource(1234))
	acc := int64(1 << 20)
	for i := range out {
		gain := rng.Intn(4096)
		loss := rng.Intn(4096)
		acc += int64(gain - loss)
		if acc < 0 {
			acc = int64(rng.Intn(1 << 16))
		}
		out[i] = uint32(acc)
	}
	return out
}

// Generate a sequence of n integers for a given bit width
func genValuesForBitWidth(width int) []uint32 {
	if width < 1 || width > 32 {
		panic("unsupported width")
	}
	var val uint32
	if width == 32 {
		val = mathMaxUint32
	} else {
		val = (1 << width) - 1
	}
	out := make([]uint32, blockSize)
	for i := range out {
		out[i] = val
	}
	return out
}

// genDataWithSmallExceptions creates data with small exception high bits for benchmarking.
// Most values fit in 8 bits, with some 9-10 bit values as exceptions.
func genDataWithSmallExceptions() []uint32 {
	out := make([]uint32, blockSize, 2*blockSize) // cap >= 256 for zero-alloc
	for i := range out {
		out[i] = uint32(i % 256) // 8-bit base values
	}
	// Add ~10 exceptions with small high bits
	for i := range 10 {
		out[i*12] = 256 + uint32(i*10) // 9-10 bit values, high bits are small
	}
	return out
}

// genDataWithLargeExceptions creates data with large exception high bits for benchmarking.
// Simulates worst-case StreamVByte compression scenario.
func genDataWithLargeExceptions() []uint32 {
	out := make([]uint32, blockSize, 2*blockSize) // cap >= 256 for zero-alloc
	for i := range out {
		out[i] = 0 // Base values all zero
	}
	// Add ~20 exceptions with large high bits (30-32 bit values)
	for i := range 20 {
		out[i*6] = mathMaxUint32 - uint32(i*1000) // Large 32-bit values
	}
	return out
}

// ----------------------------------------------------------------------------
// Helper functions for converting between byte slices and uint32 slices
// ----------------------------------------------------------------------------

// Convert a byte slice to a slice of uint32 values
func fuzzBytesToValues(data []byte) []uint32 {
	if len(data) == 0 {
		return nil
	}
	count := min((len(data)+3)/4, blockSize)
	values := make([]uint32, count)
	for i := range count {
		start := i * 4
		var v uint32
		for b := range min(4, len(data)-start) {
			v |= uint32(data[start+b]) << (8 * b)
		}
		values[i] = v
	}
	return values
}

// Convert a slice of uint32 values to a byte slice
func encodeValuesSeed(values []uint32) []byte {
	if len(values) == 0 {
		return nil
	}
	out := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[i*4:], v)
	}
	return out
}

// encodeHighBitsForTest encodes high bits using StreamVByte for test purposes
func encodeHighBitsForTest(highBits []uint32) []byte {
	return streamvbyte.EncodeUint32(highBits, nil)
}

// ----------------------------------------------------------------------------
// Helper functions for getting information from the encoded buffer
// ----------------------------------------------------------------------------

func getBitWidth(buf []byte) int {
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, bitWidth, _, _, _, _, _ := decodeHeader(header)
	return bitWidth
}

func getExceptionCount(buf []byte) int {
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
	if !hasExceptions {
		return 0
	}
	return int(buf[headerBytes+payloadBytes(bitWidth)])
}

// ----------------------------------------------------------------------------
// Helper functions for assertions
// ----------------------------------------------------------------------------

// Check for a roundtrip pack <-> unpack to ensure, data is equal
func assertRoundTrip(t *testing.T, src []uint32) []byte {
	t.Helper()
	buf := PackUint32(nil, src)
	got, err := UnpackUint32(nil, buf)
	assert.NoError(t, err)
	assert.Equal(t, len(src), len(got), "length mismatch")
	assert.Equal(t, src, got)
	return buf
}

// Check for a roundtrip pack delta <-> unpack delta to ensure, data is equal
func assertDeltaRoundTrip(t *testing.T, src []uint32) []byte {
	t.Helper()
	// Copy src since PackDelta mutates its input
	data := slices.Clone(src)
	buf := PackDeltaUint32(nil, data)
	got, err := UnpackUint32(nil, buf)
	assert.NoError(t, err)
	assert.Equal(t, len(src), len(got), "length mismatch")
	assert.Equal(t, src, got)
	return buf
}

// Check that the compressed output is smaller than the raw bytes
func assertCompressionBelowRaw(t *testing.T, buf []byte, rawBytes int) {
	t.Helper()
	assert.Less(t, len(buf), rawBytes, "expected compressed output smaller than raw bytes")
}

// Check that the encoded buffer is valid
func assertValidEncoding(t *testing.T, buf []byte) {
	t.Helper()
	if len(buf) < headerBytes {
		t.Fatalf("encoded buffer too small: %d", len(buf))
	}
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	count, bitWidth, _, hasExceptions, _, _, _ := decodeHeader(header)
	if count < 0 || count > blockSize {
		t.Fatalf("invalid element count %d", count)
	}
	payloadLen := payloadBytes(bitWidth)
	minLen := headerBytes + payloadLen
	if len(buf) < minLen {
		t.Fatalf("payload truncated: need %d bytes, have %d", minLen, len(buf))
	}
	if !hasExceptions {
		if len(buf) != minLen {
			t.Fatalf("unexpected trailing bytes without exceptions: got %d want %d", len(buf), minLen)
		}
		return
	}
	// With StreamVByte format: count(1) + svb_len(2) + positions(N) + svb_data(M)
	if len(buf) < minLen+1 {
		t.Fatalf("missing exception count byte")
	}
	excCount := int(buf[minLen])
	if excCount > blockSize {
		t.Fatalf("exception count %d exceeds block size", excCount)
	}
	// Check minimum size for exception area
	minExcLen := 1 + 2 + excCount // count + svb_len + positions
	if len(buf) < minLen+minExcLen {
		t.Fatalf("exception area too small: got %d, need at least %d", len(buf)-minLen, minExcLen)
	}
	// Read StreamVByte length and verify total size
	svbLen := int(binary.LittleEndian.Uint16(buf[minLen+1:]))
	want := minLen + 1 + 2 + excCount + svbLen
	if len(buf) != want {
		t.Fatalf("exception payload mismatch: got %d want %d (count=%d, svbLen=%d)", len(buf), want, excCount, svbLen)
	}
}
