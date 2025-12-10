package fastpfor

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// Non-delta round-trip tests
// -----------------------------------------------------------------------------

// TestMaxBlockSize verifies the exported MaxBlockSize constant matches internal logic.
func TestMaxBlockSize(t *testing.T) {
	// MaxBlockSize = headerBytes (4) + blockSize (128) * 4 bytes/int = 516
	assert.Equal(t, 516, MaxBlockSize())
}

// TestPackLengthValidation ensures Pack rejects inputs that exceed blockSize.
func TestPackLengthValidation(t *testing.T) {
	assert := assert.New(t)
	assert.Panics(func() {
		Pack(nil, make([]uint32, blockSize+1))
	})
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
	dst := make([]uint32, blockSize)

	// Note: the width 0 is not supported, so we start at 2
	for width := 2; width <= 32; width++ {
		width := width
		t.Run(fmt.Sprintf("width_%02d", width), func(t *testing.T) {
			assert := assert.New(t)
			src := genValuesForBitWidth(width)

			encoded := Pack(buf[:0], src)
			if len(encoded) > 0 {
				assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array")
			}

			header := binary.LittleEndian.Uint32(encoded[:headerBytes])
			_, bitWidth, hasExceptions, _ := decodeHeader(header)

			assert.False(hasExceptions, "unexpected exceptions for width %d", width)
			assert.Equal(width, bitWidth, "header stored wrong bit width")
			assert.Equal(headerBytes+payloadBytes(width), len(encoded), "payload size mismatch")

			decoded := Unpack(dst[:0], encoded)
			if len(decoded) > 0 {
				assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array")
			}
			assert.Equal(src, decoded, "round trip mismatch")
		})
	}
}

// TestPackBitWidthExamples documents concrete width scenarios without exceptions.
func TestPackBitWidthExamples(t *testing.T) {
	assert := assert.New(t)
	buf := make([]byte, 0, headerBytes+payloadBytes(32))
	dst := make([]uint32, blockSize)

	// Width 2 (no exceptions): simple repeating pattern that fits in two bits.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32(i % 4)
		}
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 2")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(2, bitWidth, "width 2 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 2")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 2")
		assert.Equal(src, decoded, "width 2 round trip mismatch")
	}

	// Width 5 (no exceptions): Fibonacci-like sequence staying within five bits.
	{
		src := make([]uint32, blockSize)
		fibLike := []uint32{0, 1, 1, 2, 3, 5, 8, 13, 21, 31}
		for i := range src {
			src[i] = fibLike[i%len(fibLike)]
		}
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 5")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(5, bitWidth, "width 5 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 5")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 5")
		assert.Equal(src, decoded, "width 5 round trip mismatch")
	}

	// Width 13 (no exceptions): quadratic sequence capped at 13 bits.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32((i * i * 17) & ((1 << 13) - 1))
		}
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 13")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(13, bitWidth, "width 13 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 13")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 13")
		assert.Equal(src, decoded, "width 13 round trip mismatch")
	}

	// Width 24 (no exceptions): multiplicative pattern covering 24 bits.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32((i * 123456) & ((1 << 24) - 1))
		}
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 24")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(24, bitWidth, "width 24 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 24")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 24")
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
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 32")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(32, bitWidth, "width 32 header mismatch")
		assert.False(hasExceptions, "unexpected exceptions for width 32")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 32")
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

// TestPackDeltaMonotonicDoesNotSetZigZag ensures monotonic deltas skip zigzag flag.
func TestPackDeltaMonotonicDoesNotSetZigZag(t *testing.T) {
	assert := assert.New(t)
	src := genMonotonic(32)
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, src, packScratch)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, hasZigZag := decodeHeader(header)
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
	src := []uint32{1000, 900, 950, 800, 1200, 1199, 1300, 900, 901}
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, src, packScratch)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, hasZigZag := decodeHeader(header)
	assert.True(hasZigZag, "expected zigzag flag for negative deltas")

	got := UnpackDelta(nil, buf)
	assert.Equal(src, got, "zigzag delta round-trip mismatch")
	// Even though this block only stores 9 logical values, the lane layout would still
	// serialize a full 4×32 payload if bitWidth > 0. It's therefore cheaper to set the
	// width to zero and spill every non-zero value into the exception table.
	assert.Equal(9, getExceptionCount(buf))
	assert.Equal(0, getBitWidth(buf))
}

// TestPackUnpackDeltaZigZagWithExceptions verifies zigzagged data can still patch outliers.
func TestPackUnpackDeltaZigZagWithExceptions(t *testing.T) {
	assert := assert.New(t)
	src := make([]uint32, 64)
	value := uint32(1 << 20)
	for i := range src {
		switch i {
		case 0:
			src[i] = value
		case 20:
			value -= 5000
			src[i] = value
		case 40:
			value += 1 << 24
			src[i] = value
		default:
			value++
			src[i] = value
		}
	}

	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, src, packScratch)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, hasExceptions, hasZigZag := decodeHeader(header)
	assert.True(hasZigZag, "expected zigzag flag when negative delta present")
	assert.True(hasExceptions, "expected exceptions due to large zigzagged delta")
	assert.Equal(3, getExceptionCount(buf))
	assert.Equal(2, getBitWidth(buf))
	got := UnpackDelta(nil, buf)
	assert.Equal(src, got, "zigzag delta with exceptions round-trip mismatch")
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
	assert.Equal(t, 63, len(buf))
	assert.Equal(t, 3, getBitWidth(buf))
	assert.Equal(t, 2, getExceptionCount(buf))
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

// TestPackBitWidthExceptionExamples shows how spikes trigger exception metadata.
func TestPackBitWidthExceptionExamples(t *testing.T) {
	assert := assert.New(t)
	buf := make([]byte, 0, headerBytes+payloadBytes(32))
	dst := make([]uint32, blockSize)

	// Width 5 (with exceptions): low values plus a few spikes that trigger patches.
	{
		src := make([]uint32, blockSize)
		for i := range src {
			src[i] = uint32(16 + (i % 16))
		}
		src[10] = 1<<18 | 7
		src[77] = 1<<20 | 5
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 5 exceptions")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(5, bitWidth, "width 5 exception header mismatch")
		assert.True(hasExceptions, "expected exceptions for width 5 case")
		assert.Equal(2, getExceptionCount(encoded), "width 5 exception count mismatch")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 5 exceptions")
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
		encoded := Pack(buf[:0], src)
		assert.Equal(&buf[:cap(buf)][0], &encoded[0], "expected Pack to reuse dst backing array for width 13 exceptions")
		header := binary.LittleEndian.Uint32(encoded[:headerBytes])
		_, bitWidth, hasExceptions, _ := decodeHeader(header)
		assert.Equal(13, bitWidth, "width 13 exception header mismatch")
		assert.True(hasExceptions, "expected exceptions for width 13 case")
		assert.Equal(3, getExceptionCount(encoded), "width 13 exception count mismatch")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 13 exceptions")
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

	buf := Pack(nil, data)
	assert.Equal(2, getExceptionCount(buf))
	assert.Equal(4, getBitWidth(buf))
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	assert.True(header&headerExceptionFlag != 0, "expected exception flag set")
	width := int((header >> headerWidthShift) & headerWidthMask)
	payload := payloadBytes(width)
	assert.Equal(headerBytes+payload+patchBytes(2), len(buf), "unexpected block size")
}

// -----------------------------------------------------------------------------
// Edge cases
// -----------------------------------------------------------------------------

// TestPackAppendsInPlace ensures Pack can reuse the caller's capacity and that the
// caller can still decode the appended block by slicing off the already-written prefix.
func TestPackAppendsInPlace(t *testing.T) {
	assert := assert.New(t)
	prefix := make([]byte, 8, 128)
	for i := range prefix {
		prefix[i] = byte(i)
	}
	values := []uint32{11, 22}
	buf := Pack(prefix, values)
	assert.Equal(0, getExceptionCount(buf))
	assert.Equal(1, getBitWidth(buf))
	assert.Equal(&prefix[0], &buf[0], "expected Pack to reuse dst capacity")
	assert.Equal(prefix, buf[:len(prefix)], "prefix corrupted")
	decoded := Unpack(nil, buf[len(prefix):])
	assert.Equal(values, decoded, "round trip mismatch for appended block")
	header := binary.LittleEndian.Uint32(buf[len(prefix) : len(prefix)+headerBytes])
	_, width, hasExc, _ := decodeHeader(header)
	payloadLen := payloadBytes(width)
	patchCount := 0
	if hasExc {
		patchCount = int(buf[len(prefix)+headerBytes+payloadLen])
	}
	want := len(prefix) + headerBytes + payloadLen + patchBytes(patchCount)
	assert.Equal(len(buf), want, "unexpected packed length")
}

// TestUnpackReusesDst ensures Unpack writes back into the provided buffer.
func TestUnpackReusesDst(t *testing.T) {
	assert := assert.New(t)
	input := []uint32{5, 6, 7, 8}
	buf := Pack(nil, input)
	dst := make([]uint32, blockSize)
	out := Unpack(dst[:0], buf)
	assert.Equal(len(input), len(out), "length mismatch")
	if len(out) > 0 {
		assert.Equal(&dst[0], &out[0], "expected Unpack to reuse dst backing array")
	}
	assert.Equal(input, out)
}

// TestUnpackRejectsShortBuffer guards against truncated buffers.
func TestUnpackRejectsShortBuffer(t *testing.T) {
	assert := assert.New(t)
	assert.Panics(func() {
		header := encodeHeader(4, 5, 0)
		buf := make([]byte, headerBytes)
		binary.LittleEndian.PutUint32(buf, header)
		Unpack(nil, buf)
	})
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

// TestValidateBlockLengthDirect ensures the guard panics for invalid lengths
// without going through Pack/Unpack.
func TestValidateBlockLengthDirect(t *testing.T) {
	assert := assert.New(t)
	assert.NotPanics(func() { validateBlockLength(0) })
	assert.NotPanics(func() { validateBlockLength(blockSize) })

	assert.PanicsWithValue(
		fmt.Sprintf("fastpfor: invalid block length %d (cannot be negative)", -1),
		func() { validateBlockLength(-1) },
	)
	assert.PanicsWithValue(
		fmt.Sprintf("fastpfor: block length %d exceeds maximum %d", blockSize+1, blockSize),
		func() { validateBlockLength(blockSize + 1) },
	)
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

		dst := make([]uint32, blockSize)
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
		positions := []byte{1, 3}
		values := make([]byte, len(positions)*4)
		binary.LittleEndian.PutUint32(values[0:], 5)
		binary.LittleEndian.PutUint32(values[4:], 2)

		applyExceptions(dst, positions, values, 3)

		assert.Equal(uint32(2)|(5<<3), dst[1], "unexpected patch at index 1")
		assert.Equal(uint32(4)|(2<<3), dst[3], "unexpected patch at index 3")
	})

	t.Run("panicsOnOutOfRange", func(t *testing.T) {
		dst := make([]uint32, 4)
		positions := []byte{byte(len(dst))}
		values := make([]byte, 4)
		assert.PanicsWithValue(
			fmt.Sprintf("fastpfor: exception index %d out of range (max %d)", len(dst), len(dst)-1),
			func() { applyExceptions(dst, positions, values, 5) },
		)
	})
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

// -----------------------------------------------------------------------------
// Fuzzy failures
// -----------------------------------------------------------------------------

// FuzzPackRoundTrip stresses Pack/Unpack with randomized raw inputs.
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

var (
	resultBytes []byte
	resultU32   []uint32
)

func BenchmarkPack(b *testing.B) {
	data := genSequential(blockSize)
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for range b.N {
		dst = Pack(dst[:0], data)
	}
	resultBytes = dst
}

func BenchmarkUnpack(b *testing.B) {
	buf := Pack(nil, genSequential(blockSize))
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst = Unpack(dst[:0], buf)
	}
	resultU32 = dst
}

func BenchmarkPackDelta(b *testing.B) {
	data := genMonotonic(blockSize)
	scratch := make([]uint32, blockSize)
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for range b.N {
		dst = PackDelta(dst[:0], data, scratch)
	}
	resultBytes = dst
}

func BenchmarkUnpackDelta(b *testing.B) {
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, genMonotonic(blockSize), packScratch)
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for range b.N {
		dst = UnpackDelta(dst[:0], buf)
	}
	resultU32 = dst
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

// ----------------------------------------------------------------------------
// Helper functions for getting information from the encoded buffer
// ----------------------------------------------------------------------------

func getBitWidth(buf []byte) int {
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, bitWidth, _, _ := decodeHeader(header)
	return bitWidth
}

func getExceptionCount(buf []byte) int {
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, bitWidth, hasExceptions, _ := decodeHeader(header)
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
	buf := Pack(nil, src)
	got := Unpack(nil, buf)
	assert.Equal(t, len(src), len(got), "length mismatch")
	assert.Equal(t, src, got)
	return buf
}

// Check for a roundtrip pack delta <-> unpack delta to ensure, data is equal
func assertDeltaRoundTrip(t *testing.T, src []uint32) []byte {
	t.Helper()
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, src, packScratch)
	got := UnpackDelta(nil, buf)
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
	count, bitWidth, hasExceptions, _ := decodeHeader(header)
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
	if len(buf) < minLen+1 {
		t.Fatalf("missing exception count byte")
	}
	excCount := int(buf[minLen])
	if excCount > blockSize {
		t.Fatalf("exception count %d exceeds block size", excCount)
	}
	want := minLen + 1 + excCount + excCount*4
	if len(buf) != want {
		t.Fatalf("exception payload mismatch: got %d want %d (count=%d)", len(buf), want, excCount)
	}
}
