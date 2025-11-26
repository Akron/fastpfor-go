package fastpfor

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackLengthValidation(t *testing.T) {
	assert := assert.New(t)
	assert.Panics(func() {
		Pack(nil, make([]uint32, blockSize+1))
	})
}

func TestPackUnpackEmpty(t *testing.T) {
	assertRoundTrip(t, nil)
}

func TestPackUnpackSingleValue(t *testing.T) {
	assertRoundTrip(t, []uint32{123456})
}

func TestPackUnpackShortBlock(t *testing.T) {
	assertRoundTrip(t, []uint32{0, 1, 1, 2, 3, 5, 8, 13, 21})
}

func TestPackUnpackFullBlock(t *testing.T) {
	assertRoundTrip(t, genSequential(blockSize))
}

func TestPackUnpackBitWidth32(t *testing.T) {
	max := ^uint32(0)
	assertRoundTrip(t, []uint32{max, 0, max - 1, 1234567890, 42, max})
}

func TestPackUnpackRandomData(t *testing.T) {
	src := make([]uint32, blockSize)
	rng := rand.New(rand.NewSource(42))
	for i := range src {
		src[i] = rng.Uint32()
	}
	assertRoundTrip(t, src)
}

func TestPackUnpackWithExceptions(t *testing.T) {
	src := make([]uint32, blockSize)
	for i := range src {
		src[i] = uint32(i % 7)
	}
	src[5] = 1 << 30
	src[9] = 1<<29 + 123
	buf := assertRoundTrip(t, src)
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

func TestPackFullBlockSequentialCompression(t *testing.T) {
	assert := assert.New(t)
	src := genSequential(blockSize)
	buf := assertRoundTrip(t, src)
	const bitWidthSequential = 7
	expectedBytes := headerBytes + payloadBytes(bitWidthSequential)
	assert.Equal(expectedBytes, len(buf), "sequential block should compress deterministically")
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

func TestPackFullBlockRandom16BitCompression(t *testing.T) {
	src := make([]uint32, blockSize)
	rng := rand.New(rand.NewSource(2025))
	for i := range src {
		src[i] = uint32(rng.Intn(1 << 16))
	}
	buf := assertRoundTrip(t, src)
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

func TestPackUnpackDeltaEmpty(t *testing.T) {
	assertDeltaRoundTrip(t, nil)
}

func TestPackUnpackDeltaMonotonic(t *testing.T) {
	buf := assertDeltaRoundTrip(t, genMonotonic(blockSize))
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

func TestPackUnpackDeltaMixed(t *testing.T) {
	assertDeltaRoundTrip(t, genMixed(blockSize))
}

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
}

func TestPackDeltaMonotonicDoesNotSetZigZag(t *testing.T) {
	assert := assert.New(t)
	src := genMonotonic(32)
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, src, packScratch)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	_, _, _, hasZigZag := decodeHeader(header)
	assert.False(hasZigZag, "monotonic data should not require zigzag encoding")
}

func TestPackUnpackDeltaZigZagWithExceptions(t *testing.T) {
	assert := assert.New(t)
	src := make([]uint32, 64)
	value := uint32(1 << 20)
	for i := 0; i < len(src); i++ {
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

	got := UnpackDelta(nil, buf)
	assert.Equal(src, got, "zigzag delta with exceptions round-trip mismatch")
}

func TestPackBitWidthCoverage(t *testing.T) {
	buf := make([]byte, 0, headerBytes+payloadBytes(32))
	dst := make([]uint32, blockSize)

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
		assert.Equal(0, exceptionCount(encoded, bitWidth), "width 2 exception count mismatch")
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
		assert.Equal(0, exceptionCount(encoded, bitWidth), "width 5 exception count mismatch")
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
		assert.Equal(0, exceptionCount(encoded, bitWidth), "width 13 exception count mismatch")
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
		assert.Equal(0, exceptionCount(encoded, bitWidth), "width 24 exception count mismatch")
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
		assert.Equal(0, exceptionCount(encoded, bitWidth), "width 32 exception count mismatch")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 32")
		assert.Equal(src, decoded, "width 32 round trip mismatch")
	}

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
		assert.Equal(2, exceptionCount(encoded, bitWidth), "width 5 exception count mismatch")
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
		assert.Equal(3, exceptionCount(encoded, bitWidth), "width 13 exception count mismatch")
		decoded := Unpack(dst[:0], encoded)
		assert.Equal(&dst[0], &decoded[0], "expected Unpack to reuse dst backing array for width 13 exceptions")
		assert.Equal(src, decoded, "width 13 exceptions round trip mismatch")
	}
}

func TestPackDeltaFullBlockCompression(t *testing.T) {
	buf := assertDeltaRoundTrip(t, genMonotonic(blockSize))
	assertCompressionBelowRaw(t, buf, blockSize*4)
}

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

func TestPackAppendsInPlace(t *testing.T) {
	assert := assert.New(t)
	prefix := make([]byte, 8, 128)
	for i := range prefix {
		prefix[i] = byte(i)
	}
	buf := Pack(prefix, []uint32{11, 22})
	assert.Equal(&prefix[0], &buf[0], "expected Pack to reuse dst capacity")
	assert.Equal(prefix, buf[:len(prefix)], "prefix corrupted")
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

func TestUnpackRejectsShortBuffer(t *testing.T) {
	assert := assert.New(t)
	assert.Panics(func() {
		header := encodeHeader(4, 5, 0)
		buf := make([]byte, headerBytes)
		binary.LittleEndian.PutUint32(buf, header)
		Unpack(nil, buf)
	})
}

func TestPackWritesExceptionMetadata(t *testing.T) {
	assert := assert.New(t)
	data := make([]uint32, blockSize)
	for i := range data {
		data[i] = uint32(i & 15)
	}
	data[0] = 1 << 28
	data[63] = 1<<29 + 7

	buf := Pack(nil, data)
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	assert.True(header&headerExceptionFlag != 0, "expected exception flag set")
	width := int((header >> headerWidthShift) & headerWidthMask)
	payload := payloadBytes(width)
	assert.Equal(headerBytes+payload+patchBytes(2), len(buf), "unexpected block size")
}

var (
	resultBytes []byte
	resultU32   []uint32
)

func BenchmarkPack(b *testing.B) {
	data := genSequential(blockSize)
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = Pack(dst[:0], data)
	}
	resultBytes = dst
}

func BenchmarkUnpack(b *testing.B) {
	buf := Pack(nil, genSequential(blockSize))
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = Unpack(dst[:0], buf)
	}
	resultU32 = dst
}

func BenchmarkPackDelta(b *testing.B) {
	data := genMonotonic(blockSize)
	scratch := make([]uint32, blockSize)
	dst := make([]byte, 0, headerBytes+payloadBytes(16))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = PackDelta(dst[:0], data, scratch)
	}
	resultBytes = dst
}

func BenchmarkUnpackDelta(b *testing.B) {
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, genMonotonic(blockSize), packScratch)
	dst := make([]uint32, 0, blockSize)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = UnpackDelta(dst[:0], buf)
	}
	resultU32 = dst
}

// Helpers

func genSequential(n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i)
	}
	return out
}

func genMonotonic(n int) []uint32 {
	out := make([]uint32, n)
	var acc uint32
	for i := range out {
		acc += uint32(i%7 + 1)
		out[i] = acc
	}
	return out
}

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

func expandPatternToBlock(pattern []uint32) []uint32 {
	if len(pattern) == 0 {
		return nil
	}
	out := make([]uint32, blockSize)
	for i := range out {
		out[i] = pattern[i%len(pattern)]
	}
	return out
}

func exceptionCount(buf []byte, bitWidth int) int {
	payloadLen := headerBytes + payloadBytes(bitWidth)
	if len(buf) <= payloadLen {
		return 0
	}
	return int(buf[payloadLen])
}

func assertRoundTrip(t *testing.T, src []uint32) []byte {
	t.Helper()
	buf := Pack(nil, src)
	got := Unpack(nil, buf)
	assert.Equal(t, len(src), len(got), "length mismatch")
	assert.Equal(t, src, got)
	return buf
}

func assertDeltaRoundTrip(t *testing.T, src []uint32) []byte {
	t.Helper()
	packScratch := make([]uint32, blockSize)
	buf := PackDelta(nil, src, packScratch)
	got := UnpackDelta(nil, buf)
	assert.Equal(t, len(src), len(got), "length mismatch")
	assert.Equal(t, src, got)
	return buf
}

func assertCompressionBelowRaw(t *testing.T, buf []byte, rawBytes int) {
	t.Helper()
	assert.Less(t, len(buf), rawBytes, "expected compressed output smaller than raw bytes")
}

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

func fuzzBytesToValues(data []byte) []uint32 {
	if len(data) == 0 {
		return nil
	}
	count := (len(data) + 3) / 4
	if count > blockSize {
		count = blockSize
	}
	values := make([]uint32, count)
	for i := 0; i < count; i++ {
		start := i * 4
		var v uint32
		for b := 0; b < 4; b++ {
			idx := start + b
			if idx >= len(data) {
				break
			}
			v |= uint32(data[idx]) << (8 * b)
		}
		values[i] = v
	}
	return values
}

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
