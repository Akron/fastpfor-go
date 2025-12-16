// Package fastpfor implements a FastPFOR integer compression codec.
//
// The codec operates on fixed blocks of up to 128 unsigned 32-bit integers.
// Each block begins with a 32-bit header describing the bit width of the packed
// lane sets followed by the interleaved payload (4 SIMD-friendly lanes) and a
// patch area for exception values. Callers provide the destination slices to
// PackUint32/UnpackUint32 so higher-level codecs can reuse buffers without repeated heap
// allocations. The package maintains no global mutable state.
package fastpfor

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"

	"github.com/mhr3/streamvbyte"
)

// Block configuration constants. PackUint32/UnpackUint32 always operates on at most 128
// integers, interleaved into 4 lanes to match the SIMD-PFOR layout.
const (
	// blockSize is the fixed FastPFOR block length (4 lanes × 32 elements).
	blockSize = 128
	// laneCount splits blocks into four pseudo-lanes so SIMD-friendly packing can interleave them.
	laneCount = 4
	// laneLength is the number of integers stored per lane.
	laneLength = blockSize / laneCount

	// -----------------------------------------------------------------------------
	// Header layout constants
	// -----------------------------------------------------------------------------
	//
	// The 32-bit header is structured as follows (little-endian):
	//
	//	Bits  0-7:   element count (0–128)
	//	Bits  8-13:  bit width for packed values (0–32)
	//	Bits 14-15:  integer type (00=uint8, 01=uint16, 10=uint32, 11=uint64)
	//	Bit  29:     delta flag (1 = values are delta-encoded)
	//	Bit  30:     zigzag flag (1 = deltas are zigzag-encoded)
	//	Bit  31:     exception flag (1 = patch table follows payload)
	headerBytes      = 4 // Size of the header in bytes
	headerCountBits  = 8 // Bits reserved for element count
	headerWidthBits  = 6 // Bits reserved for bit width
	headerCountMask  = (1 << headerCountBits) - 1
	headerWidthMask  = (1 << headerWidthBits) - 1
	headerWidthShift = headerCountBits

	// Integer type encoding (bits 14-15)
	headerTypeBits  = 2
	headerTypeMask  = (1 << headerTypeBits) - 1
	headerTypeShift = headerWidthShift + headerWidthBits // bits 14-15

	// Integer type values (for decoding)
	IntTypeUint8  = 0 // 00 - reserved for future use
	IntTypeUint16 = 1 // 01 - uint16 values
	IntTypeUint32 = 2 // 10 - uint32 values (default/current)
	IntTypeUint64 = 3 // 11 - reserved for future use

	// Integer type flags (for encoding via extraFlags parameter)
	headerTypeUint8Flag  = uint32(IntTypeUint8) << headerTypeShift  // 0x0000 - reserved
	headerTypeUint16Flag = uint32(IntTypeUint16) << headerTypeShift // 0x4000
	headerTypeUint32Flag = uint32(IntTypeUint32) << headerTypeShift // 0x8000 - default
	headerTypeUint64Flag = uint32(IntTypeUint64) << headerTypeShift // 0xC000 - reserved

	// Flag bits in the upper portion of the header
	headerDeltaFlag     = uint32(1 << 29)
	headerZigZagFlag    = uint32(1 << 30)
	headerExceptionFlag = uint32(1 << 31)

	// mathMaxUint32 is the maximum uint32, used while constructing bit masks without conversions.
	mathMaxUint32 = ^uint32(0)
)

// payloadBytesLUT is a precomputed lookup table for payload sizes at each bit width (0-32).
// Each entry is: ((laneLength * bitWidth + 31) / 32 * 4) * laneCount
var payloadBytesLUT = [33]int{
	0, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256,
	272, 288, 304, 320, 336, 352, 368, 384, 400, 416, 432, 448, 464, 480, 496, 512,
}

// packLanes splits the block into four SIMD-friendly lanes and bit-packs each
// lane independently. Missing tail values (len < 128) are treated as zeros.
var packLanes func(dst []byte, values []uint32, bitWidth int) = packLanesScalar

// unpackLanes performs the inverse of packLanes, up to the logical element
// count (tail values outside count retain their previous contents).
var unpackLanes func(dst []uint32, payload []byte, count, bitWidth int) = unpackLanesScalar

var deltaEncode func(dst, src []uint32) bool = deltaEncodeScalar
var deltaDecode func(dst, deltas []uint32, useZigZag bool) = deltaDecodeScalar

var (
	simdAvailable bool
	bo            = binary.LittleEndian
)

// Initialize SIMD path if available
func init() {
	initSIMDSelection()
}

// IsSIMDavailable reports whether SIMD-accelerated pack/unpack paths are active.
func IsSIMDavailable() bool {
	return simdAvailable
}

// MaxBlockSizeUint32 returns the maximum number of bytes needed to store a block of values.
func MaxBlockSizeUint32() int {
	return headerBytes + (blockSize * 4)
}

// PackUint32 encodes up to BlockSize uint32 values into the FastPFOR block format.
// The function appends the block to dst so the caller can reuse buffers and
// avoid per-block allocations. Callers must not reuse the same dst slice across
// concurrent PackUint32 invocations unless they coordinate access themselves.
// Each block writes:
//   - Header (count, bit width, exception flag)
//   - Interleaved lane payload packed at the chosen width
//   - Optional exception table (count byte, positions, high bits)
//
// For zero-allocation operation when data contains exceptions, provide a values
// slice with cap >= 256. The extra capacity (positions 128-255) is used as scratch
// space for exception handling.
func PackUint32(dst []byte, values []uint32) []byte {
	return packInternal(dst, values, headerTypeUint32Flag)
}

// packInternal is called by higher codecs. It validates the block length,
// selects the bit width, and packs the payload. It also appends the exception
// table if there are any exceptions.
//
// The extraFlags parameter can include integer type flags (headerTypeUint16Flag, etc.)
// as well as delta/zigzag flags. If no type flag is set, IntTypeUint32 is used.
func packInternal(dst []byte, values []uint32, extraFlags uint32) []byte {
	validateBlockLength(len(values))
	// Select the bit width that minimizes the serialized size.
	bitWidth, excCount := selectBitWidth(values)
	// Calculate the length of the payload
	payloadLen := payloadBytes(bitWidth)
	// Calculate the maximum length of the block (actual may be smaller due to StreamVByte)
	maxTotal := headerBytes + payloadLen + patchBytesMax(excCount)

	start := len(dst)
	dst = slices.Grow(dst, maxTotal)
	dst = dst[:start+maxTotal]
	flags := extraFlags
	if excCount > 0 {
		flags |= headerExceptionFlag
	}
	header := encodeHeader(len(values), bitWidth, flags)
	bo.PutUint32(dst[start:start+headerBytes], header)

	payloadStart := start + headerBytes
	payloadEnd := payloadStart + payloadLen
	if payloadLen > 0 {
		packLanes(dst[payloadStart:payloadEnd], values, bitWidth)
	}

	// Write exceptions directly, using values[blockSize:] as scratch for high bits
	actualPatchLen := 0
	if excCount > 0 {
		// Ensure values has scratch space (cap >= 256)
		var highBits []uint32
		if cap(values) >= 2*blockSize {
			highBits = values[blockSize : blockSize+excCount]
		} else {
			highBits = make([]uint32, excCount)
		}
		actualPatchLen = writeExceptionsDirect(dst[payloadEnd:], values[:len(values)], bitWidth, highBits)
	}

	// Trim to actual size
	actualTotal := headerBytes + payloadLen + actualPatchLen
	return dst[:start+actualTotal]
}

// UnpackUint32 decodes a PackUint32-produced buffer back into uint32 values, writing into
// the supplied dst slice (which will be resized as needed).
// If the data was delta-encoded (via PackDeltaUint32), it is automatically delta-decoded.
// Returns an error if the buffer is invalid or corrupted.
//
// For zero-allocation operation, provide a dst slice with cap >= 256. The extra capacity
// (positions 128-255) is used as scratch space for exception handling and will not appear
// in the returned slice.
func UnpackUint32(dst []uint32, buf []byte) ([]uint32, error) {
	if len(buf) < headerBytes {
		return nil, fmt.Errorf("%w: buffer too small for header (need %d bytes, got %d)",
			ErrInvalidBuffer, headerBytes, len(buf))
	}
	count, bitWidth, _, hasExceptions, hasDelta, hasZigZag := decodeHeader(bo.Uint32(buf[:headerBytes]))
	if count < 0 || count > blockSize {
		return nil, fmt.Errorf("%w: invalid element count %d", ErrInvalidBuffer, count)
	}

	payloadLen := payloadBytes(bitWidth)
	minNeeded := headerBytes + payloadLen
	if len(buf) < minNeeded {
		return nil, fmt.Errorf("%w: buffer truncated (need %d bytes, got %d)",
			ErrInvalidBuffer, minNeeded, len(buf))
	}

	// Handle empty case without allocation
	if count == 0 {
		if dst == nil {
			return nil, nil
		}
		return dst[:0], nil
	}

	// Ensure capacity for both values and scratch space (2*blockSize = 256)
	dst = ensureUint32Cap(dst, count, 2*blockSize)
	if bitWidth == 0 {
		clear(dst[:count])
	} else {
		unpackLanes(dst[:count], buf[headerBytes:minNeeded], count, bitWidth)
	}

	// Handle exceptions (StreamVByte format), using dst[blockSize:] as scratch
	if hasExceptions {
		scratch := dst[blockSize : 2*blockSize]
		if err := applyExceptions(dst[:count], buf, minNeeded, count, bitWidth, scratch); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidBuffer, err)
		}
	}

	// Apply delta decoding if the data was delta-encoded
	if hasDelta && count > 0 {
		deltaDecode(dst[:count], dst[:count], hasZigZag)
	}

	return dst[:count], nil
}

// PackDeltaUint32 delta-encodes values in-place prior to calling PackUint32.
// WARNING: This function mutates the values slice. If you need to preserve
// the original values, make a copy before calling PackDeltaUint32.
// The delta flag is set in the header so UnpackUint32 can auto-detect and decode.
//
// For zero-allocation operation when data contains exceptions, provide a values
// slice with cap >= 256. The extra capacity (positions 128-255) is used as scratch
// space for exception handling.
func PackDeltaUint32(dst []byte, values []uint32) []byte {
	validateBlockLength(len(values))
	var useZigZag bool
	if len(values) > 0 {
		useZigZag = deltaEncode(values, values) // in-place
	}
	flags := headerTypeUint32Flag | headerDeltaFlag // Always set type and delta flags
	if useZigZag {
		flags |= headerZigZagFlag
	}
	return packInternal(dst, values, flags)
}

// validateBlockLength panics if the caller tries to encode more than BlockSize
// integers. FastPFOR always operates on fixed 128-value chunks.
func validateBlockLength(n int) {
	if n < 0 {
		panic(fmt.Sprintf("fastpfor: invalid block length %d (cannot be negative)", n))
	}
	if n > blockSize {
		panic(fmt.Sprintf("fastpfor: block length %d exceeds maximum %d", n, blockSize))
	}
}

// ensureUint32Cap ensures the destination slice has at least minCap capacity
// and returns it with length n. For zero-allocation operation, pass a slice
// with cap >= 2*blockSize (256) to provide scratch space for exception handling.
func ensureUint32Cap(dst []uint32, n, minCap int) []uint32 {
	if cap(dst) >= minCap {
		return dst[:n]
	}
	return make([]uint32, n, minCap)
}

// requiredBitWidthScalar returns the minimum number of bits needed to encode every
// value in the slice without exceptions. Uses OR-reduction to avoid per-element branching.
func requiredBitWidthScalar(values []uint32) int {
	var orAll uint32
	for _, v := range values {
		orAll |= v
	}
	return bits.Len32(orAll)
}

// payloadBytes returns the lane-aligned number of bytes produced by packing a
// 128-value block at the provided bit width. Each lane stores 32 integers, so
// the result is always a multiple of 16 bytes.
func payloadBytes(bitWidth int) int {
	return payloadBytesLUT[bitWidth]
}

// patchBytesMax returns the maximum number of bytes needed to serialize the exception
// table using StreamVByte encoding for the high bits.
// Layout: count(1) + positions(N) + svb_len(2) + StreamVByte(M)
func patchBytesMax(exceptionCount int) int {
	if exceptionCount == 0 {
		return 0
	}
	return 1 + exceptionCount + 2 + streamvbyte.MaxEncodedLen(exceptionCount)
}

// patchBytesExact returns the exact byte count for a given exception table with
// the StreamVByte-encoded high bits already computed.
/*
func patchBytesExact(exceptionCount, svbLen int) int {
	if exceptionCount == 0 {
		return 0
	}
	return 1 + exceptionCount + 2 + svbLen
}
*/

// encodeHeader encodes the header for a block. It combines the count, bit width, and flags.
// The flags parameter should include the integer type (headerTypeUint16Flag, etc.).
func encodeHeader(count, bitWidth int, flags uint32) uint32 {
	return uint32(count&headerCountMask) |
		(uint32(bitWidth&headerWidthMask) << headerWidthShift) |
		flags
}

// decodeHeader decodes the header for a block. It extracts count, bit width, integer type, and flags.
func decodeHeader(header uint32) (count, bitWidth, intType int, hasExceptions, hasDelta, hasZigZag bool) {
	count = int(header & headerCountMask)
	bitWidth = int((header >> headerWidthShift) & headerWidthMask)
	intType = int((header >> headerTypeShift) & headerTypeMask)
	hasExceptions = header&headerExceptionFlag != 0
	hasDelta = header&headerDeltaFlag != 0
	hasZigZag = header&headerZigZagFlag != 0
	return
}

// packLanesScalar packs the values into the destination buffer using a scalar implementation.
// The format matches bp128 SIMD: lanes are interleaved in 16-byte blocks (4 words per block).
// For bitWidth b, each lane produces b words (since 32 values × b bits = 32b bits = b words).
// These are interleaved: [lane0_word0, lane1_word0, lane2_word0, lane3_word0, lane0_word1, ...]
func packLanesScalar(dst []byte, values []uint32, bitWidth int) {
	if bitWidth == 0 {
		return
	}
	// Reference (FastPFor.cpp):
	//
	//	for(uint32_t k = 0; k < 4; ++k)
	//	  fastpackwithoutmask(in+4*i+k, out + k*bits, bits);
	for lane := range laneCount {
		packLaneInterleaved(dst, values, lane, bitWidth)
	}
}

// packLaneInterleaved packs 32 integers from the specified lane (indices lane, lane+4, …)
// directly into the interleaved output format using a streaming 64-bit accumulator.
// Output words are written at byte offsets: lane*4, lane*4+16, lane*4+32, ... (stride 16 bytes).
func packLaneInterleaved(dst []byte, values []uint32, lane, bitWidth int) {
	// Precompute mask outside the loop to avoid repeated conditional checks
	var mask uint64
	if bitWidth >= 32 {
		mask = uint64(mathMaxUint32)
	} else {
		mask = uint64((1 << bitWidth) - 1)
	}

	var acc uint64
	var bitsInAcc int
	outByteIdx := lane * 4 // Start at lane's first word position

	// Rough C++ equivalent (FastPFor.cpp::fastpackwithoutmask):
	//
	//	for(uint32_t i = 0; i < 32; ++i) {
	//	  const uint64_t value = input[i] & mask;
	//	  buffer |= value << bitOffset;
	//	  if(bitOffset >= 32) { *out++ = uint32_t(buffer); buffer >>= 32; bitOffset -= 32; }
	//	  bitOffset += bitWidth;
	//	}
	for i := range laneLength {
		idx := lane + i*laneCount
		var v uint32
		if idx < len(values) {
			v = values[idx]
		}
		acc |= (uint64(v) & mask) << bitsInAcc
		bitsInAcc += bitWidth
		for bitsInAcc >= 32 {
			bo.PutUint32(dst[outByteIdx:], uint32(acc))
			outByteIdx += 16 // Skip to next word position for this lane (4 lanes × 4 bytes)
			acc >>= 32
			bitsInAcc -= 32
		}
	}
	if bitsInAcc > 0 {
		bo.PutUint32(dst[outByteIdx:], uint32(acc))
	}
}

// unpackLanesScalar unpacks the values from the payload into the destination buffer using a scalar implementation.
// The format matches bp128 SIMD: lanes are interleaved in 16-byte blocks (4 words per block).
func unpackLanesScalar(dst []uint32, payload []byte, count, bitWidth int) {
	if bitWidth == 0 {
		clear(dst[:count])
		return
	}
	for lane := range laneCount {
		unpackLaneInterleaved(dst, payload, lane, bitWidth, count)
	}
}

// unpackLaneInterleaved reconstructs the original integers for a single lane by reading
// directly from the interleaved payload format and writing to the lane's output positions.
// Input words are read from byte offsets: lane*4, lane*4+16, lane*4+32, ... (stride 16 bytes).
func unpackLaneInterleaved(dst []uint32, payload []byte, lane, bitWidth, count int) {
	// Precompute mask outside the loop to avoid repeated conditional checks
	var mask uint32
	if bitWidth >= 32 {
		mask = mathMaxUint32
	} else {
		mask = (1 << bitWidth) - 1
	}

	//	for(uint32_t i = 0; i < 32; ++i) {
	//	  while(bitOffset < bitWidth) { buffer |= (uint64_t)(*in++) << bitOffset; bitOffset += 32; }
	//	  output[i] = uint32_t(buffer) & mask;
	//	  buffer >>= bitWidth;
	//	  bitOffset -= bitWidth;
	//	}

	var acc uint64
	var bitsInAcc int
	inByteIdx := lane * 4 // Start at lane's first word position

	for i := range laneLength {
		for bitsInAcc < bitWidth {
			if inByteIdx+4 > len(payload) {
				bitsInAcc = bitWidth // force exit
				break
			}
			acc |= uint64(bo.Uint32(payload[inByteIdx:])) << bitsInAcc
			inByteIdx += 16 // Skip to next word position for this lane (4 lanes × 4 bytes)
			bitsInAcc += 32
		}
		value := uint32(acc) & mask
		acc >>= bitWidth
		bitsInAcc -= bitWidth
		idx := lane + i*laneCount
		if idx < count {
			dst[idx] = value
		}
	}
}

// selectBitWidth picks the bit width that minimizes the serialized size. It
// mirrors FastPFOR's getBestBFromData routine by computing the histogram of bit
// lengths once, deriving the exception counts for each candidate width from
// that histogram, and only materializing the exception list for the winning
// width.
func selectBitWidth(values []uint32) (width int, excCount int) {

	/*
	   void getBestBFromData(const IntType *in, uint8_t &bestb, uint8_t &bestcexcept,
	                         uint8_t &maxb) {
	     uint8_t bits = sizeof(IntType) * 8;
	     uint32_t freqs[65];
	     for (uint32_t k = 0; k <= bits; ++k) freqs[k] = 0;
	     for (uint32_t k = 0; k < BlockSize; ++k) {
	       freqs[asmbits(in[k])]++;
	     }
	     bestb = bits;
	     while (freqs[bestb] == 0) bestb--;
	     maxb = bestb;
	     uint32_t bestcost = bestb * BlockSize;
	     uint32_t cexcept = 0;
	     bestcexcept = static_cast<uint8_t>(cexcept);
	     for (uint32_t b = bestb - 1; b < bits; --b) {
	       cexcept += freqs[b + 1];
	       uint32_t thiscost = cexcept * overheadofeachexcept +
	                           cexcept * (maxb - b) + b * BlockSize +
	                           8;  // the  extra 8 is the cost of storing maxbits
	       if (maxb - b == 1) thiscost -= cexcept;
	       if (thiscost < bestcost) {
	         bestcost = thiscost;
	         bestb = static_cast<uint8_t>(b);
	         bestcexcept = static_cast<uint8_t>(cexcept);
	       }
	     }
	   }
	*/

	const uint32Bits = 32

	// Single pass: build histogram and find max width via OR-reduction
	var freqs [uint32Bits + 1]int
	var orAll uint32
	for _, v := range values {
		freqs[bits.Len32(v)]++
		orAll |= v
	}
	maxWidth := bits.Len32(orAll)

	bestWidth := maxWidth
	bestSize := headerBytes + payloadBytesLUT[maxWidth]
	bestExcCount := 0

	// Build cumulative "greater than" counts from the histogram
	var greater [uint32Bits + 1]int
	var running int
	for bit := uint32Bits; bit >= 0; bit-- {
		greater[bit] = running
		running += freqs[bit]
	}

	// Check for the optimal bitwidth candidate
	for candidate := range maxWidth {
		excCount := greater[candidate]
		if excCount == 0 {
			continue
		}
		size := headerBytes + payloadBytesLUT[candidate] + patchBytesMax(excCount)
		if size < bestSize || (size == bestSize && candidate < bestWidth) {
			bestSize = size
			bestWidth = candidate
			bestExcCount = excCount
		}
	}

	return bestWidth, bestExcCount
}

// collectExceptionsDirect writes exception positions to dst and high bits to highBits.
// Returns the number of exceptions collected.
func collectExceptionsDirect(values []uint32, bitWidth int, dst []byte, highBits []uint32) int {
	if bitWidth >= 32 {
		return 0
	}
	excIdx := 0
	for i, v := range values {
		if bits.Len32(v) > bitWidth {
			dst[excIdx] = byte(i)
			highBits[excIdx] = v >> bitWidth
			excIdx++
		}
	}
	return excIdx
}

// writeExceptionsDirect serializes exception positions and high bits directly.
// It collects exceptions from values into dst (positions) and highBits buffer,
// then encodes the high bits with StreamVByte.
// Returns the actual number of bytes written.
// Layout:
//
//	dst[0]        : exception count (<= 128)
//	dst[1:n+1]    : byte indices (lane order) of the exceptions
//	dst[n+1:n+3]  : uint16 length of StreamVByte data (little-endian)
//	dst[n+3:]     : StreamVByte-encoded high bits
func writeExceptionsDirect(dst []byte, values []uint32, bitWidth int, highBits []uint32) int {
	// Collect exception positions to dst[1:] and high bits to highBits
	excCount := collectExceptionsDirect(values, bitWidth, dst[1:], highBits)
	if excCount == 0 {
		return 0
	}

	// Write exception count
	dst[0] = byte(excCount)
	pos := 1 + excCount

	// Encode high bits with StreamVByte
	svbData := streamvbyte.EncodeUint32(highBits[:excCount], &streamvbyte.EncodeOptions[uint32]{
		Buffer: dst[pos+2:], // Leave space for length prefix
	})

	// Write the StreamVByte data length
	svbLen := len(svbData)
	bo.PutUint16(dst[pos:], uint16(svbLen))

	return pos + 2 + svbLen
}

// applyExceptions reads exception data from buf at the given offset and applies
// them to dst by reinserting the high parts that were spilled into the exception table.
// The scratch slice is used for StreamVByte decoding to avoid allocations.
// Returns an error if the buffer is malformed.
// Layout: count(1) + positions(N) + svb_len(2) + StreamVByte(M)
func applyExceptions(dst []uint32, buf []byte, offset, count, bitWidth int, scratch []uint32) error {
	if len(buf) < offset+1 {
		return fmt.Errorf("fastpfor: missing exception count byte at offset %d", offset)
	}

	patch := buf[offset:]
	excCount := int(patch[0])
	patch = patch[1:]

	if len(patch) < excCount {
		return fmt.Errorf("fastpfor: truncated exception positions (need %d bytes, got %d)", excCount, len(patch))
	}

	positions := patch[:excCount]
	patch = patch[excCount:]

	if len(patch) < 2 {
		return fmt.Errorf("fastpfor: missing StreamVByte length (need 2 bytes, got %d)", len(patch))
	}

	svbLen := int(bo.Uint16(patch[:2]))
	patch = patch[2:]

	if len(patch) < svbLen {
		return fmt.Errorf("fastpfor: truncated StreamVByte data (need %d bytes, got %d)", svbLen, len(patch))
	}

	// Decode high bits from StreamVByte into scratch buffer (avoids allocation)
	highBits := streamvbyte.DecodeUint32(patch[:svbLen], excCount, &streamvbyte.DecodeOptions[uint32]{
		Buffer: scratch[:excCount],
	})
	for i, idx := range positions {
		if int(idx) >= count {
			return fmt.Errorf("fastpfor: exception index %d out of range (max %d)", int(idx), count-1)
		}
		// OR the high bits of the exception value into the unpacked value at the specified index
		dst[int(idx)] |= highBits[i] << bitWidth
	}
	return nil
}

// deltaEncodeScalar computes first-order deltas in-place (dst may alias src).
// Processes backward to safely support in-place operation: each position i is
// overwritten only after all reads from that position are complete.
// Returns true if zigzag encoding was applied (some deltas were negative).
func deltaEncodeScalar(dst, src []uint32) bool {
	n := len(src)
	if n == 0 {
		return false
	}

	// Single backward pass: compute deltas, detect negatives, apply zigzag on-the-fly.
	// When the first negative delta is detected, we "catch up" by applying zigzag
	// to already-computed deltas, then continue with zigzag for remaining elements.
	needZigZag := false
	for i := n - 1; i > 0; i-- {
		if !needZigZag && src[i] < src[i-1] {
			// First negative delta: apply zigzag to already-computed deltas
			needZigZag = true
			for j := n - 1; j > i; j-- {
				dst[j] = zigzagEncode32(int32(dst[j]))
			}
		}

		delta := src[i] - src[i-1]
		if needZigZag {
			dst[i] = zigzagEncode32(int32(delta))
		} else {
			dst[i] = delta
		}
	}

	// First element (delta from implicit 0)
	if needZigZag {
		dst[0] = zigzagEncode32(int32(src[0]))
	} else {
		dst[0] = src[0]
	}

	return needZigZag
}

// deltaDecodeScalar reconstructs the prefix sums encoded by deltaEncode.
func deltaDecodeScalar(dst, deltas []uint32, useZigZag bool) {
	if useZigZag {
		var prev int64
		for i, d := range deltas {
			prev += int64(zigzagDecode32(d))
			dst[i] = uint32(prev)
		}
		return
	}
	var prev uint32
	for i, d := range deltas {
		prev += d
		dst[i] = prev
	}
}

// zigzagEncode32 encodes a 32-bit integer as a zigzag integer.
func zigzagEncode32(v int32) uint32 {
	return uint32(uint32(v<<1) ^ uint32(v>>31))
}

// zigzagDecode32 decodes a zigzag integer back into a 32-bit integer.
func zigzagDecode32(v uint32) int32 {
	return int32((v >> 1) ^ uint32(-(int32(v & 1))))
}
