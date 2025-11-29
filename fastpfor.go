// Package fastpfor implements a FastPFOR integer compression codec.
//
// The codec operates on fixed blocks of up to 128 unsigned 32-bit integers.
// Each block begins with a 32-bit header describing the bit width of the packed
// lane sets followed by the interleaved payload (4 SIMD-friendly lanes) and a
// patch area for exception values. Callers provide the destination slices to
// Pack/Unpack so higher-level codecs can reuse buffers without repeated heap
// allocations. The package maintains no global mutable state.
package fastpfor

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
)

// Block configuration constants. Pack/Unpack always operates on at most 128
// integers, interleaved into 4 lanes to match the SIMD-PFOR layout.
const (
	// blockSize is the fixed FastPFOR block length (4 lanes × 32 elements).
	blockSize = 128
	// laneCount splits blocks into four pseudo-lanes so SIMD-friendly packing can interleave them.
	laneCount = 4
	// laneLength is the number of integers stored per lane.
	laneLength = blockSize / laneCount

	// headerBytes is the number of bytes reserved for the block header. The
	// serialized 32-bit (word-aligned) header stores
	//   (a) the logical element count (0–127) in 8 bits,
	//   (b) the per-lane bit width used for packing (0–32) in 6 bits,
	//   (c) flag bits that describe optional sections (exceptions, zigzag markers, etc).
	headerBytes = 4

	// headerCountBits reserves 8 bits in the header for the logical value count (<= 128).
	headerCountBits = 8
	// headerWidthBits reserves 6 bits for the packed bit width (enough to cover 0–32).
	headerWidthBits = 6

	// headerCountMask isolates the element-count field inside the header word.
	headerCountMask = (1 << headerCountBits) - 1
	// headerWidthMask isolates the bit-width field inside the header word.
	headerWidthMask = (1 << headerWidthBits) - 1
	// headerWidthShift offsets the width bits immediately after the count bits.
	headerWidthShift = headerCountBits

	// headerExceptionFlag marks that the block contains a trailing exception table.
	headerExceptionFlag = uint32(1 << 31)

	// headerZigZagFlag marks that the block stores zigzag-encoded deltas.
	headerZigZagFlag = uint32(1 << 30)

	// mathMaxUint32 is the maximum uint32, used while constructing bit masks without conversions.
	mathMaxUint32 = ^uint32(0)
)

// packLanes splits the block into four SIMD-friendly lanes and bit-packs each
// lane independently. Missing tail values (len < 128) are treated as zeros.
var packLanes func(dst []byte, values []uint32, bitWidth int) = packLanesScalar

// unpackLanes performs the inverse of packLanes, up to the logical element
// count (tail values outside count retain their previous contents).
var unpackLanes func(dst []uint32, payload []byte, count, bitWidth int) = unpackLanesScalar

// requiredBitWidth returns the minimum number of bits needed to encode every
// value in the slice without exceptions.
var requiredBitWidth func(values []uint32) int = requiredBitWidthScalar

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

// exception tracks a single patched integer: its index in the block and the
// high bits that must be re-applied (OR-ed) after unpacking the truncated value.
type exception struct {
	index uint8
	high  uint32
}

// Pack encodes up to BlockSize uint32 values into the FastPFOR block format.
// The function appends the block to dst so the caller can reuse buffers and
// avoid per-block allocations. Callers must not reuse the same dst slice across
// concurrent Pack invocations unless they coordinate access themselves.
// Each block writes:
//   - Header (count, bit width, exception flag)
//   - Interleaved lane payload packed at the chosen width
//   - Optional exception table (count byte, positions, high bits)
func Pack(dst []byte, values []uint32) []byte {
	return packInternal(dst, values, 0)
}

// packInternal is called by higher codecs. It validates the block length,
// selects the bit width, and packs the payload. It also appends the exception
// table if there are any exceptions.
func packInternal(dst []byte, values []uint32, extraFlags uint32) []byte {
	validateBlockLength(len(values))
	// Select the bit width that minimizes the serialized size.
	// This will also collect the exceptions.
	bitWidth, exceptions := selectBitWidth(values)
	// Calculate the length of the payload
	payloadLen := payloadBytes(bitWidth)
	// Calculate the total length of the block
	total := headerBytes + payloadLen + patchBytes(len(exceptions))

	start := len(dst)
	dst = slices.Grow(dst, total)
	dst = dst[:start+total]
	flags := extraFlags
	if len(exceptions) > 0 {
		flags |= headerExceptionFlag
	}
	header := encodeHeader(len(values), bitWidth, flags)
	bo.PutUint32(dst[start:start+headerBytes], header)

	payloadStart := start + headerBytes
	payloadEnd := payloadStart + payloadLen
	if payloadLen > 0 {
		packLanes(dst[payloadStart:payloadEnd], values, bitWidth)
	}
	if len(exceptions) > 0 {
		writeExceptions(dst[payloadEnd:start+total], exceptions)
	}
	return dst
}

// Unpack decodes a Pack-produced buffer back into uint32 values, writing into
// the supplied dst slice (which will be resized as needed).
func Unpack(dst []uint32, buf []byte) []uint32 {
	if len(buf) < headerBytes {
		panic(fmt.Sprintf("fastpfor: Unpack buffer too small for header (need %d bytes, got %d)", headerBytes, len(buf)))
	}
	count, bitWidth, hasExceptions, _ := decodeHeader(bo.Uint32(buf[:headerBytes]))
	validateBlockLength(count)

	payloadLen := payloadBytes(bitWidth)
	minNeeded := headerBytes + payloadLen
	if len(buf) < minNeeded {
		panic(fmt.Sprintf("fastpfor: buffer truncated: need %d bytes, have %d", minNeeded, len(buf)))
	}

	dst = ensureUint32Len(dst, count)
	if count == 0 {
		return dst[:0]
	}
	if bitWidth == 0 {
		clear(dst)
	} else {
		unpackLanes(dst[:count], buf[headerBytes:minNeeded], count, bitWidth)
	}

	// Handle exceptions
	if hasExceptions {
		if len(buf) < minNeeded+1 {
			panic(fmt.Sprintf("fastpfor: Unpack missing exception count byte at offset %d", minNeeded))
		}
		patch := buf[minNeeded:]
		excCount := int(patch[0]) // Get the number of exceptions <= 128

		patch = patch[1:]
		if len(patch) < excCount {
			panic(fmt.Sprintf("fastpfor: Unpack truncated exception positions (need %d bytes, got %d)", excCount, len(patch)))
		}
		// Read and apply the exceptions
		positions := patch[:excCount]
		patch = patch[excCount:]
		valueBytes := excCount * 4
		if len(patch) < valueBytes {
			panic(fmt.Sprintf("fastpfor: Unpack truncated exception values (need %d bytes, got %d)", valueBytes, len(patch)))
		}
		applyExceptions(dst[:count], positions, patch[:valueBytes], bitWidth)
	}

	return dst
}

// PackDelta delta-encodes values prior to calling Pack. Callers provide a
// scratch buffer (len/cap >= block length) so the wrapper can avoid temporary
// allocations when preparing the delta payload.
func PackDelta(dst []byte, values []uint32, scratch []uint32) []byte {
	validateBlockLength(len(values))
	scratch = ensureUint32Len(scratch, len(values))
	var useZigZag bool
	if len(values) > 0 {
		useZigZag = deltaEncode(scratch[:len(values)], values)
	}
	var flags uint32
	if useZigZag {
		flags |= headerZigZagFlag
	}
	return packInternal(dst, scratch[:len(values)], flags)
}

// UnpackDelta reverses PackDelta by unpacking the delta stream and then
// performing a prefix sum (optionally zigzag-decoded) in place.
func UnpackDelta(dst []uint32, buf []byte) []uint32 {
	if len(buf) < headerBytes {
		panic(fmt.Sprintf("fastpfor: UnpackDelta buffer too small for header (need %d bytes, got %d)", headerBytes, len(buf)))
	}
	header := bo.Uint32(buf[:headerBytes])
	_, _, _, useZigZag := decodeHeader(header)
	dst = Unpack(dst[:0], buf)
	if len(dst) == 0 {
		return dst
	}
	deltaDecode(dst, dst, useZigZag)
	return dst
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

// ensureUint32Len ensures the destination slice has at least n uint32 elements.
func ensureUint32Len(dst []uint32, n int) []uint32 {
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]uint32, n)
}

// requiredBitWidthScalar returns the minimum number of bits needed to encode every
// value in the slice without exceptions.
func requiredBitWidthScalar(values []uint32) int {
	var width int
	for _, v := range values {
		if v == 0 {
			continue
		}
		if w := bits.Len32(v); w > width {
			width = w
		}
	}
	return width
}

// payloadBytes returns the lane-aligned number of bytes produced by packing a
// 128-value block at the provided bit width. Each lane stores 32 integers, so
// the result is always a multiple of 16 bytes.
func payloadBytes(bitWidth int) int {
	if bitWidth == 0 {
		return 0
	}
	bytesPerLane := ((laneLength * bitWidth) + 31) / 32 * 4
	return bytesPerLane * laneCount
}

// patchBytes returns the number of bytes needed to serialize the exception
// table (count byte + positions + 4-byte high parts).
func patchBytes(exceptionCount int) int {
	if exceptionCount == 0 {
		return 0
	}
	return 1 + exceptionCount + exceptionCount*4
}

// encodeHeader encodes the header for a block. It combines the count, bit width, and flags.
func encodeHeader(count, bitWidth int, flags uint32) uint32 {
	return uint32(count&headerCountMask) |
		(uint32(bitWidth&headerWidthMask) << headerWidthShift) |
		flags
}

// decodeHeader decodes the header for a block. It extracts the count, bit width, and flags.
func decodeHeader(header uint32) (count int, bitWidth int, hasExceptions bool, hasZigZag bool) {
	count = int(header & headerCountMask)
	bitWidth = int((header >> headerWidthShift) & headerWidthMask)
	hasExceptions = header&headerExceptionFlag != 0
	hasZigZag = header&headerZigZagFlag != 0
	return
}

// packLanesScalar packs the values into the destination buffer using a scalar implementation.
func packLanesScalar(dst []byte, values []uint32, bitWidth int) {
	if bitWidth == 0 {
		return
	}
	bytesPerLane := len(dst) / laneCount
	// Reference (FastPFor.cpp):
	//
	//	for(uint32_t k = 0; k < 4; ++k)
	//	  fastpackwithoutmask(in+4*i+k, out + k*bits, bits);
	for lane := range laneCount {
		packLaneScalar(dst[lane*bytesPerLane:(lane+1)*bytesPerLane], values, lane, bitWidth)
	}
}

// packLaneScalar packs 32 integers taken from the specified lane (lane, lane+4, …)
// into the destination buffer using a streaming 64-bit accumulator.
func packLaneScalar(output []byte, values []uint32, lane, bitWidth int) {
	// Precompute mask outside the loop to avoid repeated conditional checks
	var mask uint64
	if bitWidth >= 32 {
		mask = uint64(mathMaxUint32)
	} else {
		mask = uint64((1 << bitWidth) - 1)
	}

	var acc uint64
	var bitsInAcc int
	outIdx := 0

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
			bo.PutUint32(output[outIdx:], uint32(acc))
			outIdx += 4
			acc >>= 32
			bitsInAcc -= 32
		}
	}
	if bitsInAcc > 0 {
		bo.PutUint32(output[outIdx:], uint32(acc))
	}
}

// unpackLanesScalar unpacks the values from the payload into the destination buffer using a scalar implementation.
func unpackLanesScalar(dst []uint32, payload []byte, count, bitWidth int) {
	if bitWidth == 0 {
		clear(dst[:count])
		return
	}
	bytesPerLane := len(payload) / laneCount
	for lane := range laneCount {
		unpackLaneScalar(dst, payload[lane*bytesPerLane:(lane+1)*bytesPerLane], lane, bitWidth, count)
	}
}

// unpackLaneScalar reconstructs the original integers for a single lane and writes
// them back into dst at the interleaved lane offsets. Mirrors packLane but in
// reverse order (a literal translation of FastPFor.cpp::fastunpack)
func unpackLaneScalar(dst []uint32, input []byte, lane, bitWidth, count int) {
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
	inIdx := 0
	for i := range laneLength {
		for bitsInAcc < bitWidth {
			if inIdx >= len(input) {
				acc |= uint64(0) << bitsInAcc
				bitsInAcc = bitWidth // force exit
				break
			}
			acc |= uint64(bo.Uint32(input[inIdx:])) << bitsInAcc
			inIdx += 4
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
func selectBitWidth(values []uint32) (width int, exceptions []exception) {

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

	maxWidth := requiredBitWidth(values)
	bestWidth := maxWidth
	bestSize := headerBytes + payloadBytes(maxWidth)
	bestExcCount := 0

	// Initialize the histogram of bit lengths
	var freqs [uint32Bits + 1]int
	for _, v := range values {
		freqs[bits.Len32(v)]++
	}

	var greater [uint32Bits + 1]int
	var running int
	for bit := uint32Bits; bit >= 0; bit-- {
		greater[bit] = running
		running += freqs[bit]
	}

	// Check for the optimal bitwidth candidate
	for candidate := range maxWidth {
		excCount := greater[candidate]

		// If there are no exceptions for this bit width, skip it
		if excCount == 0 {
			continue
		}
		size := headerBytes + payloadBytes(candidate) + patchBytes(excCount)
		if size < bestSize || (size == bestSize && candidate < bestWidth) {
			bestSize = size
			bestWidth = candidate
			bestExcCount = excCount
		}
	}

	if bestWidth == maxWidth {
		return bestWidth, nil
	}
	buf := make([]exception, 0, bestExcCount)
	return bestWidth, collectExceptions(values, bestWidth, buf)
}

// collectExceptions builds the exception list for the provided bit width using
// the caller-supplied buffer to avoid per-candidate allocations.
func collectExceptions(values []uint32, bitWidth int, buf []exception) []exception {
	if bitWidth >= 32 {
		return buf[:0]
	}
	out := buf[:0]
	for i, v := range values {
		if bits.Len32(v) > bitWidth {
			out = append(out, exception{
				index: uint8(i),
				high:  v >> bitWidth,
			})
		}
	}
	return out
}

// writeExceptions serializes the exception count, their positions, and the high
// bits into dst, which must be sized via patchBytes(len(exceptions)).
// Layout mirrors FastPFOR's patch storage:
//
//	patch[0]   : exception count (<= 255)
//	patch[1:n] : byte indices (lane order) of the exceptions
//	patch[n:]  : uint32 high parts aligned to 32 bits
func writeExceptions(dst []byte, exceptions []exception) {
	if len(exceptions) == 0 {
		return
	}

	// Number of exceptions in the block
	dst[0] = byte(len(exceptions))
	pos := 1
	for _, ex := range exceptions {
		// Index of the exception in the block (always <= 127)
		dst[pos] = byte(ex.index)
		pos++
	}
	for _, ex := range exceptions {
		// High bits of the exception value to be ORed
		bo.PutUint32(dst[pos:], ex.high)
		pos += 4
	}
}

// applyExceptions patches the unpacked values by reinserting the high parts
// that were spilled into the exception table. This matches the logic in
// FastPFOR where the truncated packed values are OR'ed with (high << width).
func applyExceptions(dst []uint32, positions []byte, values []byte, bitWidth int) {
	for i, idx := range positions {
		if int(idx) >= len(dst) {
			panic(fmt.Sprintf("fastpfor: exception index %d out of range (max %d)", int(idx), len(dst)-1))
		}
		// OR the high bits of the exception value into the unpacked value at the specified index
		dst[int(idx)] |= bo.Uint32(values[i*4:]) << bitWidth
	}
}

// deltaEncode writes first-order deltas from src into dst (len(dst) == len(src)).
func deltaEncode(dst, src []uint32) bool {
	var prev uint32
	needZigZag := false
	for _, v := range src {
		if int64(v)-int64(prev) < 0 {
			needZigZag = true
		}
		prev = v
	}
	prev = 0
	if needZigZag {
		for i, v := range src {
			diff := int32(int64(v) - int64(prev))
			dst[i] = zigzagEncode32(diff)
			prev = v
		}
		return true
	}
	for i, v := range src {
		dst[i] = v - prev
		prev = v
	}
	return false
}

// deltaDecode reconstructs the prefix sums encoded by deltaEncode.
func deltaDecode(dst, deltas []uint32, useZigZag bool) {
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
