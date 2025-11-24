// Package fastpfor implements a FastPFOR integer compression codec.
//
// The codec operates on fixed blocks of up to 128 unsigned 32-bit integers.
// Each block begins with a 32-bit header describing the bit width of the packed
// lane sets followed by the interleaved payload (4 SIMD-friendly lanes) and a
// patch area for exception values. Callers provide the destination slices to
// Pack/Unpack so higher-level codecs can reuse buffers without repeated heap
// allocations. The package maintains no global mutable state, so Pack, Unpack
// and their delta variants are safe for concurrent use as long as each
// goroutine owns the dst/scratch slices it passes in.
//
// References:
//   - https://ayende.com/blog/199523-C/integer-compression-understanding-fastpfor
//   - https://ayende.com/blog/199524-C/integer-compression-the-fastpfor-code
package fastpfor

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// Block configuration constants. Pack/Unpack always operates on at most 128
// integers, interleaved into 4 pseudo lanes to match the SIMD-PFOR layout.
const (
	BlockSize  = 128
	laneCount  = 4
	laneLength = BlockSize / laneCount

	// headerBytes is the number of bytes reserved for the block header. The
	// serialized 32-bit header stores
	//   (a) the logical element count,
	//   (b) the per-lane bit width used for packing,
	//   (c) flag bits that describe optional sections (exceptions, delta markers, etc).
	headerBytes = 4
)

const (
	headerCountBits = 8
	headerWidthBits = 6

	headerCountMask  = (1 << headerCountBits) - 1
	headerWidthMask  = (1 << headerWidthBits) - 1
	headerWidthShift = headerCountBits

	headerExceptionFlag = uint32(1 << 31)
)

// Pack encodes up to BlockSize uint32 values into the FastPFOR block format.
// The function appends the block to dst so the caller can reuse buffers and
// avoid per-block allocations. Callers must not reuse the same dst slice across
// concurrent Pack invocations unless they coordinate access themselves.
// Each block writes:
//   - Header (count, bit width, exception flag)
//   - Interleaved lane payload packed at the chosen width
//   - Optional exception table (count byte, positions, high bits)
func Pack(dst []byte, values []uint32) []byte {
	validateBlockLength(len(values))
	bitWidth, exceptions := selectBitWidth(values)
	payloadLen := payloadBytes(bitWidth)
	total := headerBytes + payloadLen + patchBytes(len(exceptions))

	var start int
	dst, start = appendSpace(dst, total)
	flags := uint32(0)
	if len(exceptions) > 0 {
		flags = headerExceptionFlag
	}
	header := encodeHeader(len(values), bitWidth, flags)
	binary.LittleEndian.PutUint32(dst[start:start+headerBytes], header)

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
		panic("fastpfor: buffer too small for header")
	}
	header := binary.LittleEndian.Uint32(buf[:headerBytes])
	count, bitWidth, hasExceptions := decodeHeader(header)
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
		for i := range count {
			dst[i] = 0
		}
	} else {
		unpackLanes(dst[:count], buf[headerBytes:minNeeded], count, bitWidth)
	}

	if hasExceptions {
		if len(buf) < minNeeded+1 {
			panic("fastpfor: missing exception count")
		}
		patch := buf[minNeeded:]
		excCount := int(patch[0])
		patch = patch[1:]
		if len(patch) < excCount {
			panic("fastpfor: truncated exception positions")
		}
		positions := patch[:excCount]
		patch = patch[excCount:]
		valueBytes := excCount * 4
		if len(patch) < valueBytes {
			panic("fastpfor: truncated exception values")
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
	if len(values) > 0 {
		deltaEncode(scratch[:len(values)], values)
	}
	return Pack(dst, scratch[:len(values)])
}

// UnpackDelta reverses PackDelta by unpacking into the provided scratch space
// first and then performing a prefix sum to reconstruct the original values.
func UnpackDelta(dst []uint32, buf []byte, scratch []uint32) []uint32 {
	deltas := Unpack(scratch[:0], buf)
	if len(deltas) == 0 {
		return dst[:0]
	}
	dst = ensureUint32Len(dst, len(deltas))
	deltaDecode(dst[:len(deltas)], deltas)
	return dst[:len(deltas)]
}

// validateBlockLength panics if the caller tries to encode more than BlockSize
// integers. FastPFOR always operates on fixed 128-value chunks.
func validateBlockLength(n int) {
	if n < 0 {
		panic("fastpfor: negative block length")
	}
	if n > BlockSize {
		panic(fmt.Sprintf("fastpfor: block length %d > %d", n, BlockSize))
	}
}

// appendSpace grows dst by extra bytes and returns the resized slice plus the
// index of the first newly allocated byte. The function avoids allocating when
// the existing capacity is sufficient.
func appendSpace(dst []byte, extra int) ([]byte, int) {
	start := len(dst)
	need := start + extra
	if cap(dst) < need {
		newDst := make([]byte, need)
		copy(newDst, dst)
		dst = newDst
	} else {
		dst = dst[:need]
	}
	return dst, start
}

func ensureUint32Len(dst []uint32, n int) []uint32 {
	if cap(dst) >= n {
		return dst[:n]
	}
	return make([]uint32, n)
}

// requiredBitWidth returns the minimum number of bits needed to encode every
// value in the slice without exceptions.
func requiredBitWidth(values []uint32) int {
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

func encodeHeader(count, bitWidth int, flags uint32) uint32 {
	return uint32(count&headerCountMask) |
		(uint32(bitWidth&headerWidthMask) << headerWidthShift) |
		flags
}

func decodeHeader(header uint32) (count int, bitWidth int, hasExceptions bool) {
	count = int(header & headerCountMask)
	bitWidth = int((header >> headerWidthShift) & headerWidthMask)
	hasExceptions = header&headerExceptionFlag != 0
	return
}

// packLanes splits the block into four SIMD-friendly lanes and bit-packs each
// lane independently. Missing tail values (len < 128) are treated as zeros.
func packLanes(dst []byte, values []uint32, bitWidth int) {
	// Reference (FastPFor.cpp):
	//
	//	for(uint32_t k = 0; k < 4; ++k)
	//	  fastpackwithoutmask(in+4*i+k, out + k*bits, bits);
	if bitWidth == 0 {
		return
	}
	bytesPerLane := len(dst) / laneCount
	for lane := range laneCount {
		packLane(dst[lane*bytesPerLane:(lane+1)*bytesPerLane], values, lane, bitWidth)
	}
}

// packLane packs 32 integers taken from the specified lane (lane, lane+4, …)
// into the destination buffer using a streaming 64-bit accumulator.
func packLane(buf []byte, values []uint32, lane, bitWidth int) {
	// Rough C++ equivalent (FastPFor.cpp::fastpackwithoutmask):
	//
	//	for(uint32_t i = 0; i < 32; ++i) {
	//	  const uint64_t value = input[i] & mask;
	//	  buffer |= value << bitOffset;
	//	  if(bitOffset >= 32) { *out++ = uint32_t(buffer); buffer >>= 32; bitOffset -= 32; }
	//	  bitOffset += bitWidth;
	//	}
	if bitWidth == 0 {
		return
	}
	var mask uint64
	if bitWidth >= 32 {
		mask = uint64(mathMaxUint32)
	} else {
		mask = uint64((1 << bitWidth) - 1)
	}
	var acc uint64
	var bitsInAcc int
	out := buf
	outIdx := 0

	for i := range laneLength {
		idx := lane + i*laneCount
		var v uint32
		if idx < len(values) {
			v = values[idx]
		}
		acc |= (uint64(v) & mask) << bitsInAcc
		bitsInAcc += bitWidth
		for bitsInAcc >= 32 {
			binary.LittleEndian.PutUint32(out[outIdx:], uint32(acc))
			outIdx += 4
			acc >>= 32
			bitsInAcc -= 32
		}
	}
	if bitsInAcc > 0 {
		binary.LittleEndian.PutUint32(out[outIdx:], uint32(acc))
	}
}

// unpackLanes performs the inverse of packLanes, up to the logical element
// count (tail values outside count retain their previous contents).
func unpackLanes(dst []uint32, payload []byte, count, bitWidth int) {
	if bitWidth == 0 {
		for i := range count {
			dst[i] = 0
		}
		return
	}
	bytesPerLane := len(payload) / laneCount
	for lane := range laneCount {
		unpackLane(dst, payload[lane*bytesPerLane:(lane+1)*bytesPerLane], lane, bitWidth, count)
	}
}

// unpackLane reconstructs the original integers for a single lane and writes
// them back into dst at the interleaved lane offsets. Mirrors packLane but in
// reverse order (a literal translation of FastPFor.cpp::fastunpack)
func unpackLane(dst []uint32, buf []byte, lane, bitWidth, count int) {
	//	for(uint32_t i = 0; i < 32; ++i) {
	//	  while(bitOffset < bitWidth) { buffer |= (uint64_t)(*in++) << bitOffset; bitOffset += 32; }
	//	  output[i] = uint32_t(buffer) & mask;
	//	  buffer >>= bitWidth;
	//	  bitOffset -= bitWidth;
	//	}
	if bitWidth == 0 {
		return
	}
	var mask uint32
	if bitWidth >= 32 {
		mask = mathMaxUint32
	} else {
		mask = (1 << bitWidth) - 1
	}
	var acc uint64
	var bitsInAcc int
	inIdx := 0
	for i := range laneLength {
		for bitsInAcc < bitWidth {
			if inIdx >= len(buf) {
				acc |= uint64(0) << bitsInAcc
				bitsInAcc = bitWidth // force exit
				break
			}
			acc |= uint64(binary.LittleEndian.Uint32(buf[inIdx:])) << bitsInAcc
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

// mathMaxUint32 helps avoid repeated casts when constructing bit masks.
const mathMaxUint32 = ^uint32(0)

// exception tracks a single patched integer: its index in the block and the
// high bits that must be re-applied after unpacking the truncated value.
type exception struct {
	index uint8
	high  uint32
}

// selectBitWidth picks the bit width that minimizes the serialized size. It
// mirrors FastPFOR's getBestBFromData routine.
// We follow the same logic: iterate every candidate width, collect exceptions,
// compute header+payload+patch bytes, and prefer smaller widths on ties to keep
// SIMD packing efficient.
func selectBitWidth(values []uint32) (width int, exceptions []exception) {
	//
	//	for(int b = 0; b <= maxb; ++b) {
	//	  c = countOccurenceOfMostSignificantBit(b);
	//	  bitsRequired = 4 + 4*lanes*((b*32 + 31)/32);
	//	  bitsRequired += 8*c;
	//	  pick smallest bitsRequired, break ties with smaller b.
	//	}
	//
	maxWidth := requiredBitWidth(values)
	bestWidth := maxWidth
	bestSize := headerBytes + payloadBytes(maxWidth)
	var bestExc []exception
	var scratch [BlockSize]exception

	for candidate := 0; candidate <= maxWidth; candidate++ {
		exc := collectExceptions(values, candidate, scratch[:0])
		size := headerBytes + payloadBytes(candidate) + patchBytes(len(exc))
		if size < bestSize || (size == bestSize && candidate < bestWidth) {
			bestSize = size
			bestWidth = candidate
			bestExc = append(bestExc[:0], exc...)
		}
	}
	return bestWidth, bestExc
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
	dst[0] = byte(len(exceptions))
	pos := 1
	for _, ex := range exceptions {
		dst[pos] = byte(ex.index)
		pos++
	}
	for _, ex := range exceptions {
		binary.LittleEndian.PutUint32(dst[pos:], ex.high)
		pos += 4
	}
}

// applyExceptions patches the unpacked values by reinserting the high parts
// that were spilled into the exception table. This matches the logic in
// FastPFOR where the truncated packed values are OR'ed with (high << width).
func applyExceptions(dst []uint32, positions []byte, values []byte, bitWidth int) {
	for i, idx := range positions {
		if int(idx) >= len(dst) {
			panic("fastpfor: exception index out of range")
		}
		high := binary.LittleEndian.Uint32(values[i*4:])
		dst[int(idx)] |= high << bitWidth
	}
}

// deltaEncode writes first-order deltas from src into dst (len(dst) == len(src)).
func deltaEncode(dst, src []uint32) {
	var prev uint32
	for i, v := range src {
		dst[i] = v - prev
		prev = v
	}
}

// deltaDecode reconstructs the prefix sums encoded by deltaEncode.
func deltaDecode(dst, deltas []uint32) {
	var prev uint32
	for i, d := range deltas {
		prev += d
		dst[i] = prev
	}
}
