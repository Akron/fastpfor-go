//go:build amd64 && !noasm

package fastpfor

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

const (
	// maxPayloadBytes is the largest lane payload emitted by the SIMD kernels (32-bit width × 16 words).
	maxPayloadBytes = 32 * 16
)

func initSIMDSelection() {
	if cpu.X86.HasSSE2 {
		packLanes = packLanesSIMDPreferred
		unpackLanes = unpackLanesSIMDPreferred
		deltaEncode = deltaEncodeSIMD
		// Auto-select decode strategy based on alignment.
		deltaDecode = deltaDecodeAuto
		deltaDecodeWithOverflow = deltaDecodeWithOverflowSIMD
		simdAvailable = true
		return
	}
}

// Assembly entry points provided by pack_amd64.s/unpack_amd64.s.
//
//go:noescape
func pack32_1(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_2(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_3(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_4(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_5(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_6(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_7(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_8(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_9(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_10(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_11(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_12(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_13(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_14(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_15(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_16(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_17(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_18(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_19(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_20(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_21(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_22(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_23(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_24(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_25(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_26(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_27(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_28(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_29(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_30(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_31(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func pack32_32(in uintptr, out *byte, offset int, seed *byte)

//go:noescape
func unpack32_1(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_2(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_3(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_4(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_5(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_6(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_7(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_8(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_9(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_10(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_11(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_12(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_13(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_14(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_15(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_16(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_17(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_18(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_19(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_20(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_21(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_22(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_23(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_24(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_25(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_26(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_27(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_28(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_29(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_30(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_31(in *byte, out uintptr, offset int, seed *byte)

//go:noescape
func unpack32_32(in *byte, out uintptr, offset int, seed *byte)

var zeroSeed byte

func packLanesSIMDPreferred(dst []byte, values []uint32, bitWidth int) {
	if !simdPack(dst, values, bitWidth) {
		packLanesScalar(dst, values, bitWidth)
	}
}

// simdPack encodes up to 128 uint32 values (zero-filled) into dst using SIMD bit packing.
// dst must have space for bitWidth*16 bytes (same as scalar payload).
// Note: We use a switch instead of a dispatch table to allow the compiler to prove
// that the stack-allocated buffers don't escape (function pointers break escape analysis).
func simdPack(dst []byte, values []uint32, bitWidth int) bool {
	if bitWidth <= 0 || bitWidth > 32 || len(values) > blockSize {
		return false
	}
	needed := bitWidth * 16
	if len(dst) < needed {
		return false
	}

	var valueStorage [blockSize + 4]uint32
	valuesBuf := alignedUint32Slice(&valueStorage)
	// Precompute mask; avoid shift overflow when bitWidth == 32
	var mask uint32 = 0xFFFFFFFF
	if bitWidth < 32 {
		mask = (1 << bitWidth) - 1
	}
	for i, v := range values {
		valuesBuf[i] = v & mask
	}
	var payloadStorage [maxPayloadBytes + 16]byte
	payloadBuf := alignedByteSlice(&payloadStorage)

	inPtr := uintptr(unsafe.Pointer(&valuesBuf[0]))
	outPtr := &payloadBuf[0]

	switch bitWidth {
	case 1:
		pack32_1(inPtr, outPtr, 0, &zeroSeed)
	case 2:
		pack32_2(inPtr, outPtr, 0, &zeroSeed)
	case 3:
		pack32_3(inPtr, outPtr, 0, &zeroSeed)
	case 4:
		pack32_4(inPtr, outPtr, 0, &zeroSeed)
	case 5:
		pack32_5(inPtr, outPtr, 0, &zeroSeed)
	case 6:
		pack32_6(inPtr, outPtr, 0, &zeroSeed)
	case 7:
		pack32_7(inPtr, outPtr, 0, &zeroSeed)
	case 8:
		pack32_8(inPtr, outPtr, 0, &zeroSeed)
	case 9:
		pack32_9(inPtr, outPtr, 0, &zeroSeed)
	case 10:
		pack32_10(inPtr, outPtr, 0, &zeroSeed)
	case 11:
		pack32_11(inPtr, outPtr, 0, &zeroSeed)
	case 12:
		pack32_12(inPtr, outPtr, 0, &zeroSeed)
	case 13:
		pack32_13(inPtr, outPtr, 0, &zeroSeed)
	case 14:
		pack32_14(inPtr, outPtr, 0, &zeroSeed)
	case 15:
		pack32_15(inPtr, outPtr, 0, &zeroSeed)
	case 16:
		pack32_16(inPtr, outPtr, 0, &zeroSeed)
	case 17:
		pack32_17(inPtr, outPtr, 0, &zeroSeed)
	case 18:
		pack32_18(inPtr, outPtr, 0, &zeroSeed)
	case 19:
		pack32_19(inPtr, outPtr, 0, &zeroSeed)
	case 20:
		pack32_20(inPtr, outPtr, 0, &zeroSeed)
	case 21:
		pack32_21(inPtr, outPtr, 0, &zeroSeed)
	case 22:
		pack32_22(inPtr, outPtr, 0, &zeroSeed)
	case 23:
		pack32_23(inPtr, outPtr, 0, &zeroSeed)
	case 24:
		pack32_24(inPtr, outPtr, 0, &zeroSeed)
	case 25:
		pack32_25(inPtr, outPtr, 0, &zeroSeed)
	case 26:
		pack32_26(inPtr, outPtr, 0, &zeroSeed)
	case 27:
		pack32_27(inPtr, outPtr, 0, &zeroSeed)
	case 28:
		pack32_28(inPtr, outPtr, 0, &zeroSeed)
	case 29:
		pack32_29(inPtr, outPtr, 0, &zeroSeed)
	case 30:
		pack32_30(inPtr, outPtr, 0, &zeroSeed)
	case 31:
		pack32_31(inPtr, outPtr, 0, &zeroSeed)
	case 32:
		pack32_32(inPtr, outPtr, 0, &zeroSeed)
	default:
		return false
	}

	copy(dst[:needed], payloadBuf[:needed])
	return true
}

func unpackLanesSIMDPreferred(dst []uint32, payload []byte, count, bitWidth int) {
	if !simdUnpack(dst, payload, bitWidth, count) {
		unpackLanesScalar(dst, payload, count, bitWidth)
	}
}

// simdUnpack decodes a SIMD-packed payload into dst (count <= 128).
// Note: We use a switch instead of a dispatch table to allow the compiler to prove
// that the stack-allocated buffers don't escape (function pointers break escape analysis).
func simdUnpack(dst []uint32, payload []byte, bitWidth, count int) bool {
	if bitWidth <= 0 || bitWidth > 32 || count < 0 || count > blockSize {
		return false
	}
	needed := bitWidth * 16
	if len(payload) < needed || len(dst) < count {
		return false
	}

	var payloadStorage [maxPayloadBytes + 16]byte
	payloadBuf := alignedByteSlice(&payloadStorage)
	copy(payloadBuf[:needed], payload[:needed])
	var valueStorage [blockSize + 4]uint32
	valuesBuf := alignedUint32Slice(&valueStorage)

	inPtr := &payloadBuf[0]
	outPtr := uintptr(unsafe.Pointer(&valuesBuf[0]))

	switch bitWidth {
	case 1:
		unpack32_1(inPtr, outPtr, 0, &zeroSeed)
	case 2:
		unpack32_2(inPtr, outPtr, 0, &zeroSeed)
	case 3:
		unpack32_3(inPtr, outPtr, 0, &zeroSeed)
	case 4:
		unpack32_4(inPtr, outPtr, 0, &zeroSeed)
	case 5:
		unpack32_5(inPtr, outPtr, 0, &zeroSeed)
	case 6:
		unpack32_6(inPtr, outPtr, 0, &zeroSeed)
	case 7:
		unpack32_7(inPtr, outPtr, 0, &zeroSeed)
	case 8:
		unpack32_8(inPtr, outPtr, 0, &zeroSeed)
	case 9:
		unpack32_9(inPtr, outPtr, 0, &zeroSeed)
	case 10:
		unpack32_10(inPtr, outPtr, 0, &zeroSeed)
	case 11:
		unpack32_11(inPtr, outPtr, 0, &zeroSeed)
	case 12:
		unpack32_12(inPtr, outPtr, 0, &zeroSeed)
	case 13:
		unpack32_13(inPtr, outPtr, 0, &zeroSeed)
	case 14:
		unpack32_14(inPtr, outPtr, 0, &zeroSeed)
	case 15:
		unpack32_15(inPtr, outPtr, 0, &zeroSeed)
	case 16:
		unpack32_16(inPtr, outPtr, 0, &zeroSeed)
	case 17:
		unpack32_17(inPtr, outPtr, 0, &zeroSeed)
	case 18:
		unpack32_18(inPtr, outPtr, 0, &zeroSeed)
	case 19:
		unpack32_19(inPtr, outPtr, 0, &zeroSeed)
	case 20:
		unpack32_20(inPtr, outPtr, 0, &zeroSeed)
	case 21:
		unpack32_21(inPtr, outPtr, 0, &zeroSeed)
	case 22:
		unpack32_22(inPtr, outPtr, 0, &zeroSeed)
	case 23:
		unpack32_23(inPtr, outPtr, 0, &zeroSeed)
	case 24:
		unpack32_24(inPtr, outPtr, 0, &zeroSeed)
	case 25:
		unpack32_25(inPtr, outPtr, 0, &zeroSeed)
	case 26:
		unpack32_26(inPtr, outPtr, 0, &zeroSeed)
	case 27:
		unpack32_27(inPtr, outPtr, 0, &zeroSeed)
	case 28:
		unpack32_28(inPtr, outPtr, 0, &zeroSeed)
	case 29:
		unpack32_29(inPtr, outPtr, 0, &zeroSeed)
	case 30:
		unpack32_30(inPtr, outPtr, 0, &zeroSeed)
	case 31:
		unpack32_31(inPtr, outPtr, 0, &zeroSeed)
	case 32:
		unpack32_32(inPtr, outPtr, 0, &zeroSeed)
	default:
		return false
	}

	copy(dst[:count], valuesBuf[:count])
	return true
}

func alignedUint32Slice(storage *[blockSize + 4]uint32) []uint32 {
	base := uintptr(unsafe.Pointer(storage))
	aligned := align16(base)
	const elemSize = int(unsafe.Sizeof(uint32(0))) // size of an element for offset conversion
	offset := int(aligned - base)
	start := offset / elemSize
	return storage[start : start+blockSize]
}

func alignedByteSlice(storage *[maxPayloadBytes + 16]byte) []byte {
	base := uintptr(unsafe.Pointer(storage))
	aligned := align16(base)
	offset := int(aligned - base)
	return storage[offset : offset+maxPayloadBytes]
}

func align16(ptr uintptr) uintptr {
	const mask = 16 - 1 // mask to round up to the next 16-byte boundary
	return (ptr + mask) &^ mask
}

//go:noescape
func deltaEncodeSIMDAsm(dst *uint32, src *uint32, n int) uint32

//go:noescape
func deltaDecodeSIMDAsm(dst *uint32, src *uint32, n int)

//go:noescape
func zigzagEncodeSIMDAsm(buf *uint32, n int)

//go:noescape
func zigzagDecodeSIMDAsm(buf *uint32, n int)

//go:noescape
func deltaDecodeWithOverflowSIMDAsm(dst *uint32, src *uint32, n int) uint8

// deltaEncodeSIMD encodes the deltas of src into dst using SIMD instructions.
// This function uses aligned temporary buffers to satisfy SIMD alignment requirements.
func deltaEncodeSIMD(dst, src []uint32) bool {
	n := len(src)
	if n == 0 {
		return false
	}
	if n > blockSize {
		// Fall back to scalar for oversized input
		return deltaEncodeScalar(dst, src)
	}

	// Use aligned temporary buffers for SIMD operations
	var srcStorage [blockSize + 4]uint32
	srcBuf := alignedUint32Slice(&srcStorage)
	copy(srcBuf[:n], src)

	var dstStorage [blockSize + 4]uint32
	dstBuf := alignedUint32Slice(&dstStorage)

	need := deltaEncodeSIMDAsm(&dstBuf[0], &srcBuf[0], n)
	if need != 0 {
		zigzagEncodeSIMDAsm(&dstBuf[0], n)
		copy(dst[:n], dstBuf[:n])
		return true
	}
	copy(dst[:n], dstBuf[:n])
	return false
}

// deltaDecodeSIMD decodes the deltas of src into dst using SIMD instructions.
// This function uses aligned temporary buffers to satisfy SIMD alignment requirements
// and avoids mutating the input deltas slice.
func deltaDecodeSIMD(dst, deltas []uint32, useZigZag bool) {
	n := len(deltas)
	if n == 0 {
		return
	}
	if n > blockSize {
		// Fall back to scalar for oversized input
		deltaDecodeScalar(dst, deltas, useZigZag)
		return
	}

	srcPtr := &deltas[0]
	dstPtr := &dst[0]
	aligned := func(p *uint32) bool {
		return uintptr(unsafe.Pointer(p))&15 == 0
	}

	// Fast path: if zigzag isn't requested, decode directly when both pointers are 16-byte aligned.
	if !useZigZag && aligned(srcPtr) && aligned(dstPtr) {
		deltaDecodeSIMDAsm(dstPtr, srcPtr, n)
		return
	}

	// When zigzag is requested, prefer in-place if the caller allows mutation and alignment is safe.
	if &dst[0] == &deltas[0] && aligned(dstPtr) {
		zigzagDecodeSIMDAsm(dstPtr, n)
		deltaDecodeSIMDAsm(dstPtr, dstPtr, n)
		return
	}

	// Otherwise keep the previous behavior: work on an aligned temp to avoid mutating input and to satisfy alignment.
	var tmpStorage [blockSize + 4]uint32
	tmp := alignedUint32Slice(&tmpStorage)
	copy(tmp[:n], deltas)
	zigzagDecodeSIMDAsm(&tmp[0], n)
	deltaDecodeSIMDAsm(&tmp[0], &tmp[0], n)
	copy(dst[:n], tmp[:n])
}

// deltaDecodeAuto picks the fastest available strategy based on alignment.
// SIMD is only used when both src and dst are 16-byte aligned; otherwise scalar
// avoids the extra aligned copy overhead that the SIMD path would incur.
func deltaDecodeAuto(dst, deltas []uint32, useZigZag bool) {
	n := len(deltas)
	if n == 0 {
		return
	}
	if n > blockSize || !simdAvailable {
		deltaDecodeScalar(dst, deltas, useZigZag)
		return
	}

	srcPtr := &deltas[0]
	dstPtr := &dst[0]
	aligned := func(p *uint32) bool {
		return uintptr(unsafe.Pointer(p))&15 == 0
	}

	if aligned(srcPtr) && aligned(dstPtr) {
		deltaDecodeSIMD(dst, deltas, useZigZag)
		return
	}

	// Unaligned: scalar is faster than copying to an aligned temp for SIMD.
	deltaDecodeScalar(dst, deltas, useZigZag)
}

// deltaDecodeWithOverflowSIMD decodes deltas using SIMD and detects overflow.
// Returns the 0-based position of the first overflow, or 0 if no overflow occurred.
// Uses SIMD for both decode and overflow detection in a single pass.
func deltaDecodeWithOverflowSIMD(dst, deltas []uint32, useZigZag bool) uint8 {
	n := len(deltas)
	if n == 0 {
		return 0
	}
	if n > blockSize {
		// Fall back to scalar for oversized input
		return deltaDecodeWithOverflowScalar(dst, deltas, useZigZag)
	}

	// For zigzag, overflow detection doesn't apply in the same way (uses int64 internally)
	if useZigZag {
		deltaDecodeSIMD(dst, deltas, useZigZag)
		return 0
	}

	// Use aligned temporary buffers for SIMD operations to keep the asm code
	// working with aligned addresses and to avoid surprises when dst == deltas.
	var srcStorage [blockSize + 4]uint32
	srcBuf := alignedUint32Slice(&srcStorage)
	copy(srcBuf[:n], deltas)

	var dstStorage [blockSize + 4]uint32
	dstBuf := alignedUint32Slice(&dstStorage)

	overflowPos := deltaDecodeWithOverflowSIMDAsm(&dstBuf[0], &srcBuf[0], n)
	copy(dst[:n], dstBuf[:n])
	return overflowPos
}
