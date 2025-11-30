//go:build avogen
// +build avogen

package main

import (
	. "github.com/mmcloughlin/avo/build"
	op "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

// This file generates the SSE2 delta encode/decode kernels.
//
// We can't use the bp128 kernels for delta decoding, as we need to work on the
// patched integers (alternatively we would need to patch n lanes).
//
// The encoder implements straight D1 differential coding (δi = xi − xi−1) so we
// maximize compressibility, while still vectorizing the work with SSE2 as
// suggested by [1]. Other forms such as DM or D4
// would trade compression ratio for fewer data dependencies, but D1 keeps the
// deltas small for integer codecs like FastPFOR. The decoder follows the
// “shift-and-add” SIMD prefix-sum tree: repeated byte shifts (PSLLDQ)
// and packed additions (PADDL)
// compute inclusive scans four integers at a time.
//
// [1] D. Lemire, L. Boytsov, and N. Kurz (2016): "SIMD compression and the intersection of sorted integers",
// Software: Practice and Experience, vol. 46, no. 6, pp. 723–749, 2016, doi: 10.1002/spe.2326.

func genDeltaEncodeKernel() {
	TEXT("deltaEncodeSIMDAsm", NOSPLIT, "func(dst *uint32, src *uint32, n int) uint32")
	Doc("deltaEncodeSIMDAsm encodes a slice of uint32 using delta encoding (D1).")
	Doc("It returns a mask where bits are set if the corresponding delta was negative.")
	Doc("n must be >= 0.")

	// Load parameters
	dstParam := Load(Param("dst"), GP64())
	dstBase := dstParam.(reg.GPVirtual)
	srcParam := Load(Param("src"), GP64())
	srcBase := srcParam.(reg.GPVirtual)
	n := Load(Param("n"), GP64())

	// vecLimit = n & ^3 (multiple of 4)
	vecLimit := GP64()
	MOVQ(n, vecLimit)
	ANDQ(op.Imm(0xfffffffc), vecLimit)

	index := GP64()
	XORQ(index, index)

	prevScalar := GP32()
	XORL(prevScalar, prevScalar)

	prevVec := XMM()
	// SIMD PXOR: zero the running previous-value vector accumulator.
	PXOR(prevVec, prevVec)

	maskAcc := XMM()
	// SIMD PXOR: zero the negative-delta mask accumulator.
	PXOR(maskAcc, maskAcc)

	tailFlag := GP32()
	XORL(tailFlag, tailFlag)

	maskBits := GP32()
	XORL(maskBits, maskBits)

	curr := XMM()
	currCopy := XMM()
	shifted := XMM()
	prevAligned := XMM()
	diff := XMM()
	cmpVec := XMM()

	vecLoop := "delta_encode_vec_loop"
	vecDone := "delta_encode_vec_done"

	// Unrolled loop for 4 vectors (16 integers)
	// This reduces loop overhead and increases instruction level parallelism.
	unrollLoop := "delta_encode_unroll_loop"
	unrollDone := "delta_encode_unroll_done"

	// Calculate limit for unrolled loop: n & ^15
	unrollLimit := GP64()
	MOVQ(n, unrollLimit)
	ANDQ(op.Imm(0xffffffF0), unrollLimit)

	Label(unrollLoop)
	CMPQ(index, unrollLimit)
	JAE(op.LabelRef(unrollDone))

	// Allocate registers for the unrolled block
	var currUnroll, prevUnroll, diffUnroll [4]reg.VecVirtual
	for i := 0; i < 4; i++ {
		currUnroll[i] = XMM()
		prevUnroll[i] = XMM()
		diffUnroll[i] = XMM()
	}

	// Load 4 vectors (16 uint32s)
	// Utilizing multiple load ports if available.
	for i := 0; i < 4; i++ {
		MOVO(op.Mem{Base: srcBase, Index: index, Scale: 4, Disp: i * 16}, currUnroll[i])
	}

	for i := 0; i < 4; i++ {
		// --- Block i ---
		MOVO(currUnroll[i], prevUnroll[i])
		PSLLDQ(op.Imm(4), prevUnroll[i]) // Shift left by 1 element

		if i == 0 {
			POR(prevVec, prevUnroll[i]) // Insert the carry from previous iteration
		} else {
			// Extract carry from currUnroll[i-1]: last element becomes first of prevUnroll[i]
			carry := XMM()
			MOVO(currUnroll[i-1], carry)
			PSRLDQ(op.Imm(12), carry) // [d, 0, 0, 0]
			POR(carry, prevUnroll[i])
		}

		MOVO(currUnroll[i], diffUnroll[i])
		PSUBL(prevUnroll[i], diffUnroll[i]) // diff = curr - prev
		MOVO(diffUnroll[i], op.Mem{Base: dstBase, Index: index, Scale: 4, Disp: i * 16})

		// Accumulate sign bits for negative delta detection.
		// We want to check if diff < 0, which corresponds to curr < prev.
		// PCMPGTL(src, dest) compares dest > src.
		// So PCMPGTL(currUnroll[i], prevUnroll[i]) sets prevUnroll[i] to all ones if prevUnroll[i] > currUnroll[i], which is exactly when the delta is negative.
		PCMPGTL(currUnroll[i], prevUnroll[i])
		POR(prevUnroll[i], maskAcc)
	}

	// Update prevVec for next iteration (carry from currUnroll[3])
	// prevVec needs to be [last_element_of_currUnroll[3], 0, 0, 0]
	MOVO(currUnroll[3], prevVec)
	PSRLDQ(op.Imm(12), prevVec)

	// Update prevScalar for fallback
	MOVD(prevVec, prevScalar)

	ADDQ(op.Imm(16), index)
	JMP(op.LabelRef(unrollLoop))

	Label(unrollDone)

	// Vector loop for remaining multiples of 4
	Label(vecLoop)
	CMPQ(index, vecLimit)
	JAE(op.LabelRef(vecDone))

	blockSrc := op.Mem{Base: srcBase, Index: index, Scale: 4}
	blockDst := op.Mem{Base: dstBase, Index: index, Scale: 4}

	MOVO(blockSrc, curr)
	MOVO(curr, currCopy)

	// Shift values left by one lane (D1 alignment)
	MOVO(currCopy, shifted)
	PSLLDQ(op.Imm(4), shifted)

	// Initialize prevAligned with the shifted values
	MOVO(shifted, prevAligned)
	// Insert the carried-over last element from the previous block
	POR(prevVec, prevAligned)

	// Compute vector differences (current - previous)
	MOVO(currCopy, diff)
	PSUBL(prevAligned, diff)
	MOVO(diff, blockDst)

	// Detect negative differences (prev > curr)
	MOVO(prevAligned, cmpVec)
	PCMPGTL(currCopy, cmpVec) // cmpVec = cmpVec > currCopy
	POR(cmpVec, maskAcc)

	// Prepare prevVec for next iteration
	// Reset prevVec before capturing the last element (D1 seed).
	// Note: MOVD/MOVQ from memory to XMM zeroes the upper bits, so PXOR is redundant.
	// Stash the most recent scalar so the next block sees xi−1.
	// We need the last element of the current block.
	lastSrc := op.Mem{Base: srcBase, Index: index, Scale: 4, Disp: 12}
	MOVD(lastSrc, prevVec)
	// Scalar copy keeps the fallback loop in sync with SIMD progress.
	MOVL(lastSrc, prevScalar)

	ADDQ(op.Imm(4), index)
	JMP(op.LabelRef(vecLoop))

	Label(vecDone)
	// Collapse accumulated sign bits to a scalar mask
	MOVMSKPS(maskAcc, maskBits)

	// Process remaining elements (0-3)
	tailLoop := "delta_encode_tail_loop"
	tailDone := "delta_encode_tail_done"
	tailSkip := "delta_encode_tail_skip"

	tailSrcVal := GP32()
	tailDiff := GP32()

	Label(tailLoop)
	CMPQ(index, n)
	JAE(op.LabelRef(tailDone))

	elemSrc := op.Mem{Base: srcBase, Index: index, Scale: 4}
	elemDst := op.Mem{Base: dstBase, Index: index, Scale: 4}

	MOVL(elemSrc, tailSrcVal)
	MOVL(tailSrcVal, tailDiff)
	SUBL(prevScalar, tailDiff)
	MOVL(tailDiff, elemDst)

	CMPL(tailSrcVal, prevScalar)
	JAE(op.LabelRef(tailSkip))
	INCL(tailFlag)
	Label(tailSkip)

	MOVL(tailSrcVal, prevScalar)
	ADDQ(op.Imm(1), index)
	JMP(op.LabelRef(tailLoop))

	Label(tailDone)
	ORL(tailFlag, maskBits)
	Store(maskBits.As32(), ReturnIndex(0))
	RET()
}

func genDeltaDecodeKernel() {
	TEXT("deltaDecodeSIMDAsm", NOSPLIT, "func(dst *uint32, src *uint32, n int)")
	Doc("deltaDecodeSIMDAsm decodes a slice of uint32 using delta decoding (prefix sum).")

	dstParam := Load(Param("dst"), GP64())
	dstBase := dstParam.(reg.GPVirtual)
	srcParam := Load(Param("src"), GP64())
	srcBase := srcParam.(reg.GPVirtual)
	n := Load(Param("n"), GP64())

	vecLimit := GP64()
	MOVQ(n, vecLimit)
	ANDQ(op.Imm(0xfffffffc), vecLimit)

	index := GP64()
	XORQ(index, index)

	prevVec := XMM()
	PXOR(prevVec, prevVec) // Running prefix accumulator

	prevScalar := GP32()
	XORL(prevScalar, prevScalar)

	valVec := XMM()
	scanVec := XMM()
	tmpVec := XMM()

	vecLoop := "delta_decode_vec_loop"
	vecDone := "delta_decode_vec_done"

	// Unrolled loop
	unrollLoop := "delta_decode_unroll_loop"
	unrollDone := "delta_decode_unroll_done"

	unrollLimit := GP64()
	MOVQ(n, unrollLimit)
	ANDQ(op.Imm(0xffffffF0), unrollLimit)

	Label(unrollLoop)
	CMPQ(index, unrollLimit)
	JAE(op.LabelRef(unrollDone))

	var v, t [4]reg.VecVirtual
	for i := 0; i < 4; i++ {
		v[i] = XMM()
		t[i] = XMM()
	}

	// Load 4 blocks
	for i := 0; i < 4; i++ {
		MOVO(op.Mem{Base: srcBase, Index: index, Scale: 4, Disp: i * 16}, v[i])
	}

	// Parallel prefix sum within each vector (Kogge-Stone like)
	// Stage 1: Shift left 1 lane (4 bytes) and add
	for i := 0; i < 4; i++ {
		MOVO(v[i], t[i])
		PSLLDQ(op.Imm(4), t[i])
		PADDL(t[i], v[i])
	}

	// Stage 2: Shift left 2 lanes (8 bytes) and add
	for i := 0; i < 4; i++ {
		MOVO(v[i], t[i])
		PSLLDQ(op.Imm(8), t[i])
		PADDL(t[i], v[i])
	}

	// Now v[0]..v[3] have local prefix sums.
	// Accumulate global sums sequentially.
	// prevVec contains the sum from the previous iteration (broadcasted to all lanes).

	for i := 0; i < 4; i++ {
		// Block i
		PADDL(prevVec, v[i])
		MOVO(v[i], op.Mem{Base: dstBase, Index: index, Scale: 4, Disp: i * 16})
		// Broadcast last element of v[i] to be the start for v[i+1]
		MOVO(v[i], prevVec)
		PSHUFL(op.Imm(0xFF), prevVec, prevVec)
	}

	// Update prevScalar for fallback
	MOVD(prevVec, prevScalar)

	ADDQ(op.Imm(16), index)
	JMP(op.LabelRef(unrollLoop))

	Label(unrollDone)

	Label(vecLoop)
	CMPQ(index, vecLimit)
	JAE(op.LabelRef(vecDone))

	blockSrc := op.Mem{Base: srcBase, Index: index, Scale: 4}
	blockDst := op.Mem{Base: dstBase, Index: index, Scale: 4}

	MOVO(blockSrc, valVec)
	MOVO(valVec, scanVec)

	// Stage #1 — shift by one delta.
	MOVO(scanVec, tmpVec)
	PSLLDQ(op.Imm(4), tmpVec)
	PADDL(tmpVec, scanVec)

	// Stage #2 — shift by two deltas.
	MOVO(scanVec, tmpVec)
	PSLLDQ(op.Imm(8), tmpVec)
	PADDL(tmpVec, scanVec)

	// Add the carried scalar accumulator.
	PADDL(prevVec, scanVec)
	MOVO(scanVec, blockDst)

	// Extract the last lane to seed the next iteration.
	MOVO(scanVec, prevVec)
	PSHUFL(op.Imm(0xFF), prevVec, prevVec)
	MOVL(op.Mem{Base: dstBase, Index: index, Scale: 4, Disp: 12}, prevScalar)

	ADDQ(op.Imm(4), index)
	JMP(op.LabelRef(vecLoop))

	Label(vecDone)

	tailLoop := "delta_decode_tail_loop"
	tailDone := "delta_decode_tail_done"

	tailDelta := GP32()

	Label(tailLoop)
	CMPQ(index, n)
	JAE(op.LabelRef(tailDone))

	elemSrc := op.Mem{Base: srcBase, Index: index, Scale: 4}
	elemDst := op.Mem{Base: dstBase, Index: index, Scale: 4}

	MOVL(elemSrc, tailDelta)
	ADDL(tailDelta, prevScalar)
	MOVL(prevScalar, elemDst)

	ADDQ(op.Imm(1), index)
	JMP(op.LabelRef(tailLoop))

	Label(tailDone)
	RET()
}
