//go:build avogen
// +build avogen

package main

import (
	. "github.com/mmcloughlin/avo/build"
	op "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

// This file generates the SSE2 zigzag encode/decode kernels.
// ZigZag encoding maps signed integers to unsigned integers so that numbers with small absolute values
// (both positive and negative) are mapped to small unsigned integers.
//
// It follows the equivalent of the following C code from
// https://lemire.me/blog/2022/11/25/making-all-your-integers-positive-with-zigzag-encoding/
// int fast_decode(unsigned int x) {
//   return (x >> 1) ^ (-(x&1));
// }
//
// unsigned int fast_encode(int x) {
//   return (2*x) ^ (x >>(sizeof(int) * 8 - 1));
// }

func genZigZagEncodeKernel() {
	TEXT("zigzagEncodeSIMDAsm", NOSPLIT, "func(buf *uint32, n int)")
	Doc("zigzagEncodeSIMDAsm encodes a slice of int32 (cast to uint32) using ZigZag encoding.")
	Doc("It performs the operation in-place.")

	bufParam := Load(Param("buf"), GP64())
	bufPtr := bufParam.(reg.GPVirtual)
	n := Load(Param("n"), GP64())

	vecCount := GP64()
	MOVQ(n, vecCount)
	ANDQ(op.Imm(0xfffffffc), vecCount)

	tailCount := GP64()
	MOVQ(n, tailCount)
	ANDQ(op.Imm(3), tailCount)

	vecRemaining := GP64()
	MOVQ(vecCount, vecRemaining)

	// Unrolled loop for processing 4 vectors (16 integers) at a time.
	unrollLoop := "zigzag_encode_unroll_loop"
	unrollDone := "zigzag_encode_unroll_done"

	Label(unrollLoop)
	CMPQ(vecRemaining, op.Imm(16))
	JL(op.LabelRef(unrollDone))

	// Allocate registers for 4 blocks
	var v, s [4]reg.VecVirtual
	for i := 0; i < 4; i++ {
		v[i] = XMM()
		s[i] = XMM()
	}

	// Load 4 vectors
	for i := 0; i < 4; i++ {
		MOVO(op.Mem{Base: bufPtr, Disp: i * 16}, v[i])
	}

	// Formula: (n << 1) ^ (n >> 31)
	// s = n >> 31 (Arithmetic shift preserves sign)
	for i := 0; i < 4; i++ {
		MOVO(v[i], s[i])
		PSRAL(op.Imm(31), s[i])
	}

	// v = n << 1
	for i := 0; i < 4; i++ {
		PSLLL(op.Imm(1), v[i])
	}

	// v = v ^ s
	for i := 0; i < 4; i++ {
		PXOR(s[i], v[i])
	}

	// Store back
	for i := 0; i < 4; i++ {
		MOVO(v[i], op.Mem{Base: bufPtr, Disp: i * 16})
	}

	ADDQ(op.Imm(64), bufPtr)
	SUBQ(op.Imm(16), vecRemaining)
	JMP(op.LabelRef(unrollLoop))

	Label(unrollDone)

	// Vector loop (for remaining blocks of 4)
	vecLoop := "zigzag_encode_vec_loop"
	vecDone := "zigzag_encode_vec_done"

	valVec := XMM()
	signVec := XMM()
	shiftVec := XMM()

	Label(vecLoop)
	CMPQ(vecRemaining, op.Imm(0))
	JE(op.LabelRef(vecDone))

	MOVO(op.Mem{Base: bufPtr}, valVec)

	// signVec = valVec >> 31
	MOVO(valVec, signVec)
	PSRAL(op.Imm(31), signVec)

	// shiftVec = valVec << 1
	MOVO(valVec, shiftVec)
	PSLLL(op.Imm(1), shiftVec)

	// result = shiftVec ^ signVec
	PXOR(signVec, shiftVec)

	MOVO(shiftVec, op.Mem{Base: bufPtr})

	ADDQ(op.Imm(16), bufPtr)
	SUBQ(op.Imm(4), vecRemaining)
	JMP(op.LabelRef(vecLoop))

	Label(vecDone)

	// Tail loop for remaining elements (0-3)
	tailLoop := "zigzag_encode_tail_loop"
	tailDone := "zigzag_encode_tail_done"

	tailVal := GP32()
	tailSign := GP32()

	Label(tailLoop)
	CMPQ(tailCount, op.Imm(0))
	JE(op.LabelRef(tailDone))

	MOVL(op.Mem{Base: bufPtr}, tailVal)
	MOVL(tailVal, tailSign)
	SARL(op.Imm(31), tailSign) // Arithmetic shift for sign
	SHLL(op.Imm(1), tailVal)
	XORL(tailSign, tailVal)
	MOVL(tailVal, op.Mem{Base: bufPtr})

	ADDQ(op.Imm(4), bufPtr)
	DECQ(tailCount)
	JMP(op.LabelRef(tailLoop))

	Label(tailDone)
	RET()
}

func genZigZagDecodeKernel() {
	TEXT("zigzagDecodeSIMDAsm", NOSPLIT, "func(buf *uint32, n int)")
	Doc("zigzagDecodeSIMDAsm decodes a slice of ZigZag-encoded integers in-place.")

	bufParam := Load(Param("buf"), GP64())
	bufPtr := bufParam.(reg.GPVirtual)
	n := Load(Param("n"), GP64())

	vecCount := GP64()
	MOVQ(n, vecCount)
	ANDQ(op.Imm(0xfffffffc), vecCount)

	tailCount := GP64()
	MOVQ(n, tailCount)
	ANDQ(op.Imm(3), tailCount)

	vecRemaining := GP64()
	MOVQ(vecCount, vecRemaining)

	// Prepare constant mask for LSB isolation - Optimized out
	// ones := XMM()
	// PXOR(ones, ones)
	// PCMPEQL(ones, ones)     // Set all bits to 1
	// PSRLL(op.Imm(31), ones) // Shift right logical to get 0x00000001 in each lane

	// Unrolled loop for processing 4 vectors (16 integers) at a time.
	unrollLoop := "zigzag_decode_unroll_loop"
	unrollDone := "zigzag_decode_unroll_done"

	Label(unrollLoop)
	CMPQ(vecRemaining, op.Imm(16))
	JL(op.LabelRef(unrollDone))

	var v, l [4]reg.VecVirtual
	for i := 0; i < 4; i++ {
		v[i] = XMM()
		l[i] = XMM()
	}

	for i := 0; i < 4; i++ {
		MOVO(op.Mem{Base: bufPtr, Disp: i * 16}, v[i])
	}

	// Formula: (n >>> 1) ^ -(n & 1)
	// -(n & 1) is equivalent to (n << 31) >> 31 (arithmetic shift),
	// which broadcasts the LSB to all bits.

	for i := 0; i < 4; i++ {
		// Block i
		MOVO(v[i], l[i])
		PSLLL(op.Imm(31), l[i])
		PSRAL(op.Imm(31), l[i])
	}

	// v = n >>> 1
	for i := 0; i < 4; i++ {
		PSRLL(op.Imm(1), v[i])
	}

	// v = v ^ l
	for i := 0; i < 4; i++ {
		PXOR(l[i], v[i])
	}

	for i := 0; i < 4; i++ {
		MOVO(v[i], op.Mem{Base: bufPtr, Disp: i * 16})
	}

	ADDQ(op.Imm(64), bufPtr)
	SUBQ(op.Imm(16), vecRemaining)
	JMP(op.LabelRef(unrollLoop))

	Label(unrollDone)

	valVec := XMM()
	lsbVec := XMM()
	shiftVec := XMM()

	vecLoop := "zigzag_decode_vec_loop"
	vecDone := "zigzag_decode_vec_done"

	Label(vecLoop)
	CMPQ(vecRemaining, op.Imm(0))
	JE(op.LabelRef(vecDone))

	MOVO(op.Mem{Base: bufPtr}, valVec)

	// lsbVec = -(valVec & 1) -> (valVec << 31) >> 31
	MOVO(valVec, lsbVec)
	PSLLL(op.Imm(31), lsbVec)
	PSRAL(op.Imm(31), lsbVec)

	// shiftVec = valVec >>> 1
	MOVO(valVec, shiftVec)
	PSRLL(op.Imm(1), shiftVec)

	// result = shiftVec ^ lsbVec
	PXOR(lsbVec, shiftVec)

	MOVO(shiftVec, op.Mem{Base: bufPtr})

	ADDQ(op.Imm(16), bufPtr)
	SUBQ(op.Imm(4), vecRemaining)
	JMP(op.LabelRef(vecLoop))

	Label(vecDone)

	tailLoop := "zigzag_decode_tail_loop"
	tailDone := "zigzag_decode_tail_done"

	tailVal := GP32()
	tailShift := GP32()
	tailMask := GP32()

	Label(tailLoop)
	CMPQ(tailCount, op.Imm(0))
	JE(op.LabelRef(tailDone))

	MOVL(op.Mem{Base: bufPtr}, tailVal)
	MOVL(tailVal, tailMask)
	ANDL(op.Imm(1), tailMask)
	NEGL(tailMask) // tailMask = -(n & 1)

	MOVL(tailVal, tailShift)
	SHRL(op.Imm(1), tailShift) // tailShift = n >>> 1
	XORL(tailMask, tailShift)
	MOVL(tailShift, op.Mem{Base: bufPtr})

	ADDQ(op.Imm(4), bufPtr)
	DECQ(tailCount)
	JMP(op.LabelRef(tailLoop))

	Label(tailDone)
	RET()
}
