package fastpfor

// PackUint16 encodes up to BlockSize uint16 values into the FastPFOR block format.
// The encoding uses the same bit-packing as PackUint32 but marks the header with
// IntTypeUint16 for future native uint16 support. Since bit-width selection is
// value-based, compression is optimal for the actual values present.
//
// For zero-allocation operation when data contains exceptions, provide a values
// slice with cap >= 256. The extra capacity (positions 128-255) is used as scratch
// space for exception handling.
//
// This currently does not natively pack Uint16 - and is just a wrapper.
func PackUint16(dst []byte, values []uint16) []byte {
	var buf [2 * blockSize]uint32 // scratch space for conversion + exceptions
	for i, v := range values {
		buf[i] = uint32(v)
	}
	return packInternal(dst, buf[:len(values)], headerTypeUint16Flag)
}

// PackDeltaUint16 delta-encodes and packs uint16 values.
// Unlike PackDeltaUint32, this does NOT mutate the input slice since the values
// are copied to an internal buffer for conversion to uint32.
//
// The delta flag is set in the header so UnpackUint32 can auto-detect and decode.
// The IntTypeUint16 marker indicates the original values were uint16.
//
// For zero-allocation operation when data contains exceptions, provide a values
// slice with cap >= 256. The extra capacity (positions 128-255) is used as scratch
// space for exception handling.
//
// This currently does not natively pack Uint16 - and is just a wrapper.
func PackDeltaUint16(dst []byte, values []uint16) []byte {
	var buf [2 * blockSize]uint32 // scratch space for conversion + exceptions
	for i, v := range values {
		buf[i] = uint32(v)
	}

	n := len(values)
	if n == 0 {
		return packInternal(dst, buf[:0], headerTypeUint16Flag|headerDeltaFlag)
	}

	useZigZag := deltaEncode(buf[:n], buf[:n]) // in-place delta encoding
	flags := headerTypeUint16Flag | headerDeltaFlag
	if useZigZag {
		flags |= headerZigZagFlag
	}
	return packInternal(dst, buf[:n], flags)
}

// MaxBlockSizeUint16 returns the maximum encoded size for a block of uint16 values.
// This is identical to MaxBlockSizeUint32 since the wire format is the same;
// uint16 values are packed using the same SIMD-friendly lane format.
func MaxBlockSizeUint16() int {
	return MaxBlockSizeUint32()
}
