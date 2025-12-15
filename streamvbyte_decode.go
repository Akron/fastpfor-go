// Non-assembly StreamVByte decoding utilities.
//
// These functions support random access decoding of individual values from
// StreamVByte-encoded data without decoding the entire stream.

package fastpfor

// svbControlBlockSizeLUT is a precomputed lookup table for StreamVByte control byte sizes.
// Each control byte encodes lengths for 4 values (2 bits each, code+1 = byte length).
// Entry i = sum of byte lengths for all 4 values encoded in control byte i.
var svbControlBlockSizeLUT [256]uint8

func init() {
	for ctrl := range 256 {
		// Sum of (code+1) for all 4 values
		size := (ctrl & 0x03) + ((ctrl >> 2) & 0x03) + ((ctrl >> 4) & 0x03) + (ctrl >> 6) + 4
		svbControlBlockSizeLUT[ctrl] = uint8(size)
	}
}

// svbControlBlockSize returns the total data bytes for a StreamVByte control byte.
func svbControlBlockSize(ctrl byte) int {
	return int(svbControlBlockSizeLUT[ctrl])
}

// svbDecodeOne decodes a single value from StreamVByte data at the given index.
// The svbData should start at the StreamVByte payload (after the 2-byte length prefix).
// count is the total number of encoded values.
// This function is allocation-free and suitable for random access patterns.
func svbDecodeOne(svbData []byte, count, index int) uint32 {
	// StreamVByte format: control bytes first, then data bytes
	// Control bytes: one per 4 values, each 2-bit code = byteLength-1
	numControlBytes := (count + 3) >> 2
	controlBytes := svbData[:numControlBytes]
	dataBytes := svbData[numControlBytes:]

	// Find the block containing our value
	blockIndex := index >> 2   // index / 4
	posInBlock := index & 0x03 // index % 4

	// Sum data sizes for all blocks before ours
	dataOffset := 0
	for i := range blockIndex {
		dataOffset += svbControlBlockSize(controlBytes[i])
	}

	// Decode the value at posInBlock within this block
	ctrl := controlBytes[blockIndex]
	var value uint32
	for i := 0; i <= posInBlock; i++ {
		code := (ctrl >> (i * 2)) & 0x03
		byteLen := int(code) + 1
		if i == posInBlock {
			value = svbReadValue(dataBytes[dataOffset:], byteLen)
		}
		dataOffset += byteLen
	}

	return value
}

// svbReadValue reads a variable-length encoded value (1-4 bytes).
func svbReadValue(data []byte, byteLen int) uint32 {
	switch byteLen {
	case 1:
		return uint32(data[0])
	case 2:
		return uint32(bo.Uint16(data))
	case 3:
		return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	case 4:
		return bo.Uint32(data)
	}
	return 0
}
