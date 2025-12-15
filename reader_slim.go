package fastpfor

import "fmt"

// SlimReader provides memory-efficient random access to FastPFOR-compressed blocks.
// Unlike Reader, SlimReader does not pre-decode values into a buffer. Instead, it
// stores only a pointer to the compressed data and decodes on-the-fly when accessed.
//
// SlimReader is optimized for scenarios with millions of readers where memory is
// critical and the underlying data is provided via MMAP. Each SlimReader instance
// uses only ~40 bytes of memory (vs Reader which allocates up to 512+ bytes for
// the decoded values buffer).

// SlimReader is safe for concurrent read access to the same underlying buffer,
// but each SlimReader instance should not be accessed concurrently.
type SlimReader struct {
	buf        []byte // 24 bytes - slice header pointing to compressed data
	lastValue  uint32 // 4 bytes - cumulative value for delta iteration
	count      uint8  // 1 byte - element count (0-128)
	bitWidth   uint8  // 1 byte - bit width for packed values (0-32)
	flags      uint8  // 1 byte - packed flags (includes loaded flag)
	pos        uint8  // 1 byte - current iteration position
	payloadEnd uint16 // 2 bytes - offset where payload ends (exceptions start)
	excPos     uint8  // 1 byte - current exception index for iteration
	_          uint8  // 1 byte - padding
	// Total: 24 + 4 + 8 = 36 bytes, aligned to 40 bytes
}

// SlimReader flag bits
const (
	slimFlagDelta      = 1 << 0
	slimFlagZigZag     = 1 << 1
	slimFlagExceptions = 1 << 2
	slimFlagLoaded     = 1 << 3
)

// NewSlimReader creates an empty SlimReader that must be loaded with Load() before use.
func NewSlimReader() *SlimReader {
	return &SlimReader{}
}

// Load loads a FastPFOR-compressed byte buffer into the reader.
// This resets all internal state and can be called multiple times to reuse the reader.
// The buffer must remain valid for the lifetime of the SlimReader (ideal for MMAP).
// Delta encoding is auto-detected from the header flag.
func (r *SlimReader) Load(buf []byte) error {
	if len(buf) < headerBytes {
		return fmt.Errorf("%w: buffer too small for header (need %d bytes, got %d)",
			ErrInvalidBuffer, headerBytes, len(buf))
	}

	header := bo.Uint32(buf[:headerBytes])
	count, bitWidth, hasExceptions, hasDelta, hasZigZag := decodeHeader(header)

	if count < 0 || count > blockSize {
		return fmt.Errorf("%w: invalid element count %d", ErrInvalidBuffer, count)
	}

	payloadLen := payloadBytes(bitWidth)
	minNeeded := headerBytes + payloadLen

	if len(buf) < minNeeded {
		return fmt.Errorf("%w: buffer truncated (need %d bytes, got %d)",
			ErrInvalidBuffer, minNeeded, len(buf))
	}

	// Build flags
	var flags uint8 = slimFlagLoaded
	if hasDelta {
		flags |= slimFlagDelta
	}
	if hasZigZag {
		flags |= slimFlagZigZag
	}
	if hasExceptions {
		flags |= slimFlagExceptions
	}

	// Reset all state
	r.buf = buf
	r.count = uint8(count)
	r.bitWidth = uint8(bitWidth)
	r.flags = flags
	r.payloadEnd = uint16(minNeeded)
	r.pos = 0
	r.excPos = 0
	r.lastValue = 0

	return nil
}

// IsLoaded returns whether the reader has been loaded with data.
func (r *SlimReader) IsLoaded() bool {
	return r.flags&slimFlagLoaded != 0
}

// IsSorted returns true if the data is sorted (delta-encoded without zigzag).
func (r *SlimReader) IsSorted() bool {
	return r.flags&slimFlagDelta != 0 && r.flags&slimFlagZigZag == 0
}

// Len returns the number of elements in the block.
func (r *SlimReader) Len() int {
	return int(r.count)
}

// Get returns the value at the specified position.
// For non-delta data, this extracts only the single value (O(1)).
// For delta data, this decodes all values up to pos (O(n) due to prefix sum).
// Panics if the reader is not loaded or pos is out of range.
func (r *SlimReader) Get(pos int) (uint32, error) {
	if r.flags&slimFlagLoaded == 0 {
		return 0, ErrNotLoaded
	}
	if pos < 0 || pos >= int(r.count) {
		return 0, ErrPositionOutOfRange
	}

	// For delta-encoded data, we must decode all values up to pos for prefix sum
	if r.flags&slimFlagDelta != 0 {
		return r.getWithDelta(uint32(pos)), nil
	}

	// For non-delta data, extract just the single value
	return r.getSingle(uint32(pos)), nil
}

// getSingle extracts a single value without full block decode (non-delta path).
func (r *SlimReader) getSingle(pos uint32) uint32 {
	bitWidth := int(r.bitWidth)

	// Extract the base value from bit-packed lanes
	var value uint32
	if bitWidth > 0 {
		value = r.extractValue(pos, bitWidth)
	}

	// Check if this position has an exception
	if r.flags&slimFlagExceptions != 0 {
		value = r.applyExceptionIfPresent(pos, value, bitWidth)
	}

	return value
}

// extractValue extracts a single value from the interleaved bit-packed lanes.
// Lane layout: values are split into 4 lanes, each encoding every 4th element.
// Lane 0: v0, v4, v8, ... Lane 1: v1, v5, v9, ... etc.
// Lanes are interleaved in 16-byte blocks in the payload.
// I benchmarked that a 1-lane layout wouldn't be taht much faster than the 4-lane layout.
func (r *SlimReader) extractValue(pos uint32, bitWidth int) uint32 {
	// Determine which lane and position within the lane
	// Using bit operations: pos & 3 = pos % 4, pos >> 2 = pos / 4
	lane := int(pos) & 3
	posInLane := int(pos) >> 2

	// Calculate bit position within the lane's data
	bitPos := posInLane * bitWidth

	// The lane's words are interleaved at stride 16 bytes (4 words per block)
	// Word index within lane = bitPos / 32, bit offset within word = bitPos % 32
	wordInLane := bitPos >> 5 // bitPos / 32
	bitOffset := bitPos & 31  // bitPos % 32

	// Calculate byte offset in payload for this lane's word
	// Each 16-byte block has one word from each lane
	// Word N of lane L is at: block N * 16 + lane L * 4
	payload := r.buf[headerBytes:r.payloadEnd]
	byteOffset := wordInLane<<4 + lane<<2 // wordInLane*16 + lane*4

	// Read the value, handling the case where it spans two words
	var acc uint64
	if byteOffset+4 <= len(payload) {
		acc = uint64(bo.Uint32(payload[byteOffset:]))
	}

	// If value spans into next word, read it too
	bitsInFirstWord := 32 - bitOffset
	if bitWidth > bitsInFirstWord {
		nextByteOffset := byteOffset + 16 // Next word for this lane
		if nextByteOffset+4 <= len(payload) {
			acc |= uint64(bo.Uint32(payload[nextByteOffset:])) << 32
		}
	}

	// Extract the value
	acc >>= bitOffset
	mask := uint64((1 << bitWidth) - 1)
	if bitWidth == 32 {
		mask = 0xFFFFFFFF
	}

	return uint32(acc & mask)
}

// applyExceptionIfPresent checks if pos has an exception and applies it.
func (r *SlimReader) applyExceptionIfPresent(pos uint32, value uint32, bitWidth int) uint32 {
	patch := r.buf[r.payloadEnd:]
	excCount := int(patch[0])
	if excCount == 0 {
		return value
	}

	positions := patch[1 : 1+excCount]

	// Find if pos is in the exception list (positions are sorted ascending)
	var excIndex int
	for excIndex = range positions {
		if uint32(positions[excIndex]) == pos {
			goto applyException
		}
		if uint32(positions[excIndex]) > pos {
			return value // Passed our position, no exception
		}
	}
	return value // No exception for this position

applyException:
	// Decode only the needed exception high bit using StreamVByte random access
	svbLenOffset := 1 + excCount
	svbData := patch[svbLenOffset+2:]
	highBit := svbDecodeOne(svbData, excCount, excIndex)

	// Apply the exception
	return value | (highBit << bitWidth)
}

// getWithDelta decodes values with delta encoding (requires prefix sum).
func (r *SlimReader) getWithDelta(pos uint32) uint32 {
	// Stack-allocated buffer for decoding
	var values [blockSize]uint32

	count := int(r.count)
	bitWidth := int(r.bitWidth)

	// Decode packed values
	if bitWidth > 0 {
		unpackLanes(values[:count], r.buf[headerBytes:r.payloadEnd], count, bitWidth)
	}

	// Apply exceptions if present
	if r.flags&slimFlagExceptions != 0 {
		_ = applyExceptions(values[:], r.buf, int(r.payloadEnd), count, bitWidth)
	}

	// Apply delta decoding
	useZigZag := r.flags&slimFlagZigZag != 0
	deltaDecode(values[:count], values[:count], useZigZag)

	return values[pos]
}

// GetSafe returns the value at the specified position and whether the position is valid.
// Returns (0, false) if the reader is not loaded or pos is out of range.
func (r *SlimReader) GetSafe(pos int) (uint32, bool) {
	val, err := r.Get(pos)
	return val, err == nil
}

// Pos returns the current position for sequential iteration.
func (r *SlimReader) Pos() int {
	return int(r.pos)
}

// Reset resets the reader position to the beginning for sequential iteration.
func (r *SlimReader) Reset() {
	r.pos = 0
	r.excPos = 0
	r.lastValue = 0
}

// Next returns the next value in sequence and its position.
// Returns (value, pos, true) on success, or (0, 0, false) if not loaded or no more elements.
// For both delta and non-delta data, this is O(1) per call.
func (r *SlimReader) Next() (value uint32, pos uint8, ok bool) {
	if r.flags&slimFlagLoaded == 0 || r.pos >= r.count {
		return 0, 0, false
	}

	pos = r.pos
	value = r.nextValue()
	r.pos++
	return value, pos, true
}

// nextValue extracts the next value, using incremental delta decoding if needed.
func (r *SlimReader) nextValue() uint32 {
	bitWidth := int(r.bitWidth)

	// Extract base value from bit-packed lanes
	var value uint32
	if bitWidth > 0 {
		value = r.extractValue(uint32(r.pos), bitWidth)
	}

	// Apply exception if present
	if r.flags&slimFlagExceptions != 0 {
		value = r.applyExceptionIfPresent(uint32(r.pos), value, bitWidth)
	}

	// Apply delta decoding incrementally
	if r.flags&slimFlagDelta != 0 {
		if r.flags&slimFlagZigZag != 0 {
			value = uint32(zigzagDecode32(value))
		}
		value += r.lastValue
		r.lastValue = value
	}

	return value
}

// SkipTo advances to and returns the first value >= req.
// This method is designed for sorted data where values are monotonically increasing.
// Returns (value, pos, true) if found, or (0, 0, false) if not loaded or no value >= req exists.
//
// Uses incremental decoding with O(1) per value scanned.
func (r *SlimReader) SkipTo(req uint32) (value uint32, pos uint8, ok bool) {
	if r.flags&slimFlagLoaded == 0 {
		return 0, 0, false
	}
	for r.pos < r.count {
		p := r.pos
		v := r.nextValue()
		r.pos++

		if v >= req {
			return v, p, true
		}
	}
	return 0, 0, false
}

// Decode decodes all values into the provided destination slice.
// This is more efficient than multiple Get() calls when all values are needed.
// The dst slice will be resized as needed.
// Returns nil if the reader is not loaded.
func (r *SlimReader) Decode(dst []uint32) []uint32 {
	if r.flags&slimFlagLoaded == 0 {
		return nil
	}
	count := int(r.count)
	if cap(dst) < count {
		dst = make([]uint32, count)
	} else {
		dst = dst[:count]
	}

	if count == 0 {
		return dst
	}

	bitWidth := int(r.bitWidth)

	// Decode packed values
	if bitWidth == 0 {
		clear(dst)
	} else {
		unpackLanes(dst, r.buf[headerBytes:r.payloadEnd], count, bitWidth)
	}

	// Apply exceptions if present
	if r.flags&slimFlagExceptions != 0 {
		_ = applyExceptions(dst, r.buf, int(r.payloadEnd), count, bitWidth)
	}

	// Apply delta decoding if needed
	if r.flags&slimFlagDelta != 0 {
		useZigZag := r.flags&slimFlagZigZag != 0
		deltaDecode(dst, dst, useZigZag)
	}

	return dst
}
