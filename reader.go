package fastpfor

import (
	"errors"
	"fmt"
	"slices"
)

// Reader provides random access to a FastPFOR-compressed block.
// A Reader is not safe for concurrent use. Create multiple readers from
// the same buffer if concurrent access is needed.
type Reader struct {
	// values holds the unpacked values (decoded once on Load)
	values []uint32

	// pos is the current position for sequential iteration (0-based)
	pos int

	// count is the number of elements in the block
	count int

	// isSorted indicates if the data is sorted (delta without zigzag)
	isSorted bool

	// loaded indicates if the reader has been loaded with data
	loaded bool
}

// ErrInvalidBuffer is returned when the buffer is too small or malformed.
var ErrInvalidBuffer = errors.New("fastpfor: invalid buffer")

// ErrNotLoaded is returned when operations are called before Load().
var ErrNotLoaded = errors.New("fastpfor: reader not loaded")

// ErrPositionOutOfRange is returned when accessing a position beyond the block size.
var ErrPositionOutOfRange = errors.New("fastpfor: position out of range")

// NewReader creates an empty Reader that must be loaded with Load() before use.
func NewReader() *Reader {
	return &Reader{}
}

// Load a FastPFOR-compressed byte buffer into the reader.
// This resets all internal state and can be called multiple times to reuse the reader.
// The buffer must contain a valid single block (packed with PackUint32 or PackDeltaUint32).
func (r *Reader) Load(buf []byte) error {
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

	// Reset state
	r.count = count
	r.isSorted = hasDelta && !hasZigZag // Delta without zigzag implies sorted/monotonic
	r.pos = 0
	r.loaded = true

	if count == 0 {
		if r.values == nil {
			r.values = make([]uint32, 0)
		} else {
			r.values = r.values[:0]
		}
		return nil
	}

	// Decode all values upfront for efficient random access
	// Block size is at most 128, so this is always fast
	if cap(r.values) < count {
		r.values = make([]uint32, count)
	} else {
		r.values = r.values[:count]
	}

	if bitWidth == 0 {
		// All values are zero
		clear(r.values)
	} else {
		unpackLanes(r.values, buf[headerBytes:minNeeded], count, bitWidth)
	}

	// Apply exceptions if present
	if hasExceptions {
		if err := applyExceptions(r.values, buf, minNeeded, count, bitWidth); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidBuffer, err)
		}
	}

	// Apply delta decoding if needed
	if hasDelta && count > 0 {
		deltaDecode(r.values, r.values, hasZigZag)
	}

	return nil
}

// IsLoaded returns whether the reader has been loaded with data.
func (r *Reader) IsLoaded() bool {
	return r.loaded
}

// Len returns the number of elements in the block.
func (r *Reader) Len() int {
	return r.count
}

// Pos returns the current position for sequential iteration.
func (r *Reader) Pos() int {
	return r.pos
}

// Reset resets the reader position to the beginning for sequential iteration.
func (r *Reader) Reset() {
	r.pos = 0
}

// Get returns the value at the specified position.
// Returns an error if the reader is not loaded or pos is out of range.
func (r *Reader) Get(pos int) (uint32, error) {
	if !r.loaded {
		return 0, ErrNotLoaded
	}
	if pos < 0 || pos >= r.count {
		return 0, ErrPositionOutOfRange
	}
	return r.values[pos], nil
}

// GetSafe returns the value at the specified position and whether the position is valid.
// Returns (0, false) if the reader is not loaded or pos is out of range.
func (r *Reader) GetSafe(pos int) (uint32, bool) {
	val, err := r.Get(pos)
	return val, err == nil
}

// Next returns the next value in sequence and its position.
// Returns (value, pos, true) on success, or (0, 0, false) if not loaded or no more elements.
func (r *Reader) Next() (value uint32, pos uint8, ok bool) {
	if !r.loaded || r.pos >= r.count {
		return 0, 0, false
	}
	value = r.values[r.pos]
	pos = uint8(r.pos)
	r.pos++
	return value, pos, true
}

// SkipTo advances to and returns the first value >= req.
// This method is designed for sorted data where values are monotonically increasing.
// Returns (value, pos, true) if found, or (0, 0, false) if not loaded or no value >= req exists.
//
// Note: For non-sorted data (including delta+zigzag sawtooth patterns), this method
// uses linear scan which finds the first occurrence of a value >= req in iteration order.
func (r *Reader) SkipTo(req uint32) (value uint32, pos uint8, ok bool) {
	if !r.loaded || r.count == 0 {
		return 0, 0, false
	}

	// For sorted data (delta without zigzag), use binary search
	if r.isSorted {
		return r.skipToBinarySearch(req)
	}

	// For non-sorted data (including delta+zigzag), use linear scan
	return r.skipToLinear(req)
}

// skipToBinarySearch performs binary search for sorted data.
// Searches from current position to end using slices.BinarySearch.
func (r *Reader) skipToBinarySearch(req uint32) (value uint32, pos uint8, ok bool) {
	// Search in the slice from current position to end
	searchSlice := r.values[r.pos:]
	idx, _ := slices.BinarySearch(searchSlice, req)

	// Convert relative index to absolute position
	absPos := r.pos + idx

	if absPos >= r.count {
		r.pos = r.count
		return 0, 0, false
	}

	r.pos = absPos + 1
	return r.values[absPos], uint8(absPos), true
}

// skipToLinear performs linear scan for non-sorted data.
func (r *Reader) skipToLinear(req uint32) (value uint32, pos uint8, ok bool) {
	for r.pos < r.count {
		v := r.values[r.pos]
		p := uint8(r.pos)
		r.pos++
		if v >= req {
			return v, p, true
		}
	}
	return 0, 0, false
}

// Decode copies all decoded values into the provided destination slice.
// If dst has insufficient capacity, a new slice is allocated.
// Returns nil if the reader is not loaded.
func (r *Reader) Decode(dst []uint32) []uint32 {
	if !r.loaded {
		return nil
	}
	if cap(dst) < r.count {
		dst = make([]uint32, r.count)
	} else {
		dst = dst[:r.count]
	}
	copy(dst, r.values)
	return dst
}

// IsSorted returns whether the data is known to be sorted (monotonically increasing).
// This is true when delta encoding was used without zigzag (positive deltas only).
func (r *Reader) IsSorted() bool {
	return r.isSorted
}
