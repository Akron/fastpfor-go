package fastpfor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// loadSlimReader is a test helper that creates and loads a SlimReader.
func loadSlimReader(buf []byte) (*SlimReader, error) {
	r := NewSlimReader()
	if err := r.Load(buf); err != nil {
		return nil, err
	}
	return r, nil
}

// -----------------------------------------------------------------------------
// SlimReader Tests
// -----------------------------------------------------------------------------

// TestSlimReaderBasic tests basic SlimReader functionality.
func TestSlimReaderBasic(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30, 40, 50, 60, 70, 80}
	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)
	assert.Equal(len(values), reader.Len())

	// Test Get
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestSlimReaderDelta tests SlimReader with delta-encoded data.
func TestSlimReaderDelta(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 35, 50, 75, 100, 150, 200}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// Test Get
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestSlimReaderDeltaZigZag tests SlimReader with delta+zigzag encoding.
func TestSlimReaderDeltaZigZag(t *testing.T) {
	assert := assert.New(t)

	// Non-monotonic values require zigzag
	values := []uint32{100, 50, 75, 25, 80}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// Test Get
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestSlimReaderDeltaZigZagWithExceptions tests SlimReader with sawtooth data
// that requires both zigzag encoding and exceptions.
func TestSlimReaderDeltaZigZagWithExceptions(t *testing.T) {
	assert := assert.New(t)

	// Create sawtooth data with large negative jumps that become exceptions
	original := make([]uint32, 64)
	value := uint32(1 << 20)
	for i := range original {
		switch i {
		case 0:
			original[i] = value
		case 20:
			value -= 5000 // Negative delta
			original[i] = value
		case 40:
			value += 1 << 24 // Large positive jump (exception)
			original[i] = value
		default:
			value++
			original[i] = value
		}
	}

	src := make([]uint32, len(original))
	copy(src, original)
	packed := PackDeltaUint32(nil, src)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// Test Get() for all positions
	for i, want := range original {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}

	// Test Next() iteration
	reader.Reset()
	for i, want := range original {
		got, pos, ok := reader.Next()
		assert.True(ok, "Next() at position %d", i)
		assert.Equal(uint8(i), pos)
		assert.Equal(want, got, "Next() value at position %d", i)
	}

	// Test Decode()
	decoded := reader.Decode(nil)
	for i, want := range original {
		assert.Equal(want, decoded[i], "Decode()[%d]", i)
	}
}

// TestSlimReaderWithExceptions tests SlimReader with exception values.
func TestSlimReaderWithExceptions(t *testing.T) {
	assert := assert.New(t)

	values := make([]uint32, 64)
	for i := range values {
		values[i] = uint32(i)
	}
	// Add large values that become exceptions
	values[5] = 1000000
	values[20] = 5000000
	values[50] = 10000000

	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// Test Get
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestSlimReaderGetSafe tests the GetSafe method.
func TestSlimReaderGetSafe(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30}
	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// Valid position
	val, ok := reader.GetSafe(1)
	assert.True(ok)
	assert.Equal(uint32(20), val)

	// Invalid position
	_, ok = reader.GetSafe(10)
	assert.False(ok)
}

// TestSlimReaderDecode tests the Decode method.
func TestSlimReaderDecode(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30, 40, 50}
	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	decoded := reader.Decode(nil)
	assert.Equal(len(values), len(decoded))

	for i, want := range values {
		assert.Equal(want, decoded[i], "Decode()[%d]", i)
	}
}

// TestSlimReaderEmpty tests SlimReader with empty data.
func TestSlimReaderEmpty(t *testing.T) {
	assert := assert.New(t)

	packed := PackUint32(nil, []uint32{})

	reader, err := loadSlimReader(packed)
	assert.NoError(err)
	assert.Equal(0, reader.Len())

	decoded := reader.Decode(nil)
	assert.Equal(0, len(decoded))
}

// TestSlimReaderFullBlock tests SlimReader with a full 128-value block.
func TestSlimReaderFullBlock(t *testing.T) {
	assert := assert.New(t)

	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 7)
	}
	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// Test random access
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestSlimReaderSingleValueExtraction tests the optimized single-value extraction
// across all bit widths to ensure correctness.
func TestSlimReaderSingleValueExtraction(t *testing.T) {
	for bitWidth := 1; bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth_%d", bitWidth), func(t *testing.T) {
			assert := assert.New(t)

			// Create values that fit in the bit width
			values := make([]uint32, 128)
			mask := uint32(0xFFFFFFFF)
			if bitWidth < 32 {
				mask = (1 << bitWidth) - 1
			}
			for i := range values {
				values[i] = uint32(i*7+3) & mask
			}

			packed := PackUint32(nil, values)

			reader, err := loadSlimReader(packed)
			assert.NoError(err)

			// Test random access at various positions
			testPositions := []int{0, 1, 2, 3, 4, 31, 32, 63, 64, 95, 96, 127}
			for _, pos := range testPositions {
				if pos >= len(values) {
					continue
				}
				got, err := reader.Get(pos)
				assert.NoError(err)
				assert.Equal(values[pos], got, "bitWidth=%d, pos=%d", bitWidth, pos)
			}
		})
	}
}

// TestSlimReaderMatchesReader verifies SlimReader produces same results as Reader.
func TestSlimReaderMatchesReader(t *testing.T) {
	testCases := []struct {
		name   string
		values []uint32
		delta  bool
	}{
		{"simple", []uint32{1, 2, 3, 4, 5}, false},
		{"with_exceptions", func() []uint32 {
			v := make([]uint32, 64)
			for i := range v {
				v[i] = uint32(i)
			}
			v[10] = 1000000
			return v
		}(), false},
		{"delta_monotonic", []uint32{10, 20, 30, 40, 50}, true},
		{"delta_zigzag", []uint32{100, 50, 75, 25, 80}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			valuesCopy := make([]uint32, len(tc.values))
			copy(valuesCopy, tc.values)

			var packed []byte
			if tc.delta {
				packed = PackDeltaUint32(nil, valuesCopy)
			} else {
				packed = PackUint32(nil, tc.values)
			}

			reader, err := loadReader(packed)
			assert.NoError(err)

			slimReader, err := loadSlimReader(packed)
			assert.NoError(err)

			// Compare Get results
			for i := 0; i < reader.Len(); i++ {
				readerVal, err1 := reader.Get(i)
				slimVal, err2 := slimReader.Get(i)
				assert.NoError(err1)
				assert.NoError(err2)
				assert.Equal(readerVal, slimVal, "pos %d", i)
			}
		})
	}
}

// TestSlimReaderNotLoaded tests behavior before Load() is called.
func TestSlimReaderNotLoaded(t *testing.T) {
	assert := assert.New(t)

	reader := NewSlimReader()

	assert.False(reader.IsLoaded())

	// GetSafe should return false
	_, ok := reader.GetSafe(0)
	assert.False(ok)

	// Next should return false
	_, _, ok = reader.Next()
	assert.False(ok)

	// SkipTo should return false
	_, _, ok = reader.SkipTo(0)
	assert.False(ok)

	// Decode should return nil
	assert.Nil(reader.Decode(nil))
}

// TestSlimReaderLoad tests the Load method.
func TestSlimReaderLoad(t *testing.T) {
	assert := assert.New(t)

	reader := NewSlimReader()

	values := []uint32{10, 20, 30, 40, 50}
	packed := PackUint32(nil, values)

	assert.NoError(reader.Load(packed))
	assert.True(reader.IsLoaded())

	// Verify values
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}

	// Load again with different data
	values2 := []uint32{100, 200, 300}
	packed2 := PackUint32(nil, values2)

	assert.NoError(reader.Load(packed2))
	assert.Equal(len(values2), reader.Len())

	for i, want := range values2 {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d) after reload", i)
	}
}

// TestSlimReaderNext tests Next() iteration.
func TestSlimReaderNext(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{100, 200, 300, 400}
	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	for i, want := range values {
		val, pos, ok := reader.Next()
		assert.True(ok, "iteration %d", i)
		assert.Equal(want, val, "iteration %d", i)
		assert.Equal(uint8(i), pos, "iteration %d", i)
	}

	// Should return false after exhaustion
	_, _, ok := reader.Next()
	assert.False(ok)
}

// TestSlimReaderSkipTo tests SkipTo() functionality.
func TestSlimReaderSkipTo(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30, 40, 50, 60, 70, 80}
	packed := PackUint32(nil, values)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	// SkipTo exact value
	val, pos, ok := reader.SkipTo(30)
	assert.True(ok)
	assert.Equal(uint32(30), val)
	assert.Equal(uint8(2), pos)

	// SkipTo value not in set (should find next)
	val, pos, ok = reader.SkipTo(55)
	assert.True(ok)
	assert.Equal(uint32(60), val)
	assert.Equal(uint8(5), pos)

	// SkipTo beyond range
	_, _, ok = reader.SkipTo(100)
	assert.False(ok)
}

// TestSlimReaderNextDelta tests Next() with delta-encoded data.
func TestSlimReaderNextDelta(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 35, 50, 75, 100}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadSlimReader(packed)
	assert.NoError(err)

	for i, want := range values {
		val, pos, ok := reader.Next()
		assert.True(ok, "iteration %d", i)
		assert.Equal(want, val, "iteration %d", i)
		assert.Equal(uint8(i), pos)
	}
}

// -----------------------------------------------------------------------------
// SlimReader Benchmarks
// -----------------------------------------------------------------------------

// BenchmarkSlimReaderGet benchmarks SlimReader.Get performance.
func BenchmarkSlimReaderGet(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)
	reader, _ := loadSlimReader(packed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = reader.Get(i % 128)
	}
}

// BenchmarkSlimReaderDecode benchmarks SlimReader.Decode performance.
func BenchmarkSlimReaderDecode(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)
	reader, _ := loadSlimReader(packed)
	dst := make([]uint32, 128)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = reader.Decode(dst)
	}
}

// BenchmarkLoadSlimReader benchmarks SlimReader creation.
func BenchmarkLoadSlimReader(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = loadSlimReader(packed)
	}
}

// BenchmarkSlimReaderMemory shows the memory difference between Reader and SlimReader.
func BenchmarkSlimReaderMemory(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)

	b.Run("Reader", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = loadReader(packed)
		}
	})

	b.Run("SlimReader", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = loadSlimReader(packed)
		}
	})
}

// BenchmarkSlimReaderNext benchmarks SlimReader.Next iteration.
func BenchmarkSlimReaderNext(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)
	reader, _ := loadSlimReader(packed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if reader.Pos() >= reader.Len() {
			reader.Reset()
		}
		_, _, _ = reader.Next()
	}
}

// BenchmarkSlimReaderNextDelta benchmarks SlimReader.Next with delta data.
func BenchmarkSlimReaderNextDelta(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)
	packed := PackDeltaUint32(nil, valuesCopy)
	reader, _ := loadSlimReader(packed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if reader.Pos() >= reader.Len() {
			reader.Reset()
		}
		_, _, _ = reader.Next()
	}
}
