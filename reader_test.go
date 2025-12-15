package fastpfor

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// loadReader is a test helper that creates and loads a Reader.
func loadReader(buf []byte) (*Reader, error) {
	r := NewReader()
	if err := r.Load(buf); err != nil {
		return nil, err
	}
	return r, nil
}

// Example demonstrates basic Reader usage for random access.
func ExampleloadReader() {
	// Compress some values
	values := []uint32{10, 20, 30, 40, 50, 60, 70, 80}
	packed := PackUint32(nil, values)

	// Create a reader
	reader, err := loadReader(packed)
	if err != nil {
		panic(err)
	}

	// Random access by position
	val, _ := reader.Get(3)
	fmt.Println("Get(3):", val)

	// Sequential iteration
	reader.Reset()
	for val, pos, ok := reader.Next(); ok; val, pos, ok = reader.Next() {
		if pos >= 3 {
			break
		}
		fmt.Printf("Next: pos=%d, val=%d\n", pos, val)
	}

	// Output:
	// Get(3): 40
	// Next: pos=0, val=10
	// Next: pos=1, val=20
	// Next: pos=2, val=30
}

// ExampleloadReader_delta demonstrates using SkipTo for efficient searching in sorted data.
func ExampleloadReader_delta() {
	// Sorted values (typical for posting lists, timestamps, etc.)
	values := []uint32{100, 200, 350, 500, 750, 1000, 1500, 2000}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	// Delta encoding is optimal for sorted data
	packed := PackDeltaUint32(nil, valuesCopy)

	// loadReader auto-detects delta encoding from the header
	reader, err := loadReader(packed)
	if err != nil {
		panic(err)
	}

	// SkipTo finds the first value >= target (binary search)
	val, pos, ok := reader.SkipTo(300)
	if ok {
		fmt.Printf("SkipTo(300): pos=%d, val=%d\n", pos, val)
	}

	// Continue searching from current position
	val, pos, ok = reader.SkipTo(700)
	if ok {
		fmt.Printf("SkipTo(700): pos=%d, val=%d\n", pos, val)
	}

	// Output:
	// SkipTo(300): pos=2, val=350
	// SkipTo(700): pos=4, val=750
}

// TestLoadReaderEmpty tests loading an empty block.
func TestLoadReaderEmpty(t *testing.T) {
	assert := assert.New(t)

	packed := PackUint32(nil, []uint32{})
	reader, err := loadReader(packed)
	assert.NoError(err)
	assert.Equal(0, reader.Len())

	// Next should return false immediately
	_, _, ok := reader.Next()
	assert.False(ok, "Next() should return false for empty block")

	// SkipTo should return false
	_, _, ok = reader.SkipTo(0)
	assert.False(ok, "SkipTo() should return false for empty block")
}

// TestLoadReaderSingleValue tests loading a single-value block.
func TestLoadReaderSingleValue(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{42}
	packed := PackUint32(nil, values)
	reader, err := loadReader(packed)
	assert.NoError(err)
	assert.Equal(1, reader.Len())

	// Get by position
	got, err := reader.Get(0)
	assert.NoError(err)
	assert.Equal(uint32(42), got)

	// Next
	val, pos, ok := reader.Next()
	assert.True(ok)
	assert.Equal(uint32(42), val)
	assert.Equal(uint8(0), pos)

	// Next should return false now
	_, _, ok = reader.Next()
	assert.False(ok, "Next() should return false after exhausted")
}

// TestLoadReaderFullBlock tests loading a full 128-value block.
func TestLoadReaderFullBlock(t *testing.T) {
	assert := assert.New(t)

	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 7)
	}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)
	assert.Equal(128, reader.Len())

	// Verify all values via Get
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestReaderGet tests random access via Get.
func TestReaderGet(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30, 40, 50}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	testCases := []struct {
		pos  int
		want uint32
	}{
		{0, 10},
		{1, 20},
		{2, 30},
		{3, 40},
		{4, 50},
	}

	for _, tc := range testCases {
		got, err := reader.Get(tc.pos)
		assert.NoError(err)
		assert.Equal(tc.want, got, "Get(%d)", tc.pos)
	}
}

// TestReaderGetError tests that Get returns error for out-of-range positions.
func TestReaderGetError(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30}
	packed := PackUint32(nil, values)

	reader := NewReader()
	assert.NoError(reader.Load(packed))

	// Out of range should return error
	_, err := reader.Get(3)
	assert.ErrorIs(err, ErrPositionOutOfRange)

	// Not loaded should return error
	unloaded := NewReader()
	_, err = unloaded.Get(0)
	assert.ErrorIs(err, ErrNotLoaded)
}

// TestReaderGetSafe tests safe access via GetSafe.
func TestReaderGetSafe(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// Valid positions
	val, ok := reader.GetSafe(1)
	assert.True(ok)
	assert.Equal(uint32(20), val)

	// Invalid position
	_, ok = reader.GetSafe(10)
	assert.False(ok)
}

// TestReaderNext tests sequential iteration via Next.
func TestReaderNext(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{100, 200, 300, 400}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	for i, want := range values {
		val, pos, ok := reader.Next()
		assert.True(ok, "iteration %d", i)
		assert.Equal(want, val, "iteration %d", i)
		assert.Equal(uint8(i), pos, "iteration %d", i)
	}

	// Should return false after exhaustion
	_, _, ok := reader.Next()
	assert.False(ok, "Next() should return false after exhaustion")
}

// TestReaderReset tests resetting the reader position.
func TestReaderReset(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{1, 2, 3}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// Consume all
	for range values {
		reader.Next()
	}
	assert.Equal(3, reader.Pos())

	// Reset and iterate again
	reader.Reset()
	assert.Equal(0, reader.Pos())

	val, pos, ok := reader.Next()
	assert.True(ok)
	assert.Equal(uint32(1), val)
	assert.Equal(uint8(0), pos)
}

// TestReaderSkipToLinear tests SkipTo on non-delta (non-sorted) data.
func TestReaderSkipToLinear(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{5, 15, 10, 25, 20, 30}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// SkipTo 12 should find 15 (first value >= 12)
	val, pos, ok := reader.SkipTo(12)
	assert.True(ok)
	assert.Equal(uint32(15), val)
	assert.Equal(uint8(1), pos)

	// SkipTo 25 should find 25 (continues from position 2)
	val, pos, ok = reader.SkipTo(25)
	assert.True(ok)
	assert.Equal(uint32(25), val)
	assert.Equal(uint8(3), pos)

	// SkipTo 100 should return false (no value >= 100)
	_, _, ok = reader.SkipTo(100)
	assert.False(ok)
}

// TestLoadReaderDelta tests delta-decoded reader.
func TestLoadReaderDelta(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 35, 50, 75, 100, 150, 200}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadReader(packed)
	assert.NoError(err)
	assert.Equal(len(values), reader.Len())
	assert.True(reader.IsSorted(), "IsSorted() should return true for monotonic delta data")

	// Verify all values are correctly decoded
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestReaderSkipToBinarySearch tests SkipTo on sorted (delta-encoded) data.
func TestReaderSkipToBinarySearch(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// SkipTo exact value
	val, pos, ok := reader.SkipTo(30)
	assert.True(ok)
	assert.Equal(uint32(30), val)
	assert.Equal(uint8(2), pos)

	// SkipTo value not in set (should find next larger)
	val, pos, ok = reader.SkipTo(55)
	assert.True(ok)
	assert.Equal(uint32(60), val)
	assert.Equal(uint8(5), pos)

	// SkipTo value beyond range
	_, _, ok = reader.SkipTo(150)
	assert.False(ok)
}

// TestReaderSkipToFromBeginning tests SkipTo starting from position 0.
func TestReaderSkipToFromBeginning(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{5, 15, 25, 35, 45}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// SkipTo value smaller than first element
	val, pos, ok := reader.SkipTo(3)
	assert.True(ok)
	assert.Equal(uint32(5), val)
	assert.Equal(uint8(0), pos)
}

// TestReaderSkipToProgression tests multiple SkipTo calls in sequence.
func TestReaderSkipToProgression(t *testing.T) {
	assert := assert.New(t)

	values := make([]uint32, 100)
	for i := range values {
		values[i] = uint32(i * 10)
	}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// Multiple SkipTo calls should progress forward
	targets := []uint32{50, 200, 500, 800}
	expected := []struct {
		val uint32
		pos uint8
	}{
		{50, 5},
		{200, 20},
		{500, 50},
		{800, 80},
	}

	for i, target := range targets {
		val, pos, ok := reader.SkipTo(target)
		assert.True(ok)
		assert.Equal(expected[i].val, val, "SkipTo(%d)", target)
		assert.Equal(expected[i].pos, pos, "SkipTo(%d)", target)
	}
}

// TestReaderAll tests the All() method.
func TestReaderAll(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{1, 2, 3, 4, 5}
	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	all := reader.All()
	assert.Equal(len(values), len(all))

	for i, want := range values {
		assert.Equal(want, all[i], "All()[%d]", i)
	}

	// Verify it's a copy (modifying doesn't affect reader)
	all[0] = 999
	val, _ := reader.Get(0)
	assert.NotEqual(uint32(999), val, "All() should return a copy, not the internal slice")
}

// TestLoadReaderInvalidBuffer tests error handling for invalid buffers.
func TestLoadReaderInvalidBuffer(t *testing.T) {
	testCases := []struct {
		name string
		buf  []byte
	}{
		{"nil buffer", nil},
		{"empty buffer", []byte{}},
		{"too short for header", []byte{0, 1, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadReader(tc.buf)
			assert.Error(t, err)
		})
	}
}

// TestLoadReaderWithExceptions tests loading blocks with exceptions.
func TestLoadReaderWithExceptions(t *testing.T) {
	assert := assert.New(t)

	values := make([]uint32, 64)
	for i := range values {
		values[i] = uint32(i)
	}
	// Add some large values that will become exceptions
	values[5] = 1000000
	values[20] = 5000000
	values[50] = 10000000

	packed := PackUint32(nil, values)

	reader, err := loadReader(packed)
	assert.NoError(err)

	// Verify all values including exceptions
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestLoadReaderAutoDeltaDetect tests that loadReader auto-detects delta encoding.
func TestLoadReaderAutoDeltaDetect(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{10, 20, 35, 50, 75, 100, 150, 200}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadReader(packed)
	assert.NoError(err)
	assert.True(reader.IsSorted(), "IsSorted() should return true for monotonic delta-encoded data")

	// Verify all values are correctly decoded
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}

	// SkipTo should use binary search since data is sorted
	val, pos, ok := reader.SkipTo(30)
	assert.True(ok)
	assert.Equal(uint32(35), val)
	assert.Equal(uint8(2), pos)
}

// TestLoadReaderDeltaWithZigZag tests delta decoding with zigzag (non-monotonic data).
func TestLoadReaderDeltaWithZigZag(t *testing.T) {
	assert := assert.New(t)

	values := []uint32{100, 50, 75, 25, 80}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)

	packed := PackDeltaUint32(nil, valuesCopy)

	reader, err := loadReader(packed)
	assert.NoError(err)
	assert.False(reader.IsSorted(), "IsSorted() should return false for non-monotonic (zigzag) data")

	// Verify all values
	for i, want := range values {
		got, err := reader.Get(i)
		assert.NoError(err)
		assert.Equal(want, got, "Get(%d)", i)
	}
}

// TestReaderRandomData tests reader with random data.
func TestReaderRandomData(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))

	for trial := range 20 {
		count := rng.Intn(128) + 1
		values := make([]uint32, count)
		for i := range values {
			values[i] = rng.Uint32() & 0xFFFFFF // 24-bit values
		}

		packed := PackUint32(nil, values)
		reader, err := loadReader(packed)
		assert.NoError(t, err, "trial %d", trial)

		// Verify random access
		for i, want := range values {
			got, err := reader.Get(i)
			assert.NoError(t, err)
			assert.Equal(t, want, got, "trial %d: Get(%d)", trial, i)
		}
	}
}

// TestReaderDeltaSortedRandomData tests delta reader with sorted random data.
func TestReaderDeltaSortedRandomData(t *testing.T) {
	rng := rand.New(rand.NewSource(67890))

	for trial := range 20 {
		count := rng.Intn(128) + 1
		values := make([]uint32, count)
		for i := range values {
			values[i] = rng.Uint32() & 0xFFFFFF
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

		// Make a copy for packing (delta encode mutates)
		valuesCopy := make([]uint32, len(values))
		copy(valuesCopy, values)

		packed := PackDeltaUint32(nil, valuesCopy)
		reader, err := loadReader(packed)
		assert.NoError(t, err, "trial %d", trial)

		// Verify random access
		for i, want := range values {
			got, err := reader.Get(i)
			assert.NoError(t, err)
			assert.Equal(t, want, got, "trial %d: Get(%d)", trial, i)
		}

		// Test SkipTo with random targets
		for range 5 {
			reader.Reset()
			target := rng.Uint32() & 0xFFFFFF

			val, pos, ok := reader.SkipTo(target)

			// Find expected result
			var expectedVal uint32
			var expectedPos int
			found := false
			for i, v := range values {
				if v >= target {
					expectedVal = v
					expectedPos = i
					found = true
					break
				}
			}

			if found {
				assert.True(t, ok, "trial %d: SkipTo(%d) should find value", trial, target)
				assert.Equal(t, expectedVal, val, "trial %d: SkipTo(%d) value", trial, target)
				assert.Equal(t, expectedPos, int(pos), "trial %d: SkipTo(%d) pos", trial, target)
			} else {
				assert.False(t, ok, "trial %d: SkipTo(%d) should not find value", trial, target)
			}
		}
	}
}

// TestReaderNotLoaded tests behavior before Load() is called.
func TestReaderNotLoaded(t *testing.T) {
	assert := assert.New(t)

	reader := NewReader()

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

	// All should return nil
	assert.Nil(reader.All())
}

// TestReaderLoad tests the Load method.
func TestReaderLoad(t *testing.T) {
	assert := assert.New(t)

	reader := NewReader()

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

// ----------------------------------------------------------------------------
// Reader Benchmarks
// ----------------------------------------------------------------------------

func BenchmarkLoadReader(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = loadReader(packed)
	}
}

func BenchmarkLoadReaderDelta(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)
	packed := PackDeltaUint32(nil, valuesCopy)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = loadReader(packed)
	}
}

func BenchmarkReaderGet(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)
	reader, _ := loadReader(packed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = reader.Get(i % 128)
	}
}

func BenchmarkReaderNext(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)
	reader, _ := loadReader(packed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if reader.Pos() >= reader.Len() {
			reader.Reset()
		}
		_, _, _ = reader.Next()
	}
}

func BenchmarkReaderSkipTo(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	valuesCopy := make([]uint32, len(values))
	copy(valuesCopy, values)
	packed := PackDeltaUint32(nil, valuesCopy)
	reader, _ := loadReader(packed)

	targets := []uint32{500, 2000, 5000, 8000, 10000, 12000}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		for _, t := range targets {
			_, _, _ = reader.SkipTo(t)
		}
	}
}

func BenchmarkReaderAll(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i * 100)
	}
	packed := PackUint32(nil, values)
	reader, _ := loadReader(packed)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = reader.All()
	}
}

func BenchmarkReaderWithExceptions(b *testing.B) {
	values := make([]uint32, 128)
	for i := range values {
		values[i] = uint32(i)
	}
	// Add exceptions
	for i := range 10 {
		values[i*10] = uint32(1000000 + i)
	}
	packed := PackUint32(nil, values)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = loadReader(packed)
	}
}
