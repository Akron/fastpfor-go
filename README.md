# FastPFOR in Go

Package fastpfor implements integer encoding and decoding using
**PFOR** [1] and relying heavily on the [FastPFOR](https://github.com/fast-pack/FastPFOR) [2] implementation,
making use of SIMD kernels provided by [robskie/bp128](github.com/robskie/bp128).
Exceptions are compressed using [StreamVByte](https://github.com/mhr3/streamvbyte) [3].

*! This is work in progress and especially the layout may change significantly in the future.*

- [1] Zukowski, M., Heman, S., Nes, N., & Boncz, P. (2006). *Super-Scalar RAM-CPU Cache Compression*.
    22nd International Conference on Data Engineering (ICDE’06), 59–59.
    https://doi.org/10.1109/ICDE.2006.150
- [2] Lemire, D., & Boytsov, L. (2015). *Decoding billions of integers per second through vectorization*.
    Software: Practice and Experience, 45(1), 1–29. https://doi.org/10.1002/spe.2203
- [3] Lemire, D., Kurz, N. & Rupp, C. (2018). *Stream VByte: Faster Byte-Oriented Integer Compression*, Information Processing Letters 130.

## Installation
```sh
go generate ./internal/avo
go get github.com/akron/fastpfor-go
```

## Usage

The codec operates on fixed blocks of up to 128 unsigned 32-bit integers:

```go
package main

import (
    "fmt"
    fastpfor "github.com/akron/fastpfor-go"
)

func main() {
    // Original data (up to 128 uint32 values per block)
    values := []uint32{10, 20, 30, 40, 50, 60, 70, 80}

    // Compress
    encoded := fastpfor.PackUint32(nil, values)
    fmt.Printf("Compressed %d integers into %d bytes\n", len(values), len(encoded))

    // Decompress
    decoded, err := fastpfor.UnpackUint32(nil, encoded)
    if err != nil {
        panic(err)
    }
    fmt.Println("Decoded:", decoded)
}
```

For sorted or time-series data, use delta encoding for better compression:

```go
// Monotonically increasing data benefits from delta encoding
timestamps := []uint32{1000, 1005, 1012, 1018, 1025, 1033, 1040, 1048}

// Compress with delta encoding (mutates timestamps in-place)
encoded := fastpfor.PackDeltaUint32(nil, timestamps)

// Decompress - UnpackUint32 auto-detects delta encoding from the header
decoded, _ := fastpfor.UnpackUint32(nil, encoded)
```

**Note:** `PackDeltaUint32` performs delta encoding in-place, mutating the input slice.
If you need to preserve the original values, make a copy first.

`UnpackUint32` automatically detects and decodes delta-encoded data based on the header flags.

Reuse buffers to avoid allocations in hot paths:

```go
// Pre-allocate buffers with cap >= 256 for zero-allocation operation.
// The extra capacity (positions 128-255) is used as scratch space for exceptions.
encodeBuf := make([]byte, 0, fastpfor.MaxBlockSizeUint32())
decodeBuf := make([]uint32, 0, 256) // cap >= 256 for zero-alloc unpack
workBuf := make([]uint32, 128, 256) // cap >= 256 for zero-alloc pack

for _, block := range blocks {
    // Copy block to working buffer (PackDeltaUint32 mutates its input)
    copy(workBuf, block)
    encoded := fastpfor.PackDeltaUint32(encodeBuf[:0], workBuf[:len(block)])
    decoded, _ := fastpfor.UnpackUint32(decodeBuf[:0], encoded)
    // Process decoded...
}
```

## Reader Types

The package provides two reader types for random access to compressed blocks:

### Reader

`Reader` decodes all values upfront for fast random access. Best for repeated access patterns.

```go
reader := fastpfor.NewReader()
if err := reader.Load(compressed); err != nil {
    return err
}

// Random access by position
val, err := reader.Get(5)

// Sequential iteration
for val, pos, ok := reader.Next(); ok; val, pos, ok = reader.Next() {
    fmt.Printf("pos=%d, val=%d\n", pos, val)
}

// Binary search for sorted data (delta-encoded without zigzag)
if reader.IsSorted() {
    val, pos, ok := reader.SkipTo(1000) // Find first value >= 1000
}

// Get all values at once
values := reader.Decode(nil)
```

### SlimReader

`SlimReader` decodes on-the-fly with minimal memory overhead per instance.
Ideal for MMAP'd data with millions of readers.

```go
reader := fastpfor.NewSlimReader()
if err := reader.Load(mmappedData); err != nil {
    return err
}

// Same API as Reader
val, err := reader.Get(5)

// Sequential iteration (O(1) per call even for delta data)
for val, pos, ok := reader.Next(); ok; val, pos, ok = reader.Next() {
    process(val)
}

// Decode all values when needed
values := reader.Decode(nil)
```

## Serialization format

The serialized binary format is:

```
Integer Block
├── Header               // 4 Bytes
│   ├── count            // 8 Bits
│   ├── bitWidth         // 6 Bits
│   ├── deltaFlag        // 1 Bit (indicates delta encoding)
│   ├── zigZagFlag       // 1 Bit (indicates zigzag encoding, used with delta)
│   ├── exceptionFlag    // 1 Bit
├── Payload              // bitWidth * 16 Bytes (interleaved lanes)
│   ├── Block 0          // 16 Bytes (4 words, one per lane)
│   │   ├── Lane 0 Word 0
│   │   ├── Lane 1 Word 0
│   │   ├── Lane 2 Word 0
│   │   ├── Lane 3 Word 0
│   ├── Block 1          // 16 Bytes
│   │   ├── Lane 0 Word 1
│   │   ├── ...
│   ├── ... (bitWidth blocks total)
├── Patch (if exceptionFlag set)
│   ├── exceptionCount   // 1 Byte
│   ├── Positions        // (exceptionCount * 1) Bytes
│   │   ├── pos1         // 1 Byte
│   │   ├── ... 
│   ├── svbLen           // 2 Bytes (little-endian)
│   ├── StreamVByte      // svbLen Bytes (variable-byte encoded high bits)
```

A block always holds up to 128 uint32 integers.
The bitpacked integers in the payload are rearranged before packing,
so they can make use of SSE2 SIMD instructions.
Values are split into 4 lanes, each encoding every 4th element:

```
- Lane 0: v0, v4, v8, v12 ... v124
- Lane 1: v1, v5, v9, v13 ... v125
- Lane 2: v2, v6, v10, v14 ... v126
- Lane 3: v3, v7, v11, v15 ... v127
```

Each lane produces `bitWidth` 32-bit words. The lanes are **interleaved** in the payload
in 16-byte blocks (one word from each lane per block), matching the bp128 SIMD format:

```
Byte offset:  0    4    8   12   16   20   24   28  ...
              L0W0 L1W0 L2W0 L3W0 L0W1 L1W1 L2W1 L3W1 ...
```

Where `LxWy` = Lane x, Word y.

The positions in the exception block are not lane-splitted but absolute.
Only the bits not packed in the lanes are stored in the exceptions.
The high bits are encoded using [StreamVByte](https://github.com/mhr3/streamvbyte),
a variable-byte encoding that compresses small integers efficiently.
They are later re-applied with `dst[pos] |= exc << bitWidth`.

## Build Tags

The `noasm` build tag disables all assembly optimizations, forcing pure Go implementations:

```sh
go build -tags=noasm ./...
go test -tags=noasm ./...
```

This tag is shared with the [StreamVByte](https://github.com/mhr3/streamvbyte) dependency,
so using `-tags=noasm` disables SIMD in both libraries simultaneously. This is useful for:
- Debugging
- Cross-compilation to non-amd64 platforms
- Comparing SIMD vs scalar performance

## Fuzzing
- `go test -fuzz=FuzzPackRoundTrip -fuzztime=1m ./...`
- `go test -fuzz=FuzzPackDeltaRoundTrip -fuzztime=1m ./...`
- `go test -fuzz=FuzzPackRoundTrip -fuzztime=1m -tags=noasm ./...`
- `go test -fuzz=FuzzPackDeltaRoundTrip -fuzztime=1m -tags=noasm ./...`
- `go test -fuzz=FuzzSIMDScalarByteCompatibility -fuzztime=30s`
- `go test -race ./...`

## Benchmarking
- `go test -bench=. -benchmem -benchtime=10x`
- `go test -bench=. -benchmem -benchtime=10x -tags=noasm`

## Copyright

Copyright (c) 2015-2016 robskie <mrobskie@gmail.com>

Copyright (c) 2025 Nils Diewald
