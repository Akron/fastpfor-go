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
    encoded := fastpfor.Pack(nil, values)
    fmt.Printf("Compressed %d integers into %d bytes\n", len(values), len(encoded))

    // Decompress
    decoded := fastpfor.Unpack(nil, encoded)
    fmt.Println("Decoded:", decoded)
}
```

For sorted or time-series data, use delta encoding for better compression:

```go
// Monotonically increasing data benefits from delta encoding
timestamps := []uint32{1000, 1005, 1012, 1018, 1025, 1033, 1040, 1048}

// Compress with delta encoding (requires a scratch buffer)
scratch := make([]uint32, 128)
encoded := fastpfor.PackDelta(nil, timestamps, scratch)

// Decompress
decoded := fastpfor.UnpackDelta(nil, encoded)
```

Reuse buffers to avoid allocations in hot paths:

```go

// Pre-allocate buffers
encodeBuf := make([]byte, 0, fastpfor.MaxBlockSize())
decodeBuf := make([]uint32, 0, 128)
scratch := make([]uint32, 128)

for _, block := range blocks {
    // Reuse buffers by slicing to zero length
    encoded := fastpfor.PackDelta(encodeBuf[:0], block, scratch)
    decoded := fastpfor.UnpackDelta(decodeBuf[:0], encoded)
    // Process decoded...
}
```

## Serialization format

The serialized binary format is:

```
Integer Block
├── Header               // 4 Bytes
│   ├── count            // 8 Bits
│   ├── bitWidth         // 6 Bits
│   ├── exceptionFlag    // 1 Bit
│   ├── zigZagFlag       // 1 Bit
├── Payload
│   ├── Lane 0           // ceil(32 * bitWidth / 32) Bytes
│   ├── Lane 1
│   ├── Lane 2
│   ├── Lane 3
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
They are split into 4 lanes, each encoding every 4th element,
meaning for 128 values (starting with index 0):

```
- Lane 0: v0, v4, v8, v12 ... v124
- Lane 1: v1, v5, v9  v13 ... v125
- Lane 2: v2, v6, v10 v14 ... v126
- Lane 3: v3, v7, v11 v15 ... v127
```

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
- `go test -race ./...`

## Benchmarking
- `go test -bench=. -benchmem -benchtime=10x`
- `go test -bench=. -benchmem -benchtime=10x -tags=noasm`

## Copyright

Copyright (c) 2015-2016 robskie <mrobskie@gmail.com>

Copyright (c) 2025 Nils Diewald
