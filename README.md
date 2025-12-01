# FastPFOR in Go

Package fastpfor implements integer encoding and decoding using
**PFOR** [1] and relying on the [FastPFOR](https://github.com/fast-pack/FastPFOR) [2] implementation,
making use of SIMD kernels provided by [robskie/bp128](github.com/robskie/bp128).

*! This is work in progress and especially the layout may change significantly in the future.*

- [1] Zukowski, M., Heman, S., Nes, N., & Boncz, P. (2006). *Super-Scalar RAM-CPU Cache Compression*.
    22nd International Conference on Data Engineering (ICDE’06), 59–59.
    https://doi.org/10.1109/ICDE.2006.150
- [2] Lemire, D., & Boytsov, L. (2015). *Decoding billions of integers per second through vectorization*.
    Software: Practice and Experience, 45(1), 1–29. https://doi.org/10.1002/spe.2203


## Installation
```sh
go generate ./internal/avo
go get github.com/akron/fastpfor-go
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
├── Patch
│   ├── exceptionCount   // 1 Byte
│   ├── Positions        // (exceptionCount * 1) Bytes
│   │   ├── pos1         // 1 Byte
│   │   ├── ... 
│   ├── Exceptions       // (exceptionCount * 4) Bytes
│   │   ├── exc1         // 4 Bytes
│   │   ├── ... 
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
Only the bits not packed in the lanes are stored in the exceptions,
so they are later re-applied with `dst[pos] |= exc << bitWidth`.

## Fuzzing
- `go test -fuzz=FuzzPackRoundTrip -fuzztime=1m ./...`
- `go test -fuzz=FuzzPackDeltaRoundTrip -fuzztime=1m ./...`
- `go test -fuzz=FuzzPackRoundTrip -fuzztime=1m -tags purego ./...`
- `go test -fuzz=FuzzPackDeltaRoundTrip -fuzztime=1m -tags purego ./...`
- `go test -race ./...`

## Benchmarking
- `go test -bench=. -benchmem -benchtime=10x`
- `go test -bench=. -benchmem -benchtime=10x -tags=purego`

## Copyright

Copyright (c) 2015-2016 robskie <mrobskie@gmail.com>

Copyright (c) 2025 Nils Diewald
