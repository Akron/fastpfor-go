meta:
  id: pfor
  title: Integer Compression
  endian: le

imports:
  - streamvbyte

doc: |
  A single PFor (Patched Frame-of-Reference) compressed block.
  This is a building block for other integer compression algorithms.
  This definition corresponds to the implementation at https://github.com/akron/fastpfor-go.

seq:
  - id: header
    type: header
  - id: payload
    type: payload(header.bit_width)
    size: header.payload_size
  - id: exceptions
    type: exceptions
    if: header.flag_exception

types:
  header:
    seq:
      - id: raw
        type: u4
    instances:
      count:
        value: raw & 0xFF
        doc: Number of logical elements in the block (up to 128).
      bit_width:
        value: (raw >> 8) & 0x3F
        doc: Bit width used for packing the payload lanes.
      flag_will_overflow:
        value: (raw & (1 << 28)) != 0
        doc: Indicates the packed deltas will overflow uint32 during decode.
      flag_delta:
        value: (raw & (1 << 29)) != 0
        doc: Indicates if delta encoding was used.
      flag_zigzag:
        value: (raw & (1 << 30)) != 0
        doc: Indicates if zigzag encoding was used (only meaningful when flag_delta is set).
      flag_exception:
        value: (raw & (1 << 31)) != 0
        doc: Indicates if an exception section follows the payload.
      payload_size:
        value: bit_width * 16
        doc: Total size of the payload in bytes (4 lanes * 4 bytes/int * bit_width/32 = 16 * bit_width).

  payload:
    doc: |
      The payload contains bitpacked lane data in an interleaved format.
      Lanes are interleaved in 16-byte blocks (4 words, one per lane).
      Total size is bit_width * 16 bytes (bit_width blocks of 16 bytes each).
    params:
      - id: bit_width
        type: u1
    seq:
      - id: blocks
        type: lane_block
        repeat: expr
        repeat-expr: bit_width
        doc: Interleaved 16-byte blocks (one word from each of 4 lanes).

  lane_block:
    doc: A 16-byte block containing one word from each of the 4 lanes.
    seq:
      - id: lane_words
        type: u4
        repeat: expr
        repeat-expr: 4
        doc: Words from lane 0, 1, 2, 3 respectively.

  exceptions:
    seq:
      - id: count
        type: u1
        doc: Number of exceptions.
      - id: positions
        type: u1
        repeat: expr
        repeat-expr: count
        doc: Indices of the exceptions in the original block (0-127).
      - id: values
        type: streamvbyte(count)
        doc: High bits of the exception values, encoded using StreamVByte.



