meta:
  id: pfor
  title: Integer Compression
  endian: le

imports:
  - streamvbyte

doc: |
  This is a compression format for arrays of integers. 
  This definition corresponds to the implementation at https://github.com/akron/fastpfor-go.

seq:
  - id: blocks
    type: block
    repeat: eos

types:
  block:
    seq:
      - id: header
        type: header
      - id: payload
        type: payload(header.bit_width)
        size: header.payload_size
      - id: exceptions
        type: exceptions
        if: header.flag_exception

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
      flag_zigzag:
        value: (raw & (1 << 30)) != 0
        doc: Indicates if zigzag encoding was used.
      flag_exception:
        value: (raw & (1 << 31)) != 0
        doc: Indicates if an exception section follows the payload.
      payload_size:
        value: bit_width * 16
        doc: Total size of the payload in bytes (4 lanes * 4 bytes/int * bit_width/32 = 16 * bit_width).

  payload:
    params:
      - id: bit_width
        type: u1
    seq:
      - id: lanes
        size: bit_width * 4
        repeat: expr
        repeat-expr: 4
        doc: 4 SIMD-friendly lanes.

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
        type: u4
        repeat: expr
        repeat-expr: count
        doc: High bits of the exception values.



