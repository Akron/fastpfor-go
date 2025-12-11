meta:
  id: streamvbyte
  title: StreamVByte Integer Compression
  endian: le
doc: "Fast byte-oriented uint32 compression with separate control/data streams for SIMD decoding. Count must be known externally."

params:
  - id: count
    type: u4
    doc: Number of integers to decode (not stored in format).

seq:
  - id: control_bytes
    type: control_byte
    repeat: expr
    repeat-expr: (count + 3) / 4
    doc: Control bytes encoding byte lengths for groups of 4 integers.

  - id: data_bytes
    size-eos: true
    doc: Packed integer data, 1-4 bytes each per control code.

types:
  control_byte:
    doc: "Encodes byte lengths for 4 integers: bits 0-1, 2-3, 4-5, 6-7. Code N means N+1 bytes."
    seq:
      - id: raw
        type: u1
    instances:
      code0:
        value: raw & 0x03
        doc: Length code for 1st integer (0-3, actual bytes = code+1).
      code1:
        value: (raw >> 2) & 0x03
        doc: Length code for 2nd integer.
      code2:
        value: (raw >> 4) & 0x03
        doc: Length code for 3rd integer.
      code3:
        value: (raw >> 6) & 0x03
        doc: Length code for 4th integer.
      len0:
        value: code0 + 1
        doc: Byte length of 1st integer (1-4).
      len1:
        value: code1 + 1
        doc: Byte length of 2nd integer (1-4).
      len2:
        value: code2 + 1
        doc: Byte length of 3rd integer (1-4).
      len3:
        value: code3 + 1
        doc: Byte length of 4th integer (1-4).
      group_data_size:
        value: len0 + len1 + len2 + len3
        doc: Total bytes for this group of 4 integers (4-16).
