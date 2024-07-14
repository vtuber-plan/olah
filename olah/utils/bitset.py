# coding=utf-8
# Copyright 2024 XiaHan
# 
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

class Bitset:
    def __init__(self, size):
        self.size = size
        self.bits = bytearray((0, ) * ((size + 7) // 8))

    def set(self, index):
        if index < 0 or index >= self.size:
            raise IndexError("Index out of range")
        byte_index = index // 8
        bit_index = index % 8
        self.bits[byte_index] |= (1 << bit_index)

    def clear(self, index):
        if index < 0 or index >= self.size:
            raise IndexError("Index out of range")
        self._resize_if_needed(index)
        byte_index = index // 8
        bit_index = index % 8
        self.bits[byte_index] &= ~(1 << bit_index)

    def test(self, index):
        if index < 0 or index >= self.size:
            raise IndexError("Index out of range")
        byte_index = index // 8
        bit_index = index % 8
        return bool(self.bits[byte_index] & (1 << bit_index))

    def __str__(self):
        return ''.join(bin(byte)[2:].zfill(8) for byte in self.bits)
