# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.


class Bitset:
    def __init__(self, size) -> None:
        """
        Initializes a Bitset object with a given size.

        Args:
            size (int): The number of bits in the Bitset.
        """
        self.size = size
        self.bits = bytearray((0,) * ((size + 7) // 8))

    def set(self, index: int) -> None:
        """
        Sets the bit at the specified index to 1.

        Args:
            index (int): The index of the bit to be set.

        Raises:
            IndexError: If the index is out of range.
        """
        if index < 0 or index >= self.size:
            raise IndexError("Index out of range")
        byte_index = index // 8
        bit_index = index % 8
        self.bits[byte_index] |= 1 << bit_index

    def clear(self, index: int) -> None:
        """
        Sets the bit at the specified index to 0.

        Args:
            index (int): The index of the bit to be cleared.

        Raises:
            IndexError: If the index is out of range.
        """
        if index < 0 or index >= self.size:
            raise IndexError("Index out of range")
        self._resize_if_needed(index)
        byte_index = index // 8
        bit_index = index % 8
        self.bits[byte_index] &= ~(1 << bit_index)

    def test(self, index: int) -> None:
        """
        Checks the value of the bit at the specified index.

        Args:
            index (int): The index of the bit to be checked.

        Returns:
            bool: True if the bit is set (1), False if the bit is cleared (0).

        Raises:
            IndexError: If the index is out of range.
        """
        if index < 0 or index >= self.size:
            raise IndexError("Index out of range")
        byte_index = index // 8
        bit_index = index % 8
        return bool(self.bits[byte_index] & (1 << bit_index))

    def __str__(self):
        """
        Returns a string representation of the Bitset.

        Returns:
            str: A string representation of the Bitset object, showing the binary representation of each byte.
        """
        return "".join(bin(byte)[2:].zfill(8)[::-1] for byte in self.bits)
