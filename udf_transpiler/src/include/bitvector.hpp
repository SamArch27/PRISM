#pragma once
#include "utils.hpp"

class BitVector {
public:
  BitVector(std::size_t size, bool value) : bitset(size, value) {}

  BitVector &flip() {
    bitset.flip();
    return *this;
  }

  std::size_t size() const { return bitset.size(); }

  Vec<bool>::reference operator[](std::size_t index) {
    ASSERT((index < bitset.size()), "Accessing bitvector out of range!");
    return bitset[index];
  }

  void operator|=(BitVector &other) {
    ASSERT((bitset.size() == other.size()),
           "Bitvectors must have same size for |= operation!");
    for (std::size_t i = 0; i < bitset.size(); ++i) {
      bitset[i] = bitset[i] | other[i];
    }
  }

  void operator&=(BitVector &other) {
    ASSERT((bitset.size() == other.size()),
           "Bitvectors must have same size for |= operation!");
    for (std::size_t i = 0; i < bitset.size(); ++i) {
      bitset[i] = bitset[i] & other[i];
    }
  }

private:
  Vec<bool> bitset;
};