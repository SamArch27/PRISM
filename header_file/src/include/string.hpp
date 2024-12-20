#pragma once
#include "functions.hpp"

/**
 * Length function - done
 * Can inline
 * ArrayLengthOperator
 * StringLengthOperator
 * BitStringLenOperator
 * etc
 */
#include "duckdb/common/types/bit.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "utf8proc.hpp"

namespace duckdb {
struct StringLengthOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return LengthFun::Length<TA, TR>(input);
  }
};

struct GraphemeCountOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return LengthFun::GraphemeCount<TA, TR>(input);
  }
};

struct ArrayLengthOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return input.length;
  }
};

struct ArrayLengthBinaryOperator {
  template <class TA, class TB, class TR>
  static inline TR Operation(TA input, TB dimension) {
    if (dimension != 1) {
      throw NotImplementedException(
          "array_length for dimensions other than 1 not implemented");
    }
    return input.length;
  }
};

// strlen returns the size in bytes
struct StrLenOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return input.GetSize();
  }
};

struct OctetLenOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return Bit::OctetLength(input);
  }
};

// bitlen returns the size in bits
struct BitLenOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return 8 * input.GetSize();
  }
};

// bitstringlen returns the amount of bits in a bitstring
struct BitStringLenOperator {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return Bit::BitLength(input);
  }
};
} // namespace duckdb

/**
 * substr function - done (except substring_grapheme)
 * Cannot inline
 * SubstringUnicodeOp
 */
// #include "duckdb/function/scalar/string_functions.hpp"
namespace duckdb {
struct SubstringUnicodeOp {
  static string_t Substring(Vector &result, string_t input, int64_t offset,
                            int64_t length) {
    return SubstringFun::SubstringUnicode(result, input, offset, length);
  }
};
} // namespace duckdb

/**
 * trim function - done
 * Can inline
 * TrimOperator
 * BinaryTrimOperator
 */
// #include "duckdb/core_functions/scalar/string_functions.hpp"

// #include "duckdb/common/exception.hpp"
// #include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
// #include "utf8proc.hpp"

#include <string.h>

namespace duckdb {

template <bool LTRIM, bool RTRIM> struct TrimOperator {
  template <class INPUT_TYPE, class RESULT_TYPE>
  static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
    auto data = input.GetData();
    auto size = input.GetSize();

    utf8proc_int32_t codepoint;
    auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

    // Find the first character that is not left trimmed
    idx_t begin = 0;
    if (LTRIM) {
      while (begin < size) {
        auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
        D_ASSERT(bytes > 0);
        if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
          break;
        }
        begin += bytes;
      }
    }

    // Find the last character that is not right trimmed
    idx_t end;
    if (RTRIM) {
      end = begin;
      for (auto next = begin; next < size;) {
        auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
        D_ASSERT(bytes > 0);
        next += bytes;
        if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
          end = next;
        }
      }
    } else {
      end = size;
    }

    // Copy the trimmed string
    auto target = StringVector::EmptyString(result, end - begin);
    auto output = target.GetDataWriteable();
    memcpy(output, data + begin, end - begin);

    target.Finalize();
    return target;
  }
};

static void
GetIgnoredCodepoints(string_t ignored,
                     unordered_set<utf8proc_int32_t> &ignored_codepoints) {
  auto dataptr = reinterpret_cast<const utf8proc_uint8_t *>(ignored.GetData());
  auto size = ignored.GetSize();
  idx_t pos = 0;
  while (pos < size) {
    utf8proc_int32_t codepoint;
    pos += utf8proc_iterate(dataptr + pos, size - pos, &codepoint);
    ignored_codepoints.insert(codepoint);
  }
}

template <bool LTRIM, bool RTRIM>
string_t BinaryTrimOperator(string_t input, string_t ignored, Vector &result) {
  auto data = input.GetData();
  auto size = input.GetSize();

  unordered_set<utf8proc_int32_t> ignored_codepoints;
  GetIgnoredCodepoints(ignored, ignored_codepoints);

  utf8proc_int32_t codepoint;
  auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

  // Find the first character that is not left trimmed
  idx_t begin = 0;
  if (LTRIM) {
    while (begin < size) {
      auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
      if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
        break;
      }
      begin += bytes;
    }
  }

  // Find the last character that is not right trimmed
  idx_t end;
  if (RTRIM) {
    end = begin;
    for (auto next = begin; next < size;) {
      auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
      D_ASSERT(bytes > 0);
      next += bytes;
      if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
        end = next;
      }
    }
  } else {
    end = size;
  }

  // Copy the trimmed string
  auto target = StringVector::EmptyString(result, end - begin);
  auto output = target.GetDataWriteable();
  memcpy(output, data + begin, end - begin);

  target.Finalize();
  return target;
}
} // namespace duckdb

/**
 * left right function - done
 * Can inline
 * LeftScalarFunction<LeftRightUnicode>
 * RightScalarFunction<LeftRightUnicode>
 * LeftScalarFunction<LeftRightGrapheme>
 * RightScalarFunction<LeftRightGrapheme>
 */
namespace duckdb {
struct LeftRightUnicode {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return LengthFun::Length<TA, TR>(input);
  }

  static string_t Substring(Vector &result, string_t input, int64_t offset,
                            int64_t length) {
    return SubstringFun::SubstringUnicode(result, input, offset, length);
  }
};

struct LeftRightGrapheme {
  template <class TA, class TR> static inline TR Operation(TA input) {
    return LengthFun::GraphemeCount<TA, TR>(input);
  }

  static string_t Substring(Vector &result, string_t input, int64_t offset,
                            int64_t length) {
    return SubstringFun::SubstringGrapheme(result, input, offset, length);
  }
};

template <class OP>
static string_t LeftScalarFunction(Vector &result, const string_t str,
                                   int64_t pos) {
  if (pos >= 0) {
    return OP::Substring(result, str, 1, pos);
  }

  int64_t num_characters = OP::template Operation<string_t, int64_t>(str);
  pos = MaxValue<int64_t>(0, num_characters + pos);
  return OP::Substring(result, str, 1, pos);
}

template <class OP>
static string_t RightScalarFunction(Vector &result, const string_t str,
                                    int64_t pos) {
  int64_t num_characters = OP::template Operation<string_t, int64_t>(str);
  if (pos >= 0) {
    int64_t len = MinValue<int64_t>(num_characters, pos);
    int64_t start = num_characters - len + 1;
    return OP::Substring(result, str, start, len);
  }

  int64_t len = 0;
  if (pos != std::numeric_limits<int64_t>::min()) {
    len = num_characters - MinValue<int64_t>(num_characters, -pos);
  }
  int64_t start = num_characters - len + 1;
  return OP::Substring(result, str, start, len);
}
} // namespace duckdb

/**
 * strpos / instr / position - done
 * Can inline
 * InstrOperator
 */
namespace duckdb {
struct InstrOperator {
  template <class TA, class TB, class TR>
  static inline TR Operation(TA haystack, TB needle) {
    int64_t string_position = 0;

    auto location = ContainsFun::Find(haystack, needle);
    if (location != DConstants::INVALID_INDEX) {
      auto len = (utf8proc_ssize_t)location;
      auto str = reinterpret_cast<const utf8proc_uint8_t *>(haystack.GetData());
      D_ASSERT(len <= (utf8proc_ssize_t)haystack.GetSize());
      for (++string_position; len > 0; ++string_position) {
        utf8proc_int32_t codepoint;
        auto bytes = utf8proc_iterate(str, len, &codepoint);
        str += bytes;
        len -= bytes;
      }
    }
    return string_position;
  }
};
} // namespace duckdb

/**
 * concat function - support only: || (not concat, concat_ws)
 * Can inline
 *
 */
namespace duckdb {
string_t ConcatOperator(Vector &result, string_t left, string_t right) {
  auto left_data = left.GetData();
  auto right_data = right.GetData();
  auto left_size = left.GetSize();
  auto right_size = right.GetSize();

  auto target = StringVector::EmptyString(result, left_size + right_size);
  auto target_data = target.GetDataWriteable();
  memcpy(target_data, left_data, left_size);
  memcpy(target_data + left_size, right_data, right_size);

  target.Finalize();
  return target;
}

// PrefixOperator
static bool PrefixFunction(const string_t &str, const string_t &pattern) {
  auto str_length = str.GetSize();
  auto patt_length = pattern.GetSize();
  if (patt_length > str_length) {
    return false;
  }
  if (patt_length <= string_t::PREFIX_LENGTH) {
    // short prefix
    if (patt_length == 0) {
      // length = 0, return true
      return true;
    }

    // prefix early out
    const char *str_pref = str.GetPrefix();
    const char *patt_pref = pattern.GetPrefix();
    for (idx_t i = 0; i < patt_length; ++i) {
      if (str_pref[i] != patt_pref[i]) {
        return false;
      }
    }
    return true;
  } else {
    // prefix early out
    const char *str_pref = str.GetPrefix();
    const char *patt_pref = pattern.GetPrefix();
    for (idx_t i = 0; i < string_t::PREFIX_LENGTH; ++i) {
      if (str_pref[i] != patt_pref[i]) {
        // early out
        return false;
      }
    }
    // compare the rest of the prefix
    const char *str_data = str.GetData();
    const char *patt_data = pattern.GetData();
    D_ASSERT(patt_length <= str_length);
    for (idx_t i = string_t::PREFIX_LENGTH; i < patt_length; ++i) {
      if (str_data[i] != patt_data[i]) {
        return false;
      }
    }
    return true;
  }
}
struct PrefixOperator {
  template <class TA, class TB, class TR>
  static inline TR Operation(TA left, TB right) {
    return PrefixFunction(left, right);
  }
};
} // namespace duckdb
