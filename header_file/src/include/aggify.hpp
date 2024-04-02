#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

struct StateBase {
  template <class T> static inline void CreateValue(T &value) {}

  template <class T> static inline void DestroyValue(T &value) {}

  template <class T>
  static inline void AssignValue(T &target, T new_value, bool is_initialized) {
    target = new_value;
  }

  template <typename T>
  static inline void ReadValue(Vector &result, T &arg, T &target) {
    target = arg;
  }
};

template <> void StateBase::CreateValue(Vector *&value) { value = nullptr; }

template <> void StateBase::DestroyValue(string_t &value) {
  if (!value.IsInlined()) {
    delete[] value.GetData();
  }
}

template <> void StateBase::DestroyValue(Vector *&value) {
  delete value;
  value = nullptr;
}

template <>
void StateBase::AssignValue(string_t &target, string_t new_value,
                            bool is_initialized) {
  string_t old_target = target;
  if (new_value.IsInlined()) {
    target = new_value;
  } else {
    // non-inlined string, need to allocate space for it
    auto len = new_value.GetSize();
    auto ptr = new char[len];
    memcpy(ptr, new_value.GetData(), len);

    target = string_t(ptr, len);
  }
  if (is_initialized) {
    DestroyValue(old_target);
  }
}

template <>
void StateBase::ReadValue(Vector &result, string_t &arg, string_t &target) {
  target = StringVector::AddStringOrBlob(result, arg);
}

} // namespace duckdb