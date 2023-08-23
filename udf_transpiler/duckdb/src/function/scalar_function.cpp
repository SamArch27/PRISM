#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

FunctionLocalState::~FunctionLocalState() {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, bind_scalar_function_t bind,
                               dependency_function_t dependency, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs,
                               FunctionSideEffects side_effects, FunctionNullHandling null_handling)
    : BaseScalarFunction(std::move(name), std::move(arguments), std::move(return_type), side_effects,
                         std::move(varargs), null_handling),
      function(std::move(function)), bind(bind), init_local_state(init_local_state), dependency(dependency),
      statistics(statistics), serialize(nullptr), deserialize(nullptr), format_serialize(nullptr),
      format_deserialize(nullptr) {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, ScalarFunctionInfo &&function_info, bind_scalar_function_t bind,
                               dependency_function_t dependency, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs,
                               FunctionSideEffects side_effects, FunctionNullHandling null_handling)
    : BaseScalarFunction(std::move(name), std::move(arguments), std::move(return_type), side_effects,
                         std::move(varargs), null_handling),
      function(std::move(function)), function_info(std::move(function_info)), bind(bind), init_local_state(init_local_state), dependency(dependency),
      statistics(statistics), serialize(nullptr), deserialize(nullptr), format_serialize(nullptr),
      format_deserialize(nullptr) {
	has_scalar_funcition_info = true;
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               bind_scalar_function_t bind, dependency_function_t dependency,
                               function_statistics_t statistics, init_local_state_t init_local_state,
                               LogicalType varargs, FunctionSideEffects side_effects,
                               FunctionNullHandling null_handling)
    : ScalarFunction(string(), std::move(arguments), std::move(return_type), std::move(function), bind, dependency,
                     statistics, init_local_state, std::move(varargs), side_effects, null_handling) {
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               ScalarFunctionInfo &&function_info, bind_scalar_function_t bind, dependency_function_t dependency,
                               function_statistics_t statistics, init_local_state_t init_local_state,
                               LogicalType varargs, FunctionSideEffects side_effects,
                               FunctionNullHandling null_handling)
    : ScalarFunction(string(), std::move(arguments), std::move(return_type), std::move(function), std::move(function_info), bind, dependency,
                     statistics, init_local_state, std::move(varargs), side_effects, null_handling) {
}

bool ScalarFunction::operator==(const ScalarFunction &rhs) const {
	return name == rhs.name && arguments == rhs.arguments && return_type == rhs.return_type && varargs == rhs.varargs &&
	       bind == rhs.bind && dependency == rhs.dependency && statistics == rhs.statistics;
}

bool ScalarFunction::operator!=(const ScalarFunction &rhs) const {
	return !(*this == rhs);
}

bool ScalarFunction::Equal(const ScalarFunction &rhs) const {
	// number of types
	if (this->arguments.size() != rhs.arguments.size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->arguments.size(); ++i) {
		if (this->arguments[i] != rhs.arguments[i]) {
			return false;
		}
	}
	// return type
	if (this->return_type != rhs.return_type) {
		return false;
	}
	// varargs
	if (this->varargs != rhs.varargs) {
		return false;
	}

	return true; // they are equal
}

void ScalarFunction::NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() >= 1);
	result.Reference(input.data[0]);
}

std::string ScalarFunctionInfo::LogicalTypeIdToCppType(LogicalTypeId type) {
      switch (type) {
      case LogicalTypeId::BOOLEAN:
            return "bool";
      case LogicalTypeId::TINYINT:
            return "int8_t";
      case LogicalTypeId::SMALLINT:
            return "int16_t";
      case LogicalTypeId::INTEGER:
            return "int32_t";
      case LogicalTypeId::BIGINT:
            return "int64_t";
      case LogicalTypeId::UTINYINT:
            return "uint8_t";
      case LogicalTypeId::USMALLINT:
            return "uint16_t";
      case LogicalTypeId::UINTEGER:
            return "uint32_t";
      case LogicalTypeId::UBIGINT:
            return "uint64_t";
      case LogicalTypeId::HUGEINT:
            return "hugeint_t";
      case LogicalTypeId::FLOAT:
            return "float";
      case LogicalTypeId::DOUBLE:
            return "double";
      // case LogicalTypeId::DECIMAL:
      //       return "decimal_t";
      // case LogicalTypeId::DATE:
      //       return "date_t";
      // case LogicalTypeId::TIME:
      //       return "dtime_t";
      // case LogicalTypeId::TIMESTAMP:
      //       return "timestamp_t";
      // case LogicalTypeId::VARCHAR:
      //       return "string_t";
      // case LogicalTypeId::BLOB:
      //       return "blob_t";
      // case LogicalTypeId::INTERVAL:
      //       return "interval_t";
      // case LogicalTypeId::LIST:
      //       return "list_entry_t";
      // case LogicalTypeId::STRUCT:
      //       return "struct_t";
      // case LogicalTypeId::MAP:
      //       return "map_t";
      default:
            throw NotImplementedException("Unimplemented type for C++ code generation");
      }
}

std::string ScalarFunctionInfo::PhysicalTypeIdToCppType(PhysicalType type_id){
      switch (type_id) {
      case PhysicalType::BOOL:
            return "bool";
      case PhysicalType::INT8:
            return "int8_t";
      case PhysicalType::INT16:
            return "int16_t";
      case PhysicalType::INT32:
            return "int32_t";
      case PhysicalType::INT64:
            return "int64_t";
      case PhysicalType::UINT8:
            return "uint8_t";
      case PhysicalType::UINT16:
            return "uint16_t";
      case PhysicalType::UINT32:
            return "uint32_t";
      case PhysicalType::UINT64:
            return "uint64_t";
      case PhysicalType::INT128:
            return "hugeint_t";
      case PhysicalType::FLOAT:
            return "float";
      case PhysicalType::DOUBLE:
            return "double";
      case PhysicalType::VARCHAR:
            return "string_t";
      // case PhysicalType::STRUCT:
      //       return "struct_t";
      // case PhysicalType::LIST:
      //       return "list_entry_t";
      // case PhysicalType::MAP:
      //       return "map_t";
      default:
            throw NotImplementedException("Unimplemented type for C++ code generation");
      }
}

// TranspilerScalarFunction::TranspilerScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
// 	                          scalar_function_t function, ScalarFunctionInfo &&function_info, bind_scalar_function_t bind,
// 	                          dependency_function_t dependency, function_statistics_t statistics,
// 	                          init_local_state_t init_local_state,
// 	                          LogicalType varargs, FunctionSideEffects side_effects,
// 	                          FunctionNullHandling null_handling)
// 	: ScalarFunction(std::move(name), std::move(arguments), std::move(return_type), std::move(function), bind, dependency,
// 		statistics, init_local_state, std::move(varargs), side_effects, null_handling), function_info(std::move(function_info)) {}

// TranspilerScalarFunction::TranspilerScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function, ScalarFunctionInfo &&function_info, 
// 	                          bind_scalar_function_t bind,
// 	                          dependency_function_t dependency, function_statistics_t statistics,
// 	                          init_local_state_t init_local_state,
// 	                          LogicalType varargs, FunctionSideEffects side_effects,
// 	                          FunctionNullHandling null_handling)
// 	: ScalarFunction(std::move(arguments), std::move(return_type), std::move(function), bind, dependency,
// 		statistics, init_local_state, std::move(varargs), side_effects, null_handling), function_info(std::move(function_info)) {}

} // namespace duckdb
