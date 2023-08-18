//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/plan_serialization.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct FunctionLocalState {
	DUCKDB_API virtual ~FunctionLocalState();

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class Binder;
class BoundFunctionExpression;
class DependencyList;
class ScalarFunctionCatalogEntry;

struct FunctionStatisticsInput {
	FunctionStatisticsInput(BoundFunctionExpression &expr_p, optional_ptr<FunctionData> bind_data_p,
	                        vector<BaseStatistics> &child_stats_p, unique_ptr<Expression> *expr_ptr_p)
	    : expr(expr_p), bind_data(bind_data_p), child_stats(child_stats_p), expr_ptr(expr_ptr_p) {
	}

	BoundFunctionExpression &expr;
	optional_ptr<FunctionData> bind_data;
	vector<BaseStatistics> &child_stats;
	unique_ptr<Expression> *expr_ptr;
};

//! The type used for scalar functions
typedef std::function<void(DataChunk &, ExpressionState &, Vector &)> scalar_function_t;
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_scalar_function_t)(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments);
typedef unique_ptr<FunctionLocalState> (*init_local_state_t)(ExpressionState &state,
                                                             const BoundFunctionExpression &expr,
                                                             FunctionData *bind_data);
typedef unique_ptr<BaseStatistics> (*function_statistics_t)(ClientContext &context, FunctionStatisticsInput &input);
//! Adds the dependencies of this BoundFunctionExpression to the set of dependencies
typedef void (*dependency_function_t)(BoundFunctionExpression &expr, DependencyList &dependencies);

typedef void (*function_serialize_t)(FieldWriter &writer, const FunctionData *bind_data,
                                     const ScalarFunction &function);
typedef unique_ptr<FunctionData> (*function_deserialize_t)(PlanDeserializationState &state, FieldReader &reader,
                                                           ScalarFunction &function);

typedef void (*function_format_serialize_t)(FormatSerializer &serializer, const optional_ptr<FunctionData> bind_data,
                                            const ScalarFunction &function);
typedef unique_ptr<FunctionData> (*function_format_deserialize_t)(FormatDeserializer &deserializer,
                                                                  ScalarFunction &function);

/**
 * This is used to register the SCALAR scalar functions
*/
class ScalarFunctionInfo{
public:
enum SpecialValueHandling : uint8_t 
				{BinaryNumericDivideWrapper, 
				BinaryZeroIsNullWrapper, 
				BinaryZeroIsNullHugeintWrapper, 
				VectorFrontWrapper,						// insert Vector& result as the first argument to the function
				VectorBackWrapper,						// append Vector& result as the last argument to the function
				SubStringAutoLengthWrapper				// auto add the length of the string
														// as the thrird argument
				};
public:
    /**
     * function name in the header file 
    */
    std::string cpp_name;
    /**
     * if the function definition is templated (not used)
    */
    bool templated = false;
	std::vector<std::string> template_args;
    /**
     * if use the default null handling method which is pass NULL return NULL
	 * udf_todo: implement the specific rule for each type
    */
    // bool default_null = true;
	std::vector<SpecialValueHandling> special_handling;
	/**
	 * if the function is a switch function (not used)
	*/
    bool if_switch = false;
	/**
	 * if the function is a string function (not used)
	*/
    bool if_string = false;
    /**
     * length of input_type should be the same as return_type because they are 
     * one to one mapping
    */
    // vector<vector<LogicalType>> input_type;
    // vector<LogicalType> return_type;
    DUCKDB_API ScalarFunctionInfo(){}
	DUCKDB_API ScalarFunctionInfo(std::string cpp_name) : cpp_name(cpp_name) {}
	// DUCKDB_API ScalarFunctionInfo(std::string cpp_name, bool if_string) : cpp_name(cpp_name), if_string(if_string) {}
	DUCKDB_API ScalarFunctionInfo(std::string cpp_name, std::vector<std::string> template_args) : cpp_name(cpp_name), templated(true), template_args(template_args) {}
	// DUCKDB_API ScalarFunctionInfo(std::string cpp_name, std::vector<std::string> template_args, b) : cpp_name(cpp_name), templated(true), template_args(template_args), if_string(if_string) {}
	DUCKDB_API ScalarFunctionInfo(std::string cpp_name, std::vector<SpecialValueHandling> special_handling) : cpp_name(cpp_name), special_handling(special_handling) {}
	// DUCKDB_API ScalarFunctionInfo(std::string cpp_name, std::vector<SpecialValueHandling> special_handling, bool if_string) : cpp_name(cpp_name), special_handling(special_handling), if_string(if_string) {}
	DUCKDB_API ScalarFunctionInfo(std::string cpp_name, std::vector<std::string> template_args, std::vector<SpecialValueHandling> special_handling) : cpp_name(cpp_name), templated(true), template_args(template_args), special_handling(special_handling) {}
	// DUCKDB_API ScalarFunctionInfo(string cpp_name, bool templated = false, bool if_switch = false, bool default_null = true, bool if_string = false):
    //                         cpp_name(cpp_name), templated(templated), default_null(default_null), if_switch(if_switch), if_string(if_string){}
	DUCKDB_API ScalarFunctionInfo &operator=(const ScalarFunctionInfo &other) {
		cpp_name = other.cpp_name;
		templated = other.templated;
		template_args = other.template_args;
		// default_null = other.default_null;
		special_handling = other.special_handling;
		if_switch = other.if_switch;
		if_string = other.if_string;
		return *this;
	}
	DUCKDB_API ScalarFunctionInfo &operator=(ScalarFunctionInfo &&other) {
		cpp_name = std::move(other.cpp_name);
		templated = other.templated;
		template_args = std::move(other.template_args);
		// default_null = other.default_null;
		special_handling = std::move(other.special_handling);
		if_switch = other.if_switch;
		if_string = other.if_string;
		return *this;
	}
	DUCKDB_API ScalarFunctionInfo(ScalarFunctionInfo &&other){
		*this = std::move(other);
	}
	DUCKDB_API ScalarFunctionInfo(const ScalarFunctionInfo &other){
		*this = other;
	}

	/**
	 * get the string representation of the function
	*/
	DUCKDB_API std::string str(){
		std::string ret = cpp_name;
		if(template_args.size() > 0){
			ret += "<";
			for(auto &arg : template_args){
				ret += arg + ", ";
			}
			ret = ret.substr(0, ret.size()-2);
			ret += ">";
		}
		// if(if_string) ret += "(string)";
		return ret;
	}
};

class ScalarFunction : public BaseScalarFunction {
public:
	DUCKDB_API ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
	                          scalar_function_t function, bind_scalar_function_t bind = nullptr,
	                          dependency_function_t dependency = nullptr, function_statistics_t statistics = nullptr,
	                          init_local_state_t init_local_state = nullptr,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
	                          FunctionSideEffects side_effects = FunctionSideEffects::NO_SIDE_EFFECTS,
	                          FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING);
	
	DUCKDB_API ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
	                          scalar_function_t function, ScalarFunctionInfo &&function_info, bind_scalar_function_t bind = nullptr,
	                          dependency_function_t dependency = nullptr, function_statistics_t statistics = nullptr,
	                          init_local_state_t init_local_state = nullptr,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
	                          FunctionSideEffects side_effects = FunctionSideEffects::NO_SIDE_EFFECTS,
	                          FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING);


	DUCKDB_API ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	                          bind_scalar_function_t bind = nullptr, dependency_function_t dependency = nullptr,
	                          function_statistics_t statistics = nullptr, init_local_state_t init_local_state = nullptr,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
	                          FunctionSideEffects side_effects = FunctionSideEffects::NO_SIDE_EFFECTS,
	                          FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING);
							
	DUCKDB_API ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
							  ScalarFunctionInfo &&function_info, 
	                          bind_scalar_function_t bind = nullptr, dependency_function_t dependency = nullptr,
	                          function_statistics_t statistics = nullptr, init_local_state_t init_local_state = nullptr,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
	                          FunctionSideEffects side_effects = FunctionSideEffects::NO_SIDE_EFFECTS,
	                          FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING);					

	// ScalarFunction(const ScalarFunction &other): function(other.function), bind(other.bind), init_local_state(other.init_local_state), 
	// 	dependency(other.dependency), statistics(other.statistics), serialize(other.serialize), deserialize(other.deserialize), 
	// 	format_serialize(other.format_serialize), format_deserialize(other.format_deserialize), function_info(other.function_info), 
	// 	has_scalar_funcition_info(other.has_scalar_funcition_info), BaseScalarFunction(other) {}
	//! The main scalar function to execute
	scalar_function_t function;
	//! The bind function (if any)
	bind_scalar_function_t bind;
	//! Init thread local state for the function (if any)
	init_local_state_t init_local_state;
	//! The dependency function (if any)
	dependency_function_t dependency;
	//! The statistics propagation function (if any)
	function_statistics_t statistics;

	function_serialize_t serialize;
	function_deserialize_t deserialize;

	function_format_serialize_t format_serialize;
	function_format_deserialize_t format_deserialize;

	bool has_scalar_funcition_info = false;
	ScalarFunctionInfo function_info;

	DUCKDB_API bool operator==(const ScalarFunction &rhs) const;
	DUCKDB_API bool operator!=(const ScalarFunction &rhs) const;

	DUCKDB_API bool Equal(const ScalarFunction &rhs) const;

public:

	DUCKDB_API static void NopFunction(DataChunk &input, ExpressionState &state, Vector &result);

	template <class TA, class TR, class OP>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() >= 1);
		UnaryExecutor::Execute<TA, TR, OP>(input.data[0], result, input.size());
	}

	template <class TA, class TB, class TR, class OP>
	static void BinaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() == 2);
		BinaryExecutor::ExecuteStandard<TA, TB, TR, OP>(input.data[0], input.data[1], result, input.size());
	}

	template <class TA, class TB, class TC, class TR, class OP>
	static void TernaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() == 3);
		TernaryExecutor::ExecuteStandard<TA, TB, TC, TR, OP>(input.data[0], input.data[1], input.data[2], result,
		                                                     input.size());
	}

public:
	template <class OP>
	static scalar_function_t GetScalarUnaryFunction(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, int8_t, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, int16_t, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, int32_t, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, int64_t, OP>;
			break;
		case LogicalTypeId::UTINYINT:
			function = &ScalarFunction::UnaryFunction<uint8_t, uint8_t, OP>;
			break;
		case LogicalTypeId::USMALLINT:
			function = &ScalarFunction::UnaryFunction<uint16_t, uint16_t, OP>;
			break;
		case LogicalTypeId::UINTEGER:
			function = &ScalarFunction::UnaryFunction<uint32_t, uint32_t, OP>;
			break;
		case LogicalTypeId::UBIGINT:
			function = &ScalarFunction::UnaryFunction<uint64_t, uint64_t, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, hugeint_t, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, float, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, double, OP>;
			break;
		default:
			throw InternalException("Unimplemented type for GetScalarUnaryFunction");
		}
		return function;
	}

	static void GetScalarUnaryFunctionInfoTemplate(LogicalType type, std::vector<string> &template_args) {
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			template_args = {"int8_t", "int8_t"};
			break;
		case LogicalTypeId::SMALLINT:
			template_args = {"int16_t", "int16_t"};
			break;
		case LogicalTypeId::INTEGER:
			template_args = {"int32_t", "int32_t"};
			break;
		case LogicalTypeId::BIGINT:
			template_args = {"int64_t", "int64_t"};
			break;
		case LogicalTypeId::UTINYINT:
			template_args = {"uint8_t", "uint8_t"};
			break;
		case LogicalTypeId::USMALLINT:
			template_args = {"uint16_t", "uint16_t"};
			break;
		case LogicalTypeId::UINTEGER:
			template_args = {"uint32_t", "uint32_t"};
			break;
		case LogicalTypeId::UBIGINT:
			template_args = {"uint64_t", "uint64_t"};
			break;
		case LogicalTypeId::HUGEINT:
			template_args = {"hugeint_t", "hugeint_t"};
			break;
		case LogicalTypeId::FLOAT:
			template_args = {"float", "float"};
			break;
		case LogicalTypeId::DOUBLE:
			template_args = {"double", "double"};
			break;
		default:
			throw InternalException("Unimplemented type for GetScalarUnaryFunction");
		}
	}

	template <class TR, class OP>
	static scalar_function_t GetScalarUnaryFunctionFixedReturn(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
			break;
		case LogicalTypeId::UTINYINT:
			function = &ScalarFunction::UnaryFunction<uint8_t, TR, OP>;
			break;
		case LogicalTypeId::USMALLINT:
			function = &ScalarFunction::UnaryFunction<uint16_t, TR, OP>;
			break;
		case LogicalTypeId::UINTEGER:
			function = &ScalarFunction::UnaryFunction<uint32_t, TR, OP>;
			break;
		case LogicalTypeId::UBIGINT:
			function = &ScalarFunction::UnaryFunction<uint64_t, TR, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, TR, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, TR, OP>;
			break;
		default:
			throw InternalException("Unimplemented type for GetScalarUnaryFunctionFixedReturn");
		}
		return function;
	}
};

// class TranspilerScalarFunction : public ScalarFunction {
// public:
// 	ScalarFunctionInfo function_info;
// 	DUCKDB_API TranspilerScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
// 	                          scalar_function_t function, ScalarFunctionInfo &&function_info, bind_scalar_function_t bind = nullptr,
// 	                          dependency_function_t dependency = nullptr, function_statistics_t statistics = nullptr,
// 	                          init_local_state_t init_local_state = nullptr,
// 	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
// 	                          FunctionSideEffects side_effects = FunctionSideEffects::NO_SIDE_EFFECTS,
// 	                          FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING);

// 	DUCKDB_API TranspilerScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function, ScalarFunctionInfo &&function_info, 
// 	                          bind_scalar_function_t bind = nullptr, dependency_function_t dependency = nullptr,
// 	                          function_statistics_t statistics = nullptr, init_local_state_t init_local_state = nullptr,
// 	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
// 	                          FunctionSideEffects side_effects = FunctionSideEffects::NO_SIDE_EFFECTS,
// 	                          FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING);
// };

} // namespace duckdb
