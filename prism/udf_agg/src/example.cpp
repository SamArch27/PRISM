struct AggState13
{
	bool isInitialized;
	int32_t count;
};

struct CustomAggOperation13
{

	template <class STATE>
	static void Initialize(STATE &state)
	{
		state.isInitialized = false;
		state.count = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
	{
		for (idx_t i = 0; i < count; i++)
		{
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input)
	{
		if(state.isInitialized == false){
			state.isInitialized = true;
			state.count = 0;
		}
		state.count++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.isInitialized == false) {
			return;
		}
		else{
			target.isInitialized = true;
			target.count += source.count;
		}
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data)
	{
		if (state.isInitialized == false)
		{
			target = (int64_t)0;
			return;
		}
		target = state.count;
	}

	static bool IgnoreNull()
	{
		return false;
	}
};

auto ordersbycustomeraggregate = AggregateFunction::UnaryAggregate<AggState13, int32_t, int32_t, CustomAggOperation13>(LogicalType::INTEGER, LogicalType::INTEGER, FunctionNullHandling::SPECIAL_HANDLING);
ordersbycustomeraggregate.name = "ordersbycustomeraggregate";
ExtensionUtil::RegisterFunction(instance, ordersbycustomeraggregate);

// udf
CREATE MACRO OrdersByCustomer(cust_key) AS 
(SELECT "val_1" AS "result" FROM 
    LATERAL 
    (SELECT
        (SELECT count("orders"."o_orderkey") AS "count" FROM 
            orders AS "orders" WHERE "orders"."o_custkey" = "cust_key") AS "val_1") AS "let0"("val_1"));

// query
// SELECT c_custkey, OrdersByCustomerWithCustomAgg(c_custkey) FROM customer order by *;
