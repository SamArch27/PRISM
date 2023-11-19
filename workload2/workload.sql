SELECT c_custkey, OrdersByCustomer(c_custkey) FROM customer;
SELECT P_PARTKEY, PromoRevenue(p_partkey) FROM part WHERE P_TYPE LIKE 'PROMO%%';
SELECT O_ORDERKEY, O_TOTALPRICE
FROM orders
WHERE VolumeCustomer(o_orderkey) = 1;
SELECT DiscountedRevenue() as dr;
SELECT P_PARTKEY, MinCostSupp(p_partkey)
FROM part;
SELECT S_NAME, S_SUPPKEY
FROM supplier
WHERE WaitingOrders(S_SUPPKEY) > 0;