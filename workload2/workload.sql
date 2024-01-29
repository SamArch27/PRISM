SELECT c_custkey, OrdersByCustomer2(c_custkey) FROM customer;
SELECT P_PARTKEY, PromoRevenue2(p_partkey) FROM part WHERE P_TYPE LIKE 'PROMO%%';
SELECT O_ORDERKEY, O_TOTALPRICE
FROM orders
WHERE VolumeCustomer2(o_orderkey) = 1;
SELECT DiscountedRevenue2() as dr;
SELECT P_PARTKEY, MinCostSupp2(p_partkey)
FROM part;
SELECT S_NAME, S_SUPPKEY
FROM supplier
WHERE WaitingOrders2(S_SUPPKEY) > 0;