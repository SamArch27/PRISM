SELECT c_custkey, OrdersByCustomerWithCustomAgg(c_custkey) FROM customer;
SELECT P_PARTKEY, PromoRevenueWithCustomAgg(p_partkey) FROM part WHERE P_TYPE LIKE 'PROMO%%';
SELECT O_ORDERKEY, O_TOTALPRICE
FROM orders
WHERE VolumeCustomerWithCustomAgg(O_ORDERKEY) = 1;
SELECT DiscountedRevenueWithCustomAgg() as dr;
SELECT P_PARTKEY, MinCostSuppWithCustomAgg(p_partkey)
FROM part;
SELECT S_NAME, S_SUPPKEY
FROM supplier
WHERE WaitingOrdersWithCustomAgg(S_SUPPKEY) > 0;