SELECT c_custkey, OrdersByCustomer(c_custkey) FROM customer order by *;
SELECT P_PARTKEY, PromoRevenue(P_PARTKEY) FROM part WHERE P_TYPE LIKE 'PROMO%%';
SELECT O_ORDERKEY, O_TOTALPRICE
FROM orders
WHERE VolumeCustomer(O_ORDERKEY) = 1;
SELECT DiscountedRevenue() as dr;
SELECT P_PARTKEY, MinCostSupp(P_PARTKEY)
FROM part;
SELECT S_NAME, S_SUPPKEY
FROM supplier
WHERE WaitingOrders(S_SUPPKEY) > 0;