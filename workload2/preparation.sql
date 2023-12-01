CREATE MACRO OrdersByCustomer(cust_key) AS 
(SELECT "val_1" AS "result" FROM 
    LATERAL 
    (SELECT
        (SELECT count("orders"."o_orderkey") AS "count" FROM 
            orders AS "orders" WHERE "orders"."o_custkey" = "cust_key") AS "val_1") AS "let0"("val_1"));

CREATE MACRO OrdersByCustomer2(cust_key) AS 
(SELECT "val_2" AS "result"
    FROM LATERAL
         (SELECT "cust_key" AS "custkey_1") AS "let0"("custkey_1"), 
         LATERAL (SELECT 0 AS "val_1") AS "let1"("val_1"), 
         LATERAL
         (SELECT (SELECT CASE WHEN (count(*)) > 0 THEN ordersbycustomeraggregate("s"."o_orderkey", "val_1")
                              ELSE "val_1"
                         END AS "val"
                  FROM 
                       (SELECT "orders"."o_orderkey" AS "o_orderkey"
                        FROM orders AS "orders"
                        WHERE "orders"."o_custkey" = "custkey_1"
                       ) AS "s"("o_orderkey")) AS "val_2"
         ) AS "let2"("val_2"));

create macro PromoRevenue(partkey) AS
(SELECT "revenue_1" AS "result"
    FROM LATERAL
         (SELECT (SELECT sum("lineitem"."l_extendedprice")-
                          sum("lineitem"."l_extendedprice"*"lineitem"."l_discount") AS "row"
                  FROM lineitem AS "lineitem"
                  WHERE "lineitem"."l_partkey" = "partkey") AS "revenue_1"
         ) AS "let0"("revenue_1"));

create macro PromoRevenue2(partkey) AS
(SELECT "revenue_2" AS "result"
    FROM LATERAL (SELECT "partkey" AS "pkey_1") AS "let0"("pkey_1"), 
         LATERAL (SELECT 0 AS "revenue_1") AS "let1"("revenue_1"), 
         LATERAL
         (SELECT (SELECT CASE WHEN (count(*))
                                   >
                                   0 THEN promo_revenue_agg("tmp"."l_extendedprice"::bigint, "tmp"."l_discount"::bigint
                                              ,
                                              "revenue_1"::bigint)
                              ELSE "revenue_1"
                         END AS "revenue"
                  FROM 
                       (SELECT "lineitem"."l_extendedprice" AS "l_extendedprice", 
                               "lineitem"."l_discount" AS "l_discount"
                        FROM lineitem AS "lineitem"
                        WHERE "lineitem"."l_partkey" = "pkey_1"
                       ) AS "tmp"("l_extendedprice", "l_discount")) AS "revenue_2"
         ) AS "let2"("revenue_2"));

create macro VolumeCustomer(orderkey) AS
(SELECT "ifresult4".*
    FROM LATERAL (SELECT "orderkey" AS "ok_1") AS "let0"("ok_1"), 
         LATERAL (SELECT 0 AS "i_1") AS "let1"("i_1"), 
         LATERAL
         (SELECT (SELECT sum("lineitem"."l_quantity"::INT) AS "count"
                  FROM lineitem AS "lineitem"
                  WHERE "lineitem"."l_orderkey" = "ok_1") AS "d_2"
         ) AS "let2"("d_2"), 
         LATERAL (SELECT "d_2" > (300) AS "q4_1") AS "let3"("q4_1"), 
         LATERAL
         ((SELECT "i_3" AS "result"
           FROM LATERAL (SELECT 1 AS "i_3") AS "let5"("i_3")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "i_1" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult4");

create macro VolumeCustomer2(orderkey) AS
(SELECT "ifresult5".*
    FROM LATERAL (SELECT "orderkey" AS "ok_1") AS "let0"("ok_1"), 
         LATERAL (SELECT 0 AS "i_1") AS "let1"("i_1"), 
         LATERAL (SELECT 0 AS "d_1") AS "let2"("d_1"), 
         LATERAL
         (SELECT (SELECT CASE WHEN (count(*)) > 0 THEN volume_customer_agg("tmp"."l_quantity"::int
                                                           ,
                                                           "d_1")
                              ELSE "d_1"
                         END AS "d"
                  FROM 
                       (SELECT "lineitem"."l_quantity" AS "l_quantity"
                        FROM lineitem AS "lineitem"
                        WHERE "lineitem"."l_orderkey" = "ok_1"
                       ) AS "tmp"("l_quantity")) AS "d_2"
         ) AS "let3"("d_2"), 
         LATERAL (SELECT "d_2" > (300) AS "q4_1") AS "let4"("q4_1"), 
         LATERAL
         ((SELECT "i_3" AS "result"
           FROM LATERAL (SELECT 1 AS "i_3") AS "let6"("i_3")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "i_1" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult5");

create macro DiscountedRevenue() as
(SELECT (SELECT sum("lineitem"."l_extendedprice")- 
                    sum("lineitem"."l_extendedprice"*"lineitem"."l_discount") AS "row"
            FROM lineitem AS "lineitem", part AS "part"
            WHERE (("part"."p_partkey" = "lineitem"."l_partkey"
                    AND
                    "part"."p_brand" = 'Brand#12'
                    AND
                    "part"."p_container" = ANY (ARRAY['SM CASE', 
                                                      'SM BOX', 
                                                      'SM PACK', 
                                                      'SM PKG'] :: bpchar[])
                    AND
                    ("lineitem"."l_quantity" >= (1) AND "lineitem"."l_quantity" <= (11))
                    AND
                    ("part"."p_size" >= 1 AND "part"."p_size" <= 5)
                    AND
                    "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 'AIR REG'] :: bpchar[])
                    AND
                    "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON')
                   OR
                   ("part"."p_partkey" = "lineitem"."l_partkey"
                    AND
                    "part"."p_brand" = 'Brand#23'
                    AND
                    "part"."p_container" = ANY (ARRAY['MED BAG', 
                                                      'MED BOX', 
                                                      'MED PKG', 
                                                      'MED PACK'] :: bpchar[])
                    AND
                    ("lineitem"."l_quantity" >= (10) AND "lineitem"."l_quantity" <= (20))
                    AND
                    ("part"."p_size" >= 1 AND "part"."p_size" <= 10)
                    AND
                    "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 'AIR REG'] :: bpchar[])
                    AND
                    "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON')
                   OR
                   ("part"."p_partkey" = "lineitem"."l_partkey"
                    AND
                    "part"."p_brand" = 'Brand#34'
                    AND
                    "part"."p_container" = ANY (ARRAY['LG CASE', 
                                                      'LG BOX', 
                                                      'LG PACK', 
                                                      'LG PKG'] :: bpchar[])
                    AND
                    ("lineitem"."l_quantity" >= (20) AND "lineitem"."l_quantity" <= (30))
                    AND
                    ("part"."p_size" >= 1 AND "part"."p_size" <= 15)
                    AND
                    "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 'AIR REG'] :: bpchar[])
                    AND
                    "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'))) AS "result");

create macro DiscountedRevenue2() as
(SELECT "val_2" AS "result"
    FROM LATERAL (SELECT 0 AS "val_1") AS "let0"("val_1"), 
         LATERAL
         (SELECT (SELECT CASE WHEN (count(*))
                                   >
                                   0 THEN sum_discounted_price("tmp"."l_extendedprice"::int, "tmp"."l_discount"::int
                                              ,
                                              "val_1"::int)
                              ELSE "val_1"
                         END AS "val"
                  FROM 
                       (SELECT "lineitem"."l_extendedprice" AS "l_extendedprice", 
                               "lineitem"."l_discount" AS "l_discount"
                        FROM lineitem AS "lineitem", part AS "part"
                        WHERE (("part"."p_partkey" = "lineitem"."l_partkey"
                                AND
                                "part"."p_brand" = 'Brand#12'
                                AND
                                "part"."p_container" = ANY (ARRAY['SM CASE', 
                                                                  'SM BOX', 
                                                                  'SM PACK', 
                                                                  'SM PKG'] :: bpchar[])
                                AND
                                ("lineitem"."l_quantity" >= (1) AND "lineitem"."l_quantity" <= (11))
                                AND
                                ("part"."p_size" >= 1 AND "part"."p_size" <= 5)
                                AND
                                "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 'AIR REG'] :: bpchar[])
                                AND
                                "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON')
                               OR
                               ("part"."p_partkey" = "lineitem"."l_partkey"
                                AND
                                "part"."p_brand" = 'Brand#23'
                                AND
                                "part"."p_container" = ANY (ARRAY['MED BAG', 
                                                                  'MED BOX', 
                                                                  'MED PKG', 
                                                                  'MED PACK'] :: bpchar[])
                                AND
                                ("lineitem"."l_quantity" >= (10)
                                 AND
                                 "lineitem"."l_quantity" <= (20))
                                AND
                                ("part"."p_size" >= 1 AND "part"."p_size" <= 10)
                                AND
                                "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 'AIR REG'] :: bpchar[])
                                AND
                                "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON')
                               OR
                               ("part"."p_partkey" = "lineitem"."l_partkey"
                                AND
                                "part"."p_brand" = 'Brand#34'
                                AND
                                "part"."p_container" = ANY (ARRAY['LG CASE', 
                                                                  'LG BOX', 
                                                                  'LG PACK', 
                                                                  'LG PKG'] :: bpchar[])
                                AND
                                ("lineitem"."l_quantity" >= (20)
                                 AND
                                 "lineitem"."l_quantity" <= (30))
                                AND
                                ("part"."p_size" >= 1 AND "part"."p_size" <= 15)
                                AND
                                "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 'AIR REG'] :: bpchar[])
                                AND
                                "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'))
                       ) AS "tmp"("l_extendedprice", "l_discount")) AS "val_2"
         ) AS "let1"("val_2"));

create macro MinCostSupp(pk) AS
(SELECT "val_1" AS "result"
    FROM LATERAL
         (SELECT (SELECT arg_min("supplier"."s_name", 
                                "partsupp"."ps_supplycost"::int) AS "row"
                  FROM partsupp AS "partsupp", supplier AS "supplier"
                  WHERE ("partsupp"."ps_partkey" = "pk"
                         AND
                         "partsupp"."ps_suppkey" = "supplier"."s_suppkey")) AS "val_1"
         ) AS "let0"("val_1"));

create macro MinCostSupp2(pk) AS
(SELECT "val_1" AS "result"
    FROM LATERAL (SELECT "pk" AS "key_1") AS "let0"("key_1"), 
         LATERAL (SELECT NULL AS "val_tmp") AS "tmp"("val_tmp"),
         LATERAL (SELECT 100000 AS "pmin_cost_1") AS "let1"("pmin_cost_1"), 
         LATERAL
         (SELECT (SELECT CASE WHEN (count(*))
                                   >
                                   0 THEN MinCostSuppAggregate("tmp"."ps_supplycost"::int, "tmp"."s_name",
                                               "pmin_cost_1")
                              ELSE "val_tmp"
                         END AS "val"
                  FROM 
                       (SELECT "partsupp"."ps_supplycost" AS "ps_supplycost", 
                               "supplier"."s_name" AS "s_name"
                        FROM partsupp AS "partsupp", supplier AS "supplier"
                        WHERE ("partsupp"."ps_partkey" = "key_1"
                               AND
                               "partsupp"."ps_suppkey" = "supplier"."s_suppkey")
                       ) AS "tmp"("ps_supplycost", "s_name")) AS "val_1"
         ) AS "let2"("val_1"));

create macro WaitingOrders(suppkey) AS
(SELECT "val_2" AS "result"
    FROM LATERAL (SELECT "suppkey" AS "skey_1") AS "let0"("skey_1"), 
         LATERAL
         (SELECT (SELECT count("q"."ok") AS "ok"
                  FROM 
                       (SELECT "l1"."l_orderkey" AS "ok", 
                               "l1"."l_partkey" AS "pk", 
                               "l1"."l_suppkey" AS "sk", 
                               "l1"."l_linenumber" AS "ln"
                        FROM lineitem AS "l1", orders AS "orders"
                        WHERE ("l1"."l_suppkey" = "skey_1"
                               AND
                               "orders"."o_orderkey" = "l1"."l_orderkey"
                               AND
                               "orders"."o_orderstatus" = 'F'
                               AND
                               "l1"."l_receiptdate" > "l1"."l_commitdate"
                               AND
                               EXISTS (SELECT "l2"."l_orderkey" AS "l_orderkey", 
                                              "l2"."l_partkey" AS "l_partkey", 
                                              "l2"."l_suppkey" AS "l_suppkey", 
                                              "l2"."l_linenumber" AS "l_linenumber", 
                                              "l2"."l_quantity" AS "l_quantity", 
                                              "l2"."l_extendedprice" AS "l_extendedprice", 
                                              "l2"."l_discount" AS "l_discount", 
                                              "l2"."l_tax" AS "l_tax", 
                                              "l2"."l_returnflag" AS "l_returnflag", 
                                              "l2"."l_linestatus" AS "l_linestatus", 
                                              "l2"."l_shipdate" AS "l_shipdate", 
                                              "l2"."l_commitdate" AS "l_commitdate", 
                                              "l2"."l_receiptdate" AS "l_receiptdate", 
                                              "l2"."l_shipinstruct" AS "l_shipinstruct", 
                                              "l2"."l_shipmode" AS "l_shipmode", 
                                              "l2"."l_comment" AS "l_comment"
                                       FROM lineitem AS "l2"
                                       WHERE ("l2"."l_orderkey" = "l1"."l_orderkey"
                                              AND
                                              "l2"."l_suppkey" <> "l1"."l_suppkey"))
                               AND
                               NOT EXISTS (SELECT "l3"."l_orderkey" AS "l_orderkey", 
                                                  "l3"."l_partkey" AS "l_partkey", 
                                                  "l3"."l_suppkey" AS "l_suppkey", 
                                                  "l3"."l_linenumber" AS "l_linenumber", 
                                                  "l3"."l_quantity" AS "l_quantity", 
                                                  "l3"."l_extendedprice" AS "l_extendedprice", 
                                                  "l3"."l_discount" AS "l_discount", 
                                                  "l3"."l_tax" AS "l_tax", 
                                                  "l3"."l_returnflag" AS "l_returnflag", 
                                                  "l3"."l_linestatus" AS "l_linestatus", 
                                                  "l3"."l_shipdate" AS "l_shipdate", 
                                                  "l3"."l_commitdate" AS "l_commitdate", 
                                                  "l3"."l_receiptdate" AS "l_receiptdate", 
                                                  "l3"."l_shipinstruct" AS "l_shipinstruct", 
                                                  "l3"."l_shipmode" AS "l_shipmode", 
                                                  "l3"."l_comment" AS "l_comment"
                                           FROM lineitem AS "l3"
                                           WHERE ("l3"."l_orderkey" = "l1"."l_orderkey"
                                                  AND
                                                  "l3"."l_suppkey" <> "l1"."l_suppkey"
                                                  AND
                                                  "l3"."l_receiptdate" > "l3"."l_commitdate")))
                       ) AS "q"("ok", "pk", "sk", "ln")) AS "val_2"
         ) AS "let1"("val_2"));

create macro WaitingOrders2(suppkey) AS
(SELECT "val_2" AS "result"
    FROM LATERAL (SELECT "suppkey" AS "skey_1") AS "let0"("skey_1"), 
         LATERAL (SELECT 0 AS "val_1") AS "let1"("val_1"), 
         LATERAL
         (SELECT (SELECT CASE WHEN (count(*)) > 0 THEN ordersbycustomeraggregate("q"."ok", "val_1")
                              ELSE "val_1"
                         END AS "val"
                  FROM 
                       (SELECT "l1"."l_orderkey" AS "ok", 
                               "l1"."l_partkey" AS "pk", 
                               "l1"."l_suppkey" AS "sk", 
                               "l1"."l_linenumber" AS "ln"
                        FROM lineitem AS "l1", orders AS "orders"
                        WHERE ("l1"."l_suppkey" = "skey_1"
                               AND
                               "orders"."o_orderkey" = "l1"."l_orderkey"
                               AND
                               "orders"."o_orderstatus" = 'F'
                               AND
                               "l1"."l_receiptdate" > "l1"."l_commitdate"
                               AND
                               EXISTS (SELECT "l2"."l_orderkey" AS "l_orderkey", 
                                              "l2"."l_partkey" AS "l_partkey", 
                                              "l2"."l_suppkey" AS "l_suppkey", 
                                              "l2"."l_linenumber" AS "l_linenumber", 
                                              "l2"."l_quantity" AS "l_quantity", 
                                              "l2"."l_extendedprice" AS "l_extendedprice", 
                                              "l2"."l_discount" AS "l_discount", 
                                              "l2"."l_tax" AS "l_tax", 
                                              "l2"."l_returnflag" AS "l_returnflag", 
                                              "l2"."l_linestatus" AS "l_linestatus", 
                                              "l2"."l_shipdate" AS "l_shipdate", 
                                              "l2"."l_commitdate" AS "l_commitdate", 
                                              "l2"."l_receiptdate" AS "l_receiptdate", 
                                              "l2"."l_shipinstruct" AS "l_shipinstruct", 
                                              "l2"."l_shipmode" AS "l_shipmode", 
                                              "l2"."l_comment" AS "l_comment"
                                       FROM lineitem AS "l2"
                                       WHERE ("l2"."l_orderkey" = "l1"."l_orderkey"
                                              AND
                                              "l2"."l_suppkey" <> "l1"."l_suppkey"))
                               AND
                               NOT EXISTS (SELECT "l3"."l_orderkey" AS "l_orderkey", 
                                                  "l3"."l_partkey" AS "l_partkey", 
                                                  "l3"."l_suppkey" AS "l_suppkey", 
                                                  "l3"."l_linenumber" AS "l_linenumber", 
                                                  "l3"."l_quantity" AS "l_quantity", 
                                                  "l3"."l_extendedprice" AS "l_extendedprice", 
                                                  "l3"."l_discount" AS "l_discount", 
                                                  "l3"."l_tax" AS "l_tax", 
                                                  "l3"."l_returnflag" AS "l_returnflag", 
                                                  "l3"."l_linestatus" AS "l_linestatus", 
                                                  "l3"."l_shipdate" AS "l_shipdate", 
                                                  "l3"."l_commitdate" AS "l_commitdate", 
                                                  "l3"."l_receiptdate" AS "l_receiptdate", 
                                                  "l3"."l_shipinstruct" AS "l_shipinstruct", 
                                                  "l3"."l_shipmode" AS "l_shipmode", 
                                                  "l3"."l_comment" AS "l_comment"
                                           FROM lineitem AS "l3"
                                           WHERE ("l3"."l_orderkey" = "l1"."l_orderkey"
                                                  AND
                                                  "l3"."l_suppkey" <> "l1"."l_suppkey"
                                                  AND
                                                  "l3"."l_receiptdate" > "l3"."l_commitdate")))
                       ) AS "q"("ok", "pk", "sk", "ln")) AS "val_2"
         ) AS "let2"("val_2"));
         