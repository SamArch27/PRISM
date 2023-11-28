call dbgen(sf=1);
pragma build_agg;

create macro OrdersByCustomerWithCustomAgg(cust_key) as
(SELECT "ifresult4".*
    FROM LATERAL
         (SELECT NULL :: int4 AS "custkey_2") AS "let0"("custkey_2"), 
         LATERAL (SELECT "cust_key" AS "custkey_1") AS "let1"("custkey_1"), 
         LATERAL (SELECT 0 AS "val_1") AS "let2"("val_1"), 
         LATERAL
         (SELECT EXISTS (SELECT "orders"."o_orderkey" AS "o_orderkey"
                         FROM orders AS "orders"
                         WHERE "orders"."o_custkey" = "custkey_1") AS "q4_1"
         ) AS "let3"("q4_1"), 
         LATERAL
         ((SELECT "val_3" AS "result"
           FROM LATERAL
                (SELECT (SELECT ordersbycustomeraggregate("s"."o_orderkey", "val_1") AS "row"
                         FROM 
                              (SELECT "orders"."o_orderkey" AS "o_orderkey"
                               FROM orders AS "orders"
                               WHERE "orders"."o_custkey" = "custkey_1"
                              ) AS "s"("o_orderkey")) AS "val_3"
                ) AS "let5"("val_3")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "val_1" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult4");

create macro PromoRevenueWithCustomAgg(partkey) AS
(SELECT "ifresult4".*
    FROM LATERAL (SELECT NULL :: int4 AS "pkey_2") AS "let0"("pkey_2"), 
         LATERAL (SELECT "partkey" AS "pkey_1") AS "let1"("pkey_1"), 
         LATERAL (SELECT 0 AS "revenue_1") AS "let2"("revenue_1"), 
         LATERAL
         (SELECT EXISTS (SELECT "lineitem"."l_extendedprice" AS "l_extendedprice", 
                                "lineitem"."l_discount" AS "l_discount"
                         FROM lineitem AS "lineitem"
                         WHERE "lineitem"."l_partkey" = "pkey_1") AS "q4_1"
         ) AS "let3"("q4_1"), 
         LATERAL
         ((SELECT "revenue_3" AS "result"
           FROM LATERAL
                (SELECT (SELECT promo_revenue_agg("tmp"."l_extendedprice" :: bigint, 
                                 "tmp"."l_discount" :: bigint, 
                                 "revenue_1" :: bigint) AS "row"
                         FROM 
                              (SELECT "lineitem"."l_extendedprice" AS "l_extendedprice", 
                                      "lineitem"."l_discount" AS "l_discount"
                               FROM lineitem AS "lineitem"
                               WHERE "lineitem"."l_partkey" = "pkey_1"
                              ) AS "tmp"("l_extendedprice", "l_discount")) AS "revenue_3"
                ) AS "let5"("revenue_3")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "revenue_1" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult4");

create macro VolumeCustomerWithCustomAgg(orderkey) AS
(SELECT "ifresult7".*
    FROM LATERAL (SELECT NULL :: int8 AS "ok_3") AS "let0"("ok_3"), 
         LATERAL (SELECT NULL :: int4 AS "i_5") AS "let1"("i_5"), 
         LATERAL (SELECT NULL :: numeric AS "d_3") AS "let2"("d_3"), 
         LATERAL (SELECT "orderkey" AS "ok_1") AS "let3"("ok_1"), 
         LATERAL (SELECT 0 AS "i_1") AS "let4"("i_1"), 
         LATERAL (SELECT 0 AS "d_1") AS "let5"("d_1"), 
         LATERAL
         (SELECT EXISTS (SELECT "lineitem"."l_quantity" AS "l_quantity"
                         FROM lineitem AS "lineitem"
                         WHERE "lineitem"."l_orderkey" = "ok_1") AS "q4_1"
         ) AS "let6"("q4_1"), 
         LATERAL
         ((SELECT "ifresult10".*
           FROM LATERAL
                (SELECT (SELECT volume_customer_agg("tmp"."l_quantity" :: int, "d_1") AS "row"
                         FROM 
                              (SELECT "lineitem"."l_quantity" AS "l_quantity"
                               FROM lineitem AS "lineitem"
                               WHERE "lineitem"."l_orderkey" = "ok_1"
                              ) AS "tmp"("l_quantity")) AS "d_5"
                ) AS "let8"("d_5"), 
                LATERAL (SELECT "d_5" > (300) AS "q8_2") AS "let9"("q8_2"), 
                LATERAL
                ((SELECT "i_4" AS "result"
                  FROM LATERAL (SELECT 1 AS "i_4") AS "let11"("i_4")
                  WHERE NOT "q8_2" IS DISTINCT FROM True)
                   UNION ALL
                 (SELECT "i_1" AS "result"
                  WHERE "q8_2" IS DISTINCT FROM True)
                ) AS "ifresult10"
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "ifresult15".*
           FROM LATERAL (SELECT "d_1" > (300) AS "q8_2") AS "let14"("q8_2"), 
                LATERAL
                ((SELECT "i_4" AS "result"
                  FROM LATERAL (SELECT 1 AS "i_4") AS "let16"("i_4")
                  WHERE NOT "q8_2" IS DISTINCT FROM True)
                   UNION ALL
                 (SELECT "i_1" AS "result"
                  WHERE "q8_2" IS DISTINCT FROM True)
                ) AS "ifresult15"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult7");

create macro DiscountedRevenueWithCustomAgg() as
(SELECT "ifresult2".*
    FROM LATERAL (SELECT 0 AS "val_1") AS "let0"("val_1"), 
         LATERAL
         (SELECT EXISTS (SELECT "lineitem"."l_extendedprice" AS "l_extendedprice", 
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
                                 ("lineitem"."l_quantity" >= (1)
                                  AND
                                  "lineitem"."l_quantity" <= (11))
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
                                 "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'))) AS "q4_1"
         ) AS "let1"("q4_1"), 
         LATERAL
         ((SELECT "val_3" AS "result"
           FROM LATERAL
                (SELECT (SELECT sum_discounted_price("tmp"."l_extendedprice" :: int, 
                                 "tmp"."l_discount" :: int, 
                                 "val_1" :: int) AS "row"
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
                                       ("lineitem"."l_quantity" >= (1)
                                        AND
                                        "lineitem"."l_quantity" <= (11))
                                       AND
                                       ("part"."p_size" >= 1 AND "part"."p_size" <= 5)
                                       AND
                                       "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 
                                                                            'AIR REG'] :: bpchar[])
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
                                       "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 
                                                                            'AIR REG'] :: bpchar[])
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
                                       "lineitem"."l_shipmode" = ANY (ARRAY['AIR', 
                                                                            'AIR REG'] :: bpchar[])
                                       AND
                                       "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'))
                              ) AS "tmp"("l_extendedprice", "l_discount")) AS "val_3"
                ) AS "let3"("val_3")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "val_1" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult2");

create macro MinCostSuppWithCustomAgg(pk) AS
(SELECT "ifresult6".*
    FROM LATERAL (SELECT NULL :: int8 AS "key_2") AS "let0"("key_2"), 
         LATERAL (SELECT NULL :: bpchar AS "val") AS "let1"("val"), 
         LATERAL
         (SELECT NULL :: numeric AS "pmin_cost_2") AS "let2"("pmin_cost_2"), 
         LATERAL (SELECT "pk" AS "key_1") AS "let3"("key_1"), 
         LATERAL (SELECT 100000 AS "pmin_cost_1") AS "let4"("pmin_cost_1"), 
         LATERAL
         (SELECT EXISTS (SELECT "partsupp"."ps_supplycost" AS "ps_supplycost", 
                                "supplier"."s_name" AS "s_name"
                         FROM partsupp AS "partsupp", supplier AS "supplier"
                         WHERE ("partsupp"."ps_partkey" = "key_1"
                                AND
                                "partsupp"."ps_suppkey" = "supplier"."s_suppkey")) AS "q4_1"
         ) AS "let5"("q4_1"), 
         LATERAL
         ((SELECT "val_2" AS "result"
           FROM LATERAL
                (SELECT (SELECT MinCostSuppAggregate("tmp"."ps_supplycost" :: int, 
                                 "tmp"."s_name", 
                                 "pmin_cost_1") AS "row"
                         FROM 
                              (SELECT "partsupp"."ps_supplycost" AS "ps_supplycost", 
                                      "supplier"."s_name" AS "s_name"
                               FROM partsupp AS "partsupp", supplier AS "supplier"
                               WHERE ("partsupp"."ps_partkey" = "key_1"
                                      AND
                                      "partsupp"."ps_suppkey" = "supplier"."s_suppkey")
                              ) AS "tmp"("ps_supplycost", "s_name")) AS "val_2"
                ) AS "let7"("val_2")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "val" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult6");

create macro WaitingOrdersWithCustomAgg(suppkey) AS
(SELECT "ifresult4".*
    FROM LATERAL (SELECT NULL :: int8 AS "skey_2") AS "let0"("skey_2"), 
         LATERAL (SELECT "suppkey" AS "skey_1") AS "let1"("skey_1"), 
         LATERAL (SELECT 0 AS "val_1") AS "let2"("val_1"), 
         LATERAL
         (SELECT EXISTS (SELECT "l1"."l_orderkey" AS "ok", 
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
                                                   "l3"."l_receiptdate"
                                                   >
                                                   "l3"."l_commitdate")))) AS "q4_1"
         ) AS "let3"("q4_1"), 
         LATERAL
         ((SELECT "val_3" AS "result"
           FROM LATERAL
                (SELECT (SELECT ordersbycustomeraggregate("q"."ok",
                                 "val_1") AS "row"
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
                                                         "l3"."l_receiptdate"
                                                         >
                                                         "l3"."l_commitdate")))
                              ) AS "q"("ok", "pk", "sk", "ln")) AS "val_3"
                ) AS "let5"("val_3")
           WHERE NOT "q4_1" IS DISTINCT FROM True)
            UNION ALL
          (SELECT "val_1" AS "result"
           WHERE "q4_1" IS DISTINCT FROM True)
         ) AS "ifresult4");
         