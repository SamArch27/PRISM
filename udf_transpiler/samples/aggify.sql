create table orders (o_orderkey bigint, o_custkey integer);
CREATE FUNCTION OrdersByCustomer(ckey integer, tmp int) RETURNS integer STABLE AS $$
DECLARE custkey integer := ckey;
okey bigint;
adsf integer := tmp;
val integer := adsf + 1;
BEGIN FOR okey IN (
    SELECT O_ORDERKEY
    FROM orders
    WHERE O_CUSTKEY = custkey
) LOOP val := val + 1;
END LOOP;
RETURN val;
END;
$$ LANGUAGE plpgsql;
create macro OrdersByCustomer3(ckey, tmp) as (
    (
        SELECT (
                CASE
                    WHEN count(*) > 0 THEN custom_agg3((tmp) + 1, fetchQueryVar0)
                    ELSE (tmp + 1)
                END
            )
        FROM (
                SELECT *
                FROM (
                        (
                            SELECT O_ORDERKEY
                            FROM orders
                            WHERE O_CUSTKEY = (ckey)
                        )
                    ) fetchQueryTmpTable(fetchQueryVar0)
            ) aggify_tmp
    )
);
select (
        SELECT (
                CASE
                    WHEN count(*) > 0 THEN custom_agg1((tmp) + 1, fetchQueryVar0)
                    ELSE (tmp + 1)
                END
            )
        FROM (
                SELECT *
                FROM (
                        (
                            SELECT O_ORDERKEY
                            FROM orders
                            WHERE O_CUSTKEY = (ckey)
                        )
                    ) fetchQueryTmpTable(fetchQueryVar0)
            ) aggify_tmp
    )
from (
        values (3, 4),
            (1,2),
            (5,10)
    ) t(ckey, tmp);



select c_orderkey,
    ads
from (
        values (3, 3),
            (3, 3)
    ) as tmp(ckey, val),
    lateral (
        SELECT CASE
                WHEN count(*) > 0 THEN custom_agg3(val + 1, fetchQueryVar0)
                ELSE (((val)) + 1)
            END
        FROM (
                SELECT *
                FROM (
                        (
                            SELECT orders.O_ORDERKEY
                            FROM orders
                            WHERE O_CUSTKEY = ((ckey))
                        )
                    ) fetchQueryTmpTable(fetchQueryVar0)
            )
    ) aggify_tmp(ads);
create macro test3(a, b) as (
    (
        SELECT (
                CASE
                    WHEN count(*) > 0 THEN count(b)
                    ELSE ANY_VALUE(b)
                END
            )
        FROM (
                SELECT *
                FROM (
                        (
                            SELECT col0
                            FROM (
                                    values (1),
                                        (2),
                                        (2)
                                ) tmp(col0)
                            WHERE col0 = ANY_VALUE(a)
                        )
                    ) fetchQueryTmpTable(fetchQueryVar0)
            ) aggify_tmp
    )
);
select C_CUSTKEY,
    (
        select dt4.retval
        from (
                select C_CUSTKEY as ckey
            ) dt0,
            lateral (
                select 0 as val
            ) dt1,
            lateral (
                select count(dt2.o_orderkey) as udtVal
                from (
                        select O_ORDERKEY
                        from orders
                        where O_CUSTKEY = dt0.ckey
                    ) dt2
            ) dt3,
            lateral (
                select dt2.udtVal.val as retval
            ) dt4,
    )
from customer;