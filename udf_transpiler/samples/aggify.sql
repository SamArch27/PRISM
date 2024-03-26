create table orders (o_orderkey bigint, o_custkey integer);
–
CREATE FUNCTION OrdersByCustomer(ckey integer, tmp int)
    RETURNS integer
    STABLE AS
$$
DECLARE
    custkey integer := ckey;
    okey    bigint;
    adsf   integer := tmp;
    val     integer := adsf+1;
BEGIN
    FOR okey IN (SELECT O_ORDERKEY FROM orders WHERE O_CUSTKEY = custkey)
        LOOP
            val := val + okey::int;
        END LOOP;
    RETURN val;
END;
$$ LANGUAGE plpgsql;