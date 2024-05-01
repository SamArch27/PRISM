CREATE FUNCTION OrdersByCustomer()
    RETURNS bigint
    STABLE AS
$$
DECLARE
    id bigint;
    num    bigint;
    argsmallest     bigint := 0;
    smallest        bigint := 1000000000;
BEGIN
    FOR id, num IN (SELECT a, b FROM generate_series(2,3) t1(a), generate_series(4,5) t2(b))
        LOOP
            if num < smallest then
                smallest := num;
                argsmallest := id;
            end if;
        END LOOP;
    RETURN argsmallest;
END;
$$ LANGUAGE plpgsql;