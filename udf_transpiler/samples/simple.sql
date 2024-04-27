CREATE FUNCTION if_func(cid decimal(3,2), cid2 BIGINT) RETURNS decimal(18,3) AS $$
DECLARE p1 decimal(3,2) = 10;
BEGIN
    RETURN cid;
END; $$
LANGUAGE PLPGSQL;