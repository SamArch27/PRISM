CREATE FUNCTION boolean_test() RETURNS boolean AS $$
BEGIN
    RETURN 1=1;
END; $$
LANGUAGE PLPGSQL;