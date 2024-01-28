CREATE FUNCTION unsupported_func(cid integer) RETURNS VARCHAR AS $$
DECLARE
    users_rec int;
    full_name varchar;
BEGIN
    users_rec := (SELECT catalog_name FROM temp.information_schema.schemata WHERE cid=catalog_name);
    IF users_rec = 0 THEN
        RETURN 'http://';
    END IF;
    -- RETURN '';
END;$$
LANGUAGE PLPGSQL;