CREATE FUNCTION unsupported_func(cid integer) RETURNS VARCHAR AS $$
DECLARE
    users_rec RECORD;
    full_name varchar;
BEGIN
    SELECT INTO users_rec * FROM users WHERE user_id=3;

    IF users_rec.homepage IS NULL THEN
        -- user entered no homepage, return "http://"

        RETURN "http://";
    END IF;
END;$$
LANGUAGE PLPGSQL;