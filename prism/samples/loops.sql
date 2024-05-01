CREATE FUNCTION return_1() RETURNS INTEGER AS $$
BEGIN
    RETURN 1;
END; $$
LANGUAGE PLPGSQL;

CREATE FUNCTION exit_loop() RETURNS INTEGER AS $$
declare i integer = 0;
BEGIN
    LOOP
        
        EXIT;  -- exit loop
        i = 5;
        
    END LOOP;
    RETURN i;
END; $$
LANGUAGE PLPGSQL;

CREATE FUNCTION continue_loop() RETURNS INTEGER AS $$
declare i integer = 0;
BEGIN
    while i < 10 LOOP
        i = i + 1;
        CONTINUE;
        i = i + 2;
    END LOOP;
    RETURN i;
END; $$
LANGUAGE PLPGSQL;

CREATE FUNCTION simple_loop() RETURNS INTEGER AS $$
declare i integer = 0;
BEGIN
    LOOP
        IF i > 9 THEN
            EXIT;  -- exit loop
        else
            i = i + 1;
        
        END IF;
    END LOOP;
    RETURN i;
END; $$
LANGUAGE PLPGSQL;





-- LOOP
--     -- some computations
--     EXIT WHEN count > 0;
-- END LOOP;

CREATE FUNCTION for_loop_vanilla() RETURNS INTEGER AS $$
declare i integer = 0;
BEGIN
    FOR j IN 1..10 LOOP
        i = i + 1;
    END LOOP;
    RETURN i;
END; $$
LANGUAGE PLPGSQL;

CREATE FUNCTION for_loop_step() RETURNS INTEGER AS $$
declare i integer = 0;
BEGIN
    FOR j IN 1..10 BY 2 LOOP
        i = i + 1;
    END LOOP;
    RETURN i;
END; $$
LANGUAGE PLPGSQL;

CREATE FUNCTION for_loop_reverse() RETURNS INTEGER AS $$
declare i integer = 0;
BEGIN
    FOR j IN REVERSE 10..1 LOOP
        i = i + 1;
    END LOOP;
    RETURN i;
END; $$
LANGUAGE PLPGSQL;

CREATE FUNCTION for_loop_variable_shadow() RETURNS INTEGER AS $$
declare j integer = 5;
declare acc integer = 0;
BEGIN
    FOR j IN 1..10 LOOP
        acc = acc + j + 1;
    END LOOP;
    RETURN acc;
END; $$
LANGUAGE PLPGSQL;

-- CREATE FUNCTION for_loop_variable_shadow2() RETURNS INTEGER AS $$
-- declare j integer = 5;
-- BEGIN
--     FOR j IN 1..10 LOOP
--         j = j + 1;
--     END LOOP;
--     RETURN j;
-- END; $$
-- LANGUAGE PLPGSQL;


-- drop function for_loop_variable_shadow3;

-- CREATE FUNCTION for_loop_variable_shadow3() RETURNS INTEGER AS $$
-- declare acc integer = 0;
-- BEGIN
--     FOR j IN 1..10 LOOP
--         acc = acc 
--     END LOOP;
--     RETURN acc;
-- END; $$
-- LANGUAGE PLPGSQL;

-- CREATE FUNCTION for_loop_variable_shadow_nested() RETURNS INTEGER AS $$
-- declare j integer = 5;
-- declare acc integer = 0;
-- BEGIN
--     FOR j IN 1..10 LOOP
--         acc = acc + j;
--         FOR j IN 11..20 LOOP
--             acc = acc + j;
--         END LOOP;
--     END LOOP;
--     RETURN j;
-- END; $$
-- LANGUAGE PLPGSQL;

-- CREATE FUNCTION while_loop() RETURNS INTEGER AS $$
-- declare i integer = 0;
-- BEGIN
--     while i < 10 LOOP
--         i = i + 1;
--     END LOOP;
--     RETURN i;
-- END; $$
-- LANGUAGE PLPGSQL;


-- drop table test;

-- CREATE TABLE test (id integer, num integer);
-- INSERT INTO test VALUES (1,2),(2,4),(3,6),(4,8),(5,10);
-- select id,for_loop_reverse() from test;