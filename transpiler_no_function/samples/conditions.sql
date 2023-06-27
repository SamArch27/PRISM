CREATE FUNCTION if_func(cid BIGINT) RETURNS VARCHAR AS $$
DECLARE pd VARCHAR = 'function'; 
DECLARE pd2 VARCHAR;
declare i integer = 0;
BEGIN
    pd2 := concat(pd, 'test');
    if cid < 3 then 
        cid := cid + 1;
    elsif cid = 3 then
    elsif cid > 5 then
        cid := cid * 2;
    else
        cid := cid - 1;
        cid := cid + 1;
    end if;
    RETURN concat(cid,pd2);
END; $$
LANGUAGE PLPGSQL;