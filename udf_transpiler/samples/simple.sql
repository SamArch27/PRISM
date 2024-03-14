create function q5Conditions(
    odate date,
    p int
) returns date as $$
declare ret date;
begin
    ret = odate + p;
    return ret;
end $$ LANGUAGE PLPGSQL;