CREATE FUnCTION itos(val1 int, val2 decimal(9,5)) RETuRNS double AS $$
BEGIN
RETURN val1+val2;
END; $$
LANGUAGE PLPGSQL;