CREATE FUnCTION addNumbers(val1 DOUBLE, val2 DOUBLE) RETuRNS double AS $$
DECLARE p1 integer = 10*val1;
DECLARE p2 integer;
BEGIN
RETURN val1 + val2 + p1+p2;
END; $$
LANGUAGE PLPGSQL;

CREATE FUnCTION addNumbers2(val1 DOUBLE, val2 DOUBLE) RETuRNS double AS $$
DECLARE p1 integer = 10*val1;
DECLARE p2 integer=20;
BEGIN
RETURN val1 + val2 + p1+p2;
END; $$
LANGUAGE PLPGSQL;

CREATE FUnCTION addNumbers3(val1 DOUBLE, val2 DOUBLE) RETuRNS integer AS $$
DECLARE p1 integer = 10*val1;
DECLARE p2 integer=20;
BEGIN
RETURN val1 + val2 + p1+p2;
END; $$
LANGUAGE PLPGSQL;


CREATE FUnCTION addNumbers4(val1 DOUBLE, val2 DOUBLE) RETuRNS double AS $$
DECLARE p1 integer = 10*val1;
p2 integer := p1 + 20;
quantity integer DEFAULT 32;
BEGIN
RETURN val1 + val2 + p1+p2;
END; $$
LANGUAGE PLPGSQL;