CREATE FUNCTION f_update(revenue DECIMAL(25,5), fetched_price DECIMAL(12, 2),
    fetched_discount DECIMAL(12, 2))
    RETURNS BIGINT AS
$$
BEGIN
    RETURN revenue + fetched_price * (1 - fetched_discount);
END;
$$ LANGUAGE plpgsql;