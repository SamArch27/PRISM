CREATE FUNCTION getManufact_complex(itm INT)
RETURNS VARCHAR(50)
AS
$$
DECLARE
	man VARCHAR(50);
	cnt1 int; cnt2 int;
BEGIN
	man  := '';  
	-- was this item sold in this year through store or catalog?
	cnt1 := (SELECT COUNT(*) FROM bench.store_sales, bench.date_dim WHERE ss_item_sk=itm AND d_date_sk=ss_sold_date_sk AND d_year=2003);
	cnt2  := (SELECT COUNT(*) FROM bench.catalog_sales, bench.date_dim WHERE cs_item_sk=itm AND d_date_sk=cs_sold_date_sk AND d_year=2003);
	IF (cnt1 > 0 AND cnt2 > 0) THEN
		man := (SELECT i_manufact FROM bench.item WHERE i_item_sk = itm);
	ELSE
		man := 'outdated item'; --implies that this item is not sold in a recent year at all AND is probably outdated
	END IF;
	RETURN man;
END
 $$ LANGUAGE PLPGSQL;
