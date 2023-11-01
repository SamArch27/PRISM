-- TPC TPC-H Parameter Substitution (Version 2.17.3 build 0)
-- using default substitutions
-- $ID$
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- Functional Query Definition
-- Approved February 1998


-- udf version of query19




drop function discount_price;

CREATE FUNCTION discount_price(extprice decimal(15,2),disc decimal(15,2)) returns decimal(15,2) as $$
begin
return extprice*(1-disc);
END; $$
LANGUAGE PLPGSQL;


drop function q19conditions;

CREATE FUNCTION q19conditions(pcontainer char, lqty DECIMAL(15,2),psize int,shipmode char,shipinst char,pbrand char) RETURNS INTEGER AS $$
declare val int = 0;
BEGIN

    if (shipmode in ('AIR','AIR REG') and shipinst = 'DELIVER IN PERSON')
    then

      if pbrand = 'Brand#12' 
        and pcontainer in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and lqty >= 1
        and lqty <= 1 + 10
        and psize BETWEEN 1 AND 5
      then
        val = 1;
      end if;


      if pbrand = 'Brand#23' 
        and pcontainer in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and lqty >= 10
        and lqty <= 10 + 10
        and psize BETWEEN 1 AND 10
      then
        val = 1;
      end if;

      if pbrand = 'Brand#34' 
        and pcontainer in ('LG CASE', 'LG BOX', 'LG PKG', 'LG PACK')
        and lqty >= 20
        and lqty <= 20 + 10
        and psize BETWEEN 1 AND 15
      then
        val = 1;
      end if;

    end if;
    
    RETURN val;
END; $$
LANGUAGE PLPGSQL;

SELECT SUM(discount_price(L_EXTENDEDPRICE, L_DISCOUNT))
AS REVENUE
FROM LINEITEM join PART on L_PARTKEY = P_PARTKEY
WHERE q19conditions(P_CONTAINER, L_QUANTITY, P_SIZE,
L_SHIPMODE, L_SHIPINSTRUCT, P_BRAND ) = 1;