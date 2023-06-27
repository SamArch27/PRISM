-- NOTE: "total_value" and "avg_actbal" udfs are not supported since they have select statements

create function discount_price(extprice decimal(12,2), disc decimal(12,2))
returns decimal(18,4) as $$
begin
    return extprice*(1-disc);
end $$
LANGUAGE PLPGSQL;

create function discount_taxprice(extprice decimal(12,2), disc decimal(12,2), tax decimal(12,2))
returns decimal(18,6) as $$
begin
    return extprice*(1-disc) * (1+tax);
end $$
LANGUAGE PLPGSQL;

create function profit_amount(extprice decimal(12,2),
                                  discount decimal(12,2),
                                  suppcost decimal(12,2),
                                  qty int)
returns decimal(18,4) as $$
begin
    return extprice*(1-discount)-suppcost*qty;
end $$
LANGUAGE PLPGSQL;

create function isShippedBefore(shipdate date,
                                duration int,
                                stdatechar varchar)
returns int as $$
declare stdate date = cast(stdatechar as date);
declare newdate date = stdate+duration;
begin
    
    if (shipdate > newdate) then
        return 0;
    end if;
    return 1;
end $$
LANGUAGE PLPGSQL;

create function checkDate(d varchar,
                          odate date,
                          shipdate date)
returns int as $$
begin
    if(odate < d AND shipdate > d) then
        return 1;
    end if;
    return 0;
end $$
LANGUAGE PLPGSQL;

create function q3conditions(cmkt varchar,
                             odate date,
                             shipdate date)
returns int as $$
declare thedate varchar = '1995-03-15';
declare stdate date;
declare newdate date;
begin
    if(cmkt <> 'BUILDING') then
        return 0;
    end if;
    if NOT (odate < thedate AND shipdate > thedate) then
        return 0;
    end if;
    stdate = cast(thedate as date);
    newdate = stdate+122;
    if (shipdate > newdate) then
        return 0;
    end if;
    return 1;
end $$
LANGUAGE PLPGSQL;

create function q5Conditions(rname char,
                             odate date)
returns int as $$
declare beginDatechar varchar = '1994-01-01';
declare beginDate date = cast(beginDatechar as date);
declare newdate date;
begin
    if(rname <> 'ASIA') then
        return 0;
    end if;
    if(odate < beginDate) then
        return 0;
    end if;
    newdate = beginDate + (INTERVAL '1 YEAR'); -- DATEADD(YY, 1, beginDate);
    if(odate >= newdate) then
        return 0;
    end if;
    return 1;
end $$
LANGUAGE PLPGSQL;

create function q6conditions(shipdate date,
                             discount decimal(12,2),
                             qty int)
returns int as $$
declare stdateChar varchar = '1994-01-01';
declare stdate date = cast(stdateChar as date);
declare newdate date = stdate + (INTERVAL '1 YEAR'); -- dateadd(yy, 1, stdate);
declare val decimal(12,2) = 0.06;
declare epsilon decimal(12,2) = 0.01;
declare lowerbound decimal(12,2);
declare upperbound decimal(12,2);
begin
    if(shipdate < stdateChar) then
        return 0;
    end if;
    if(shipdate >= newdate) then
        return 0;
    end if;
    if(qty >= 24) then
        return 0;
    end if;
    
    lowerbound = val - epsilon;
    upperbound = val + epsilon;
    if(discount >= lowerbound AND discount <= upperbound) then
        return 1;
    end if;
    return 0;
end $$
LANGUAGE PLPGSQL;

create function q7conditions(n1name varchar,
                             n2name varchar,
                             shipdate date)
returns int as $$
begin
    if(shipdate NOT BETWEEN '1995-01-01' AND '1996-12-31') then
        return 0;
    end if;
    if(n1name = 'FRANCE' AND n2name = 'GERMANY') then
        return 1;
    elsif(n1name = 'GERMANY' AND n2name = 'FRANCE') then
        return 1;
    end if;
    return 0;
end $$
LANGUAGE PLPGSQL;

create function q10conditions(odate date, retflag char)
returns int as $$
declare stdatechar varchar = '1993-10-01';
declare stdate date = cast(stdatechar as date);
declare newdate date = stdate + (INTERVAL '3 MONTH'); -- dateadd(mm, 3, stdate);
begin
    if (retflag <> 'R') then
        return 0;
    end if;
    if (odate >= stdatechar AND odate < newdate) then
        return 1;
    end if;
    return 0;
end $$
LANGUAGE PLPGSQL;

create function line_count(oprio char, mode varchar)
returns int as $$
declare val int = 0;
begin
    if(mode = 'high') then
        if(oprio = '1-URGENT' OR oprio = '2-HIGH') then
            val = 1;
        end if;
    elsif(mode = 'low') then
        if(oprio <> '1-URGENT' AND oprio <> '2-HIGH') then
            val = 1;
        end if;
    end if;
    return val;
end $$
LANGUAGE PLPGSQL;

create function q12conditions(shipmode char,
                              commitdate date,
                              receiptdate date,
                              shipdate date)
returns int as $$
declare stdatechar varchar = '1994-01-01';
declare stdate date = cast(stdatechar as date);
declare newdate date = stdate + (INTERVAL '1 YEAR'); -- dateadd(year, 1, stdate);
begin
    if(shipmode = 'MAIL' OR shipmode ='SHIP') then
        if(receiptdate < '1994-01-01') then
            return 0;
        end if;
        if(commitdate < receiptdate AND shipdate < commitdate AND receiptdate < newdate) then
            return 1;
        end if;
    end if;
    return 0;
end $$
LANGUAGE PLPGSQL;

create function promo_disc(ptype varchar(25),
                           extprice decimal(12,2),
                           disc decimal(12,2))
returns decimal(12,2) as $$
declare val decimal(12,2);
begin
    if(ptype LIKE 'PROMO%%') then
        val = extprice*(1-disc);
    else
        val = 0.0;
    end if;
    return val;
end $$
LANGUAGE PLPGSQL;

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
