create function udf13(numSalesFromStore int, numSalesFromCatalog int, numSalesFromWeb int)
returns varchar
language plpgsql
as
$$
declare
 maxChannel varchar;
begin
    if(numSalesFromStore>numSalesFromCatalog)then
        maxChannel := 'Store';
        if(numSalesfromWeb>numSalesFromStore)then
            maxChannel := 'Web';
        end if;
    else
        maxChannel := 'Catalog';
        if(numSalesfromWeb>numSalesFromCatalog)then
            maxChannel := 'Web';
        end if;
    end if;

    return maxChannel;                                  
end;
$$;

create function udf132(numSalesFromStore int, numSalesFromCatalog int, numSalesFromWeb int)
returns varchar
language plpgsql
as
$$
declare
 maxChannel varchar;
begin
    if(numSalesFromStore>numSalesFromCatalog)then
        maxChannel := 'Store';
        if(numSalesfromWeb>numSalesFromStore)then
            maxChannel := 'Web';
        end if;
    else
        maxChannel := 'Catalog';
        if(numSalesfromWeb>numSalesFromCatalog)then
            maxChannel := 'Web';
        end if;
    end if;

    return maxChannel;                                  
end;
$$;