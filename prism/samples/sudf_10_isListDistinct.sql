-- Check if a given list of varchars is distinct. Called from sudf_14_sameManager

CREATE FUNCTION isListDistinct(list VARCHAR, delim CHAR) RETURNS BOOL AS
$$
DECLARE
  part TEXT;
  pos  int;
BEGIN
  list := LTRIM(RTRIM(list)) || delim;
  pos  := STRPOS(list, delim)::int;
  WHILE pos > 0 LOOP
    part := LTRIM(RTRIM(LEFT(list,pos)));
    list := SUBSTRING(list, pos+1, LENGTH(list));
    IF STRPOS(list,part) <> 0 THEN
      RETURN FALSE; 
    END IF;

    pos := STRPOS(list,delim)::int;
  END LOOP;

  RETURN TRUE; 
END
$$ LANGUAGE PLPGSQL;