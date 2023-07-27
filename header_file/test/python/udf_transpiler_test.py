import duckdb

def test_udf1():
    conn = duckdb.connect('');
    conn.execute("SELECT udf1('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Udf1 Sam 🐥");