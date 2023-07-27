import duckdb

def test_udf_transpiler():
    conn = duckdb.connect('');
    conn.execute("SELECT udf_transpiler('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Udf_transpiler Sam 🐥");