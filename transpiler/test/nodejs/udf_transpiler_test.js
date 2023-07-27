var duckdb = require('../../duckdb/tools/nodejs');
var assert = require('assert');

describe(`udf_transpiler extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:');
        conn = new duckdb.Connection(db);
        done();
    });

    it('function should return expected constant', function (done) {
        db.all("SELECT udf_transpiler('Sam') as value;", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{value: "Udf_transpiler Sam 🐥"}]);
            done();
        });
    });
});