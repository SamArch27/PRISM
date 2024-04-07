init:
	- git clone https://github.com/hkulyc/duckdb.git udf_transpiler/duckdb
	cd udf_transpiler/duckdb && git switch cherry_pick
	ln -s ../udf_transpiler/duckdb/ ./header_file/duckdb

pull:
	cd udf_transpiler/duckdb && git pull && git switch cherry_pick
	cd udf_transpiler/ && git pull

	