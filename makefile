init:
	- git clone https://github.com/hkulyc/duckdb.git udf_transpiler/duckdb
	ln -s ../udf_transpiler/duckdb/ ./header_file/duckdb

pull:
	cd udf_transpiler/duckdb && git pull
	cd udf_transpiler/ && git pull

	