init:
	- git clone https://github.com/hkulyc/duckdb.git prism/duckdb
	cd prism/duckdb && git switch cherry_pick
	ln -s ../prism/duckdb/ ./header_file/duckdb

pull:
	cd prism/duckdb && git pull && git switch cherry_pick
	cd prism/ && git pull

	
