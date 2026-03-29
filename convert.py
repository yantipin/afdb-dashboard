# 2. Convert TSV → Parquet (partitioned by batch for extra speed)
import pandas as pd
import duckdb
import sys
import os

fnc = sys.argv[1]
dir = os.path.dirname(fnc)
name = os.path.basename(fnc).split('.')[0]
fnpa = f"{dir}/{name}.parquet"
fndd = f"{dir}/{name}.duckdb"
print(fnpa)

if not os.path.exists(fnpa):
	print('reading from', fnpa, '..')
	df = pd.read_csv(fnc)
	print('writing to', fnpa, '..')
	df.to_parquet(fnpa, compression="zstd", engine="pyarrow")
else:
	print('reading from', fnpa, '..')
	df = pd.read_parquet(fnpa)

print(f"DataFrame shape: {df.shape[0]:,} rows × {df.shape[1]:,} columns")

# Convert the Parquet DataFrame into a DuckDB persistent file
if not os.path.exists(fndd):
    print('writing to', fndd, '..')
    with duckdb.connect(database=fndd) as con:
        con.execute("INSTALL fts; LOAD fts;")
        # Save DataFrame as a table in DuckDB
        con.register('df', df)
        con.execute("CREATE TABLE complexes AS SELECT * FROM df")
    print('DuckDB file created:', fndd)
else:
    print('DuckDB file already exists:', fndd)

# 
# # Optional: create a persistent DuckDB file (even faster)
# duckdb.execute("""
# 	CREATE TABLE complexes AS 
# 	SELECT * FROM read_parquet('{fnpa}');
# """)
# duckdb.execute("INSTALL fts; LOAD fts;")  # for text search

# # duckdb.execute("CREATE INDEX idx_name ON complexes USING fts(model_id);")
# # Save the DuckDB table to a persistent file
# duckdb_file = f"{dir}/{name}.duckdb"
# if not os.path.exists(duckdb_file):
#     print("Saving DuckDB database to", duckdb_file, "...")
#     # Create and persist the DuckDB file with the table
#     with duckdb.connect(database=duckdb_file) as con:
#         con.execute("INSTALL fts; LOAD fts;")
#         # Write the DataFrame directly to the DuckDB database file
#         con.execute("CREATE TABLE complexes AS SELECT * FROM df")


