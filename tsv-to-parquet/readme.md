## Steps to Convert TSV to Parquet Using PySpark

#### 1. Download the dataset
```
# download hits.tsv ~100M rows 
wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz
```

#### 2. Update the file paths:

- Replace `/path/to/hits.tsv` with the path to your TSV file.

- Replace `/path/to/output_parquet` with the desired output directory for the Parquet files.

#### 3. Execute tsv-to-parquet.py
```bash
python3 tsv-to-parquet.py
# or
spark-submit tsv-to-parquet.py
```

#### 4. Verify output
```bash
ls hits/

part-00000-cef638b9-0e14-408f-bea5-484a40c6bf88-c000.snappy.parquet
part-00001-cef638b9-0e14-408f-bea5-484a40c6bf88-c000.snappy.parquet
part-00002-cef638b9-0e14-408f-bea5-484a40c6bf88-c000.snappy.parquet
[...]
part-00556-cef638b9-0e14-408f-bea5-484a40c6bf88-c000.snappy.parquet
part-00557-cef638b9-0e14-408f-bea5-484a40c6bf88-c000.snappy.parquet
```

## Key Points in the Script
#### Schema Definition
The schema is explicitly defined to match your table structure. This ensures that Spark reads the data correctly and avoids inference issues.

#### Reading the TSV File
The `spark.read.csv` method reads the TSV file with the specified schema and tab (`\t`) delimiter.

#### Writing to Parquet
The `df.write.parquet` method writes the data to Parquet format. The output is saved in a directory (e.g., `/path/to/output_parquet`).

#### Memory Efficiency
Spark automatically handles large datasets by spilling to disk when memory is insufficient. This makes it ideal for 70GB TSV file.