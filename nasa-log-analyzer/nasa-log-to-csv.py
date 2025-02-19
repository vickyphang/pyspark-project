from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, coalesce, lit
from pyspark.sql.types import IntegerType
import glob

# Start Spark session
spark = SparkSession.builder \
    .appName("NASA HTTP Log Parser") \
    .getOrCreate()

# Define the regular expressions to extract necessary fields
host_pattern = r'(\S+)'  # Extracts host (non-whitespace characters)
ts_pattern = r'\[(.*?)\]'  # Extracts timestamp inside square brackets
method_uri_protocol_pattern = r'\"(\S+) (\S+) (\S+)\"'  # Extracts method, endpoint, protocol
status_pattern = r' (\d{3}) '  # Extracts the HTTP status code
content_size_pattern = r' (\d+)$'  # Extracts content size at the end of the line

# Use glob to find all .gz files in the current directory
raw_data_files = glob.glob('*.gz')

# Read all .gz files into a DataFrame
base_df = spark.read.text(raw_data_files)

# Extract required columns using regular expressions
logs_df = base_df.select(
    regexp_extract('value', host_pattern, 1).alias('host'),
    regexp_extract('value', ts_pattern, 1).alias('timestamp'),
    regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
    regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
    regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
    regexp_extract('value', status_pattern, 1).cast(IntegerType()).alias('status'),
    regexp_extract('value', content_size_pattern, 1).cast(IntegerType()).alias('content_size')
)

# Handle null values by replacing them with a default value
logs_df_cleaned = logs_df.fillna({
    'host': 'unknown',
    'timestamp': 'unknown',
    'method': 'unknown',
    'endpoint': 'unknown',
    'protocol': 'unknown',
    'status': -1,  # Default value for status
    'content_size': 0  # Default value for content size
})

# Write the cleaned DataFrame to a CSV file
output_csv_path = "output/nasa_http_log.csv"  # Replace with your desired output path
logs_df_cleaned.write.option("header", "true").csv(output_csv_path)

# Show a preview of the data
logs_df_cleaned.show(10, truncate=True)

# Optionally, print the number of records and columns
print((logs_df_cleaned.count(), len(logs_df_cleaned.columns)))
