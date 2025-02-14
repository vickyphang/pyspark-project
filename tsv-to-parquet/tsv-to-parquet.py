from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import when, col, to_timestamp, to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TSV to Parquet") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("WatchID", LongType(), False),
    StructField("JavaEnable", IntegerType(), False),
    StructField("Title", StringType(), False),
    StructField("GoodEvent", IntegerType(), False),
    StructField("EventTime", StringType(), False),  # Read as string and convert later
    StructField("EventDate", StringType(), False),  # Read as string and convert later
    StructField("CounterID", IntegerType(), False),
    StructField("ClientIP", IntegerType(), False),
    StructField("RegionID", IntegerType(), False),
    StructField("UserID", LongType(), False),
    StructField("CounterClass", IntegerType(), False),
    StructField("OS", IntegerType(), False),
    StructField("UserAgent", IntegerType(), False),
    StructField("URL", StringType(), False),
    StructField("Referer", StringType(), False),
    StructField("IsRefresh", IntegerType(), False),
    StructField("RefererCategoryID", IntegerType(), False),
    StructField("RefererRegionID", IntegerType(), False),
    StructField("URLCategoryID", IntegerType(), False),
    StructField("URLRegionID", IntegerType(), False),
    StructField("ResolutionWidth", IntegerType(), False),
    StructField("ResolutionHeight", IntegerType(), False),
    StructField("ResolutionDepth", IntegerType(), False),
    StructField("FlashMajor", IntegerType(), False),
    StructField("FlashMinor", IntegerType(), False),
    StructField("FlashMinor2", StringType(), False),
    StructField("NetMajor", IntegerType(), False),
    StructField("NetMinor", IntegerType(), False),
    StructField("UserAgentMajor", IntegerType(), False),
    StructField("UserAgentMinor", StringType(), False),
    StructField("CookieEnable", IntegerType(), False),
    StructField("JavascriptEnable", IntegerType(), False),
    StructField("IsMobile", IntegerType(), False),
    StructField("MobilePhone", IntegerType(), False),
    StructField("MobilePhoneModel", StringType(), False),
    StructField("Params", StringType(), False),
    StructField("IPNetworkID", IntegerType(), False),
    StructField("TraficSourceID", IntegerType(), False),
    StructField("SearchEngineID", IntegerType(), False),
    StructField("SearchPhrase", StringType(), False),
    StructField("AdvEngineID", IntegerType(), False),
    StructField("IsArtifical", IntegerType(), False),
    StructField("WindowClientWidth", IntegerType(), False),
    StructField("WindowClientHeight", IntegerType(), False),
    StructField("ClientTimeZone", IntegerType(), False),
    StructField("ClientEventTime", StringType(), False),  # Read as string and convert later
    StructField("SilverlightVersion1", IntegerType(), False),
    StructField("SilverlightVersion2", IntegerType(), False),
    StructField("SilverlightVersion3", IntegerType(), False),
    StructField("SilverlightVersion4", IntegerType(), False),
    StructField("PageCharset", StringType(), False),
    StructField("CodeVersion", IntegerType(), False),
    StructField("IsLink", IntegerType(), False),
    StructField("IsDownload", IntegerType(), False),
    StructField("IsNotBounce", IntegerType(), False),
    StructField("FUniqID", LongType(), False),
    StructField("OriginalURL", StringType(), False),
    StructField("HID", IntegerType(), False),
    StructField("IsOldCounter", IntegerType(), False),
    StructField("IsEvent", IntegerType(), False),
    StructField("IsParameter", IntegerType(), False),
    StructField("DontCountHits", IntegerType(), False),
    StructField("WithHash", IntegerType(), False),
    StructField("HitColor", StringType(), False),
    StructField("LocalEventTime", StringType(), False),  # Read as string and convert later
    StructField("Age", IntegerType(), False),
    StructField("Sex", IntegerType(), False),
    StructField("Income", IntegerType(), False),
    StructField("Interests", IntegerType(), False),
    StructField("Robotness", IntegerType(), False),
    StructField("RemoteIP", IntegerType(), False),
    StructField("WindowName", IntegerType(), False),
    StructField("OpenerName", IntegerType(), False),
    StructField("HistoryLength", IntegerType(), False),
    StructField("BrowserLanguage", StringType(), False),
    StructField("BrowserCountry", StringType(), False),
    StructField("SocialNetwork", StringType(), False),
    StructField("SocialAction", StringType(), False),
    StructField("HTTPError", IntegerType(), False),
    StructField("SendTiming", IntegerType(), False),
    StructField("DNSTiming", IntegerType(), False),
    StructField("ConnectTiming", IntegerType(), False),
    StructField("ResponseStartTiming", IntegerType(), False),
    StructField("ResponseEndTiming", IntegerType(), False),
    StructField("FetchTiming", IntegerType(), False),
    StructField("SocialSourceNetworkID", IntegerType(), False),
    StructField("SocialSourcePage", StringType(), False),
    StructField("ParamPrice", LongType(), False),
    StructField("ParamOrderID", StringType(), False),
    StructField("ParamCurrency", StringType(), False),
    StructField("ParamCurrencyID", IntegerType(), False),
    StructField("OpenstatServiceName", StringType(), False),
    StructField("OpenstatCampaignID", StringType(), False),
    StructField("OpenstatAdID", StringType(), False),
    StructField("OpenstatSourceID", StringType(), False),
    StructField("UTMSource", StringType(), False),
    StructField("UTMMedium", StringType(), False),
    StructField("UTMCampaign", StringType(), False),
    StructField("UTMContent", StringType(), False),
    StructField("UTMTerm", StringType(), False),
    StructField("FromTag", StringType(), False),
    StructField("HasGCLID", IntegerType(), False),
    StructField("RefererHash", LongType(), False),
    StructField("URLHash", LongType(), False),
    StructField("CLID", IntegerType(), False)
])

# Read the TSV file
df = spark.read.csv("/path/to/hits.tsv", schema=schema, sep="\t", header=False)

# Handle missing or invalid data
df = df.withColumn("Title", when(col("Title") == "�", None).otherwise(col("Title")))
df = df.withColumn("FlashMinor2", when(col("FlashMinor2") == "�", None).otherwise(col("FlashMinor2")))
df = df.withColumn("UserAgentMinor", when(col("UserAgentMinor") == "�", None).otherwise(col("UserAgentMinor")))
df = df.withColumn("HitColor", when(col("HitColor") == "�", None).otherwise(col("HitColor")))

# Convert timestamp and date columns
df = df.withColumn("EventTime", to_timestamp(col("EventTime"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("EventDate", to_date(col("EventDate"), "yyyy-MM-dd"))
df = df.withColumn("ClientEventTime", to_timestamp(col("ClientEventTime"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("LocalEventTime", to_timestamp(col("LocalEventTime"), "yyyy-MM-dd HH:mm:ss"))

# Write to Parquet
df.write.parquet("/path/to/output_parquet", mode="overwrite", compression="snappy")

print("Conversion complete!")