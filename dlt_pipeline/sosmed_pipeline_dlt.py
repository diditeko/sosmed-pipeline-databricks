import dlt # type: ignore
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, lower, regexp_replace, length, to_timestamp,
    udf,array_except,concat_ws,split,year, month, dayofmonth, hour
)
from pyspark.sql.types import StringType, ArrayType
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
import re
import string

# ==============================================================================
# 1. KONFIGURASI DAN PARAMETER
# ==============================================================================

# Kafka Topic
KAFKA_TOPIC = "Twitter-raw"

# Load Kafka credentials dari Databricks Secrets
KAFKA_BOOTSTRAP_SERVERS = spark.conf.get( # type: ignore
    "spark.kafka.bootstrap.servers",
    dbutils.secrets.get(scope="kafka-confluent", key="bootstrap-servers") # type: ignore
)
KAFKA_API_KEY = dbutils.secrets.get(scope="kafka-confluent", key="api-key") # type: ignore
KAFKA_API_SECRET = dbutils.secrets.get(scope="kafka-confluent", key="api-secret") # type: ignore

# Build JAAS config untuk SASL authentication
JAAS_CONFIG = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";'

print(f"✓ Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"✓ Topic: {KAFKA_TOPIC}")

# ==============================================================================
# 2. SCHEMA DEFINITION (Sesuai dengan Crawler Anda)
# ==============================================================================

# Schema untuk Tweet dan Reply (unified structure)
TWEET_REPLY_SCHEMA = StructType([
    StructField("type", StringType(), True),  # "tweet" atau "reply"
    StructField("id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("lang", StringType(), True),
    
    # User info (flat structure dari crawler Anda)
    StructField("username", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    
    # Reply context
    StructField("is_reply", BooleanType(), True),
    StructField("in_reply_to_id", StringType(), True),
    StructField("in_reply_to_username", StringType(), True),
    StructField("root_tweet_id", StringType(), True),  # Hanya ada di reply
    
    # Engagement metrics
    StructField("reply_count", IntegerType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("quote_count", IntegerType(), True),
    StructField("view_count", IntegerType(), True),
    StructField("bookmark_count", IntegerType(), True)
])

# ==============================================================================
# 3. LAYER BRONZE (Data Mentah dari Kafka)
# ==============================================================================

@dlt.table(
    name="twitter_bronze",
    comment="Raw data dari Kafka Confluent Cloud. Berisi tweet dan reply dalam format JSON.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "kafka_timestamp"
    }
)
def bronze_raw_data():
    """
    Membaca streaming data dari Kafka dan menyimpan sebagai Bronze layer.
    Data masih dalam format raw JSON.
    """
    
    return (
        spark.readStream # type: ignore
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            
            # Security Configuration untuk Confluent Cloud
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", JAAS_CONFIG)
            
            # Performance tuning
            .option("maxOffsetsPerTrigger", 10000)
            .option("failOnDataLoss", "false")
            
            .load()
            .select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("raw_json"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp"),
                current_timestamp().alias("ingestion_timestamp")
            )
    )

# ==============================================================================
# 4. LAYER SILVER (Data Bersih dan Terstruktur)
# ==============================================================================

@dlt.table(
    name="twitter_silver",
    comment="Cleaned and structured tweet & reply data dengan validasi quality.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "created_date,keyword,type"
    }
)
@dlt.expect_or_drop("valid_id", "tweet_id IS NOT NULL")
@dlt.expect_or_drop("valid_text", "text IS NOT NULL AND length(text) > 0")
@dlt.expect_or_drop("valid_username", "username IS NOT NULL")
@dlt.expect_or_drop("valid_type", "type IN ('tweet', 'reply')")
@dlt.expect("valid_date", "created_at IS NOT NULL", on_violation="WARN")
def silver_cleaned_data():
    """
    Parse JSON dari Bronze layer dan ekstrak fields yang relevan.
    Apply data quality checks dan transformasi.
    """
    
    return (
        dlt.read_stream("twitter_bronze")
            # Parse JSON
            .withColumn("parsed_data", from_json(col("raw_json"), TWEET_REPLY_SCHEMA))
            
            # Flatten structure
            .select(
                # Kafka metadata
                col("kafka_key"),
                col("kafka_timestamp"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("ingestion_timestamp"),
                
                # Core fields
                col("parsed_data.type").alias("type"),
                col("parsed_data.id").alias("tweet_id"),
                col("parsed_data.url").alias("tweet_url"),
                col("parsed_data.keyword").alias("keyword"),
                col("parsed_data.text").alias("text"),
                col("parsed_data.lang").alias("language"),
                
                # Parse timestamp (support multiple formats)
                coalesce(
                    to_timestamp(col("parsed_data.created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    to_timestamp(col("parsed_data.created_at"), "yyyy-MM-dd HH:mm:ss"),
                    to_timestamp(col("parsed_data.created_at"))
                ).alias("created_at"),
                
                # User info
                col("parsed_data.username").alias("username"),
                col("parsed_data.user_id").alias("user_id"),
                col("parsed_data.user_name").alias("user_name"),
                
                # Reply context
                col("parsed_data.is_reply").alias("is_reply"),
                col("parsed_data.in_reply_to_id").alias("in_reply_to_id"),
                col("parsed_data.in_reply_to_username").alias("in_reply_to_username"),
                col("parsed_data.root_tweet_id").alias("root_tweet_id"),
                
                # Engagement metrics
                col("parsed_data.reply_count").alias("reply_count"),
                col("parsed_data.retweet_count").alias("retweet_count"),
                col("parsed_data.like_count").alias("like_count"),
                col("parsed_data.quote_count").alias("quote_count"),
                col("parsed_data.view_count").alias("view_count"),
                col("parsed_data.bookmark_count").alias("bookmark_count")
            )
            
            # Derived columns
            .withColumn("created_date", to_date(col("created_at")))
            .withColumn("created_hour", hour(col("created_at")))
            .withColumn("created_day_of_week", dayofweek(col("created_at")))
            
            # Text analysis
            .withColumn("text_length", length(col("text")))
            .withColumn("word_count", size(split(col("text"), "\\s+")))
            .withColumn("has_url", col("text").rlike("http[s]?://"))
            .withColumn("has_hashtag", col("text").rlike("#\\w+"))
            .withColumn("has_mention", col("text").rlike("@\\w+"))
            
            # Engagement metrics
            .withColumn(
                "total_engagement",
                coalesce(col("like_count"), lit(0)) + 
                coalesce(col("retweet_count"), lit(0)) + 
                coalesce(col("reply_count"), lit(0)) +
                coalesce(col("quote_count"), lit(0))
            )
            
            # Extract hashtags dan mentions dari text
            .withColumn(
                "hashtags",
                expr("regexp_extract_all(text, '#(\\\\w+)', 1)")
            )
            .withColumn(
                "mentions",
                expr("regexp_extract_all(text, '@(\\\\w+)', 1)")
            )
            .withColumn("hashtag_count", size(col("hashtags")))
            .withColumn("mention_count", size(col("mentions")))
    )

# ==============================================================================
# 5. LAYER GOLD - Tweet Statistics (Main Tweets Only)
# ==============================================================================

@dlt.table(
    name="twitter_gold_tweet_stats",
    comment="Statistik untuk main tweets (type='tweet') per keyword dan tanggal"
)
def gold_tweet_stats():
    """
    Agregasi harian untuk tweet utama (bukan reply)
    """
    
    return (
        dlt.read("twitter_silver")
            .filter(col("type") == "tweet")  # Hanya main tweets
            .groupBy("created_date", "keyword")
            .agg(
                # Volume metrics
                count("*").alias("total_tweets"),
                countDistinct("username").alias("unique_users"), # type: ignore
                countDistinct("tweet_id").alias("unique_tweet_ids"), # type: ignore
                
                # Engagement metrics
                sum("like_count").alias("total_likes"),
                sum("retweet_count").alias("total_retweets"),
                sum("reply_count").alias("total_replies"),
                sum("quote_count").alias("total_quotes"),
                sum("view_count").alias("total_views"),
                sum("bookmark_count").alias("total_bookmarks"),
                sum("total_engagement").alias("total_engagement"),
                
                avg("like_count").alias("avg_likes"),
                avg("retweet_count").alias("avg_retweets"),
                avg("reply_count").alias("avg_replies"),
                avg("total_engagement").alias("avg_engagement"),
                
                # Content metrics
                avg("text_length").alias("avg_text_length"),
                avg("word_count").alias("avg_word_count"),
                avg("hashtag_count").alias("avg_hashtags"),
                avg("mention_count").alias("avg_mentions"),
                
                # Content type distribution
                sum(when(col("has_url"), 1).otherwise(0)).alias("tweets_with_url"),
                sum(when(col("has_hashtag"), 1).otherwise(0)).alias("tweets_with_hashtag"),
                sum(when(col("has_mention"), 1).otherwise(0)).alias("tweets_with_mention"),
                sum(when(col("is_reply"), 1).otherwise(0)).alias("reply_tweets"),
                
                # Language distribution
                count(when(col("language") == "en", 1)).alias("english_tweets"),
                count(when(col("language") == "id", 1)).alias("indonesian_tweets"),
                
                # Time range
                min("created_at").alias("first_tweet_time"),
                max("created_at").alias("last_tweet_time")
            )
            .orderBy(desc("created_date"), desc("total_tweets"))
    )

# ==============================================================================
# 6. LAYER GOLD - Reply Statistics
# ==============================================================================

@dlt.table(
    name="twitter_gold_reply_stats",
    comment="Statistik untuk replies per keyword dan tanggal"
)
def gold_reply_stats():
    """
    Agregasi untuk replies (type='reply')
    """
    
    return (
        dlt.read("twitter_silver")
            .filter(col("type") == "reply")
            .groupBy("created_date", "keyword")
            .agg(
                count("*").alias("total_replies"),
                countDistinct("username").alias("unique_repliers"), # type: ignore
                countDistinct("root_tweet_id").alias("unique_root_tweets"), # type: ignore
                
                # Engagement pada replies
                sum("like_count").alias("total_reply_likes"),
                sum("retweet_count").alias("total_reply_retweets"),
                avg("total_engagement").alias("avg_reply_engagement"),
                
                # Content analysis
                avg("text_length").alias("avg_reply_length"),
                avg("word_count").alias("avg_reply_words")
            )
            .orderBy(desc("created_date"), desc("total_replies"))
    )

# ==============================================================================
# 7. LAYER GOLD - Top Users (by Engagement)
# ==============================================================================

@dlt.table(
    name="twitter_gold_top_users",
    comment="Top users berdasarkan total engagement per keyword"
)
def gold_top_users():
    """
    Identifikasi top contributors berdasarkan engagement
    """
    
    from pyspark.sql.window import Window
    
    # Window untuk ranking per keyword
    window_spec = Window.partitionBy("keyword").orderBy(desc("total_engagement"))
    
    return (
        dlt.read("twitter_silver")
            .groupBy("keyword", "username", "user_name", "user_id")
            .agg(
                count("*").alias("post_count"),
                sum(when(col("type") == "tweet", 1).otherwise(0)).alias("tweet_count"),
                sum(when(col("type") == "reply", 1).otherwise(0)).alias("reply_count"),
                
                sum("like_count").alias("total_likes"),
                sum("retweet_count").alias("total_retweets"),
                sum("reply_count").alias("total_replies_received"),
                sum("total_engagement").alias("total_engagement"),
                
                avg("total_engagement").alias("avg_engagement_per_post"),
                max("created_at").alias("last_post_time")
            )
            .withColumn("user_rank", row_number().over(window_spec))
            .filter(col("user_rank") <= 50)  # Top 50 per keyword
            .orderBy("keyword", "user_rank")
    )

# ==============================================================================
# 8. LAYER GOLD - Conversation Threads
# ==============================================================================

@dlt.table(
    name="twitter_gold_conversation_threads",
    comment="Analisis conversation threads (tweet + replies-nya)"
)
def gold_conversation_threads():
    """
    Agregasi per root tweet untuk melihat engagement conversation
    """
    
    # Join tweets dengan replies-nya
    tweets_df = dlt.read("twitter_silver").filter(col("type") == "tweet")
    replies_df = dlt.read("twitter_silver").filter(col("type") == "reply")
    
    # Aggregate replies per root tweet
    reply_stats = replies_df.groupBy("root_tweet_id").agg(
        count("*").alias("direct_reply_count"),
        countDistinct("username").alias("unique_repliers"), # type: ignore
        sum("total_engagement").alias("replies_total_engagement")
    )
    
    # Join back dengan tweet asli
    return (
        tweets_df
            .join(reply_stats, tweets_df.tweet_id == reply_stats.root_tweet_id, "left")
            .select(
                col("keyword"),
                col("tweet_id"),
                col("username").alias("original_poster"),
                col("text").alias("original_tweet_text"),
                col("created_at"),
                col("created_date"),
                
                # Original tweet engagement
                col("like_count").alias("original_likes"),
                col("retweet_count").alias("original_retweets"),
                col("reply_count").alias("original_reply_count"),
                col("total_engagement").alias("original_engagement"),
                
                # Reply stats
                coalesce(col("direct_reply_count"), lit(0)).alias("actual_reply_count"),
                coalesce(col("unique_repliers"), lit(0)).alias("unique_repliers"),
                coalesce(col("replies_total_engagement"), lit(0)).alias("replies_engagement"),
                
                # Combined engagement
                (
                    col("total_engagement") + 
                    coalesce(col("replies_total_engagement"), lit(0))
                ).alias("conversation_total_engagement")
            )
            .orderBy(desc("conversation_total_engagement"))
    )

# ==============================================================================
# 9. LAYER GOLD - Trending Hashtags
# ==============================================================================

@dlt.table(
    name="twitter_gold_trending_hashtags",
    comment="Trending hashtags per keyword dan tanggal"
)
def gold_trending_hashtags():
    """
    Extract dan rank hashtags yang paling populer
    """
    
    return (
        dlt.read("twitter_silver")
            .select(
                "created_date",
                "keyword",
                "type",
                explode("hashtags").alias("hashtag")
            )
            .groupBy("created_date", "keyword", "hashtag")
            .agg(
                count("*").alias("hashtag_usage_count"),
                sum(when(col("type") == "tweet", 1).otherwise(0)).alias("in_tweets"),
                sum(when(col("type") == "reply", 1).otherwise(0)).alias("in_replies")
            )
            .filter(col("hashtag_usage_count") >= 3)  # Minimum threshold
            .orderBy(desc("created_date"), "keyword", desc("hashtag_usage_count"))
    )

# ==============================================================================
# 10. LAYER GOLD - Hourly Activity Pattern
# ==============================================================================

@dlt.table(
    name="twitter_gold_hourly_activity",
    comment="Pola aktivitas per jam untuk melihat peak hours"
)
def gold_hourly_activity():
    """
    Analisis aktivitas per jam untuk identifikasi peak hours
    """
    
    return (
        dlt.read("twitter_silver")
            .groupBy("keyword", "created_hour", "created_day_of_week")
            .agg(
                count("*").alias("total_posts"),
                sum(when(col("type") == "tweet", 1).otherwise(0)).alias("tweet_count"),
                sum(when(col("type") == "reply", 1).otherwise(0)).alias("reply_count"),
                avg("total_engagement").alias("avg_engagement")
            )
            .orderBy("keyword", "created_day_of_week", "created_hour")
    )

# ==============================================================================
# 10. LAYER GOLD - Hourly Activity Pattern
# ==============================================================================
def cleaned_text(text):
    if not text:
        return ""
    # Convert text to lowercase
    text = text.lower()
    # Remove tab, new line, and backslash
    text = text.replace('\\t', ' ').replace('\\n', ' ').replace('\\u', '').replace('\\', '')
    # Remove non ASCII characters
    text = text.encode('ascii', 'replace').decode('ascii')
    # Remove mention, link, hashtag
    text = ' '.join(re.sub("([@#][A-Za-z0-9]+)|(\w+:\/\/\S+)", " ", text).split())
    # Remove incomplete URL
    text = text.replace("http://", " ").replace("https://", " ")
    # Remove numbers
    text = re.sub(r"\d+", "", text)
    # Remove punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))
    # Remove leading and trailing whitespace
    text = text.strip()
    # Remove multiple whitespace into single whitespace
    text = re.sub('\s+', ' ', text)
    # Remove single characters
    text = re.sub(r"\b[a-zA-Z]\b", "", text)

    return text
    
#stopword
factory = StopWordRemoverFactory()
stopword_remover = factory.create_stop_word_remover()
def tokenize_stopword(text_cleaned):
    if not text_cleaned:
        return []
    stopword = stopword_remover.remove(text_cleaned)
    words = stopword.split()
    return words

#spark
clean_text = udf(cleaned_text,StringType())
tokenize = udf(tokenize_stopword,ArrayType(StringType()))
@dlt.table(
    name="twitter_gold_nlp_preprocessed",
    comment="Text yang sudah di-preprocess untuk NLP/ML (stopwords removed, cleaned)"
)
def gold_nlp_preprocess(text):
    """
    Heavy text preprocessing untuk NLP/ML analysis.
    Original text tetap ada di Silver layer untuk referensi.
    """
    
    df_cleaned = (
        dlt.read("twitter_silver")
            .withColumn("cleaned_text", clean_text(col("text")))
    )

    return(
        dlt.read("twitter_silver")
            .select(
                # IDs and metadata
                "tweet_id",
                "keyword",
                "type",
                "username",
                "created_at",
                "created_date",
                
                # Original text (tetap ada untuk reference)
                col("text").alias("original_text"),
                # Preprocessed versions
                col("cleaned_text"),
                tokenize(col("cleaned_text")).alias("stopword"),
                
                # Engagement metrics
                "total_engagement",
                "like_count",
                "retweet_count",
                "reply_count",
                
                # Language
                "language"
            )
            # Add derived features
            .withColumn("token_count", size(col("tokens")))
            .withColumn("cleaned_text_length", length(col("cleaned_text")))
            
            # Filter out empty cleaned text
            .filter(col("token_count") > 0)
    )










