import dlt # type: ignore
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import requests
from pyspark.sql import SparkSession



# Dapatkan SparkSession untuk mengakses konfigurasi Spark
spark = SparkSession.builder.appName("MLInferencePipeline").getOrCreate()

SENTIMENT_API = "https://adb-1540106430068473.13.azuredatabricks.net/serving-endpoints/sentiment/invocations"

API_TOKEN = dbutils.secrets.get(scope="ml-models", key="api-token") # type: ignore

# ==============================================================================
# UDF: CALL MODEL SERVING APIs
# ==============================================================================

def create_model_api_caller(api_url, model_name):
    """Create UDF for calling Model Serving API"""
    
    def call_api(text):
        if not text or len(text.strip()) == 0:
            return None
        
        try:
            headers = {
                "Authorization": f"Bearer {API_TOKEN}",
                "Content-Type": "application/json"
            }
            
            payload = {"dataframe_records": [{"text": text}]}
            
            response = requests.post(api_url, headers=headers, json=payload, timeout=10)
            
            if response.status_code == 200:
                return response.json()["predictions"][0]
            else:
                print(f"❌ {model_name} API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ {model_name} exception: {str(e)}")
            return None
    
    return udf(call_api, StringType())

predict_sentiment = create_model_api_caller(SENTIMENT_API, "Sentiment")


# ==============================================================================
# GOLD LAYER - ML PREDICTIONS
# ==============================================================================

@dlt.table(
    name="ml_predictions",
    comment="ML predictions: sentiment, emotion, topic"
)
def ml_predictions():
    """
    Apply ML models to preprocessed text
    
    Input: twitter_gold_nlp_preprocessed (from Pipeline 1)
    Output: Predictions with sentiment, emotion, topic
    """
    
    # Read from Pipeline 1 output
    # Use dlt.read() if both pipelines in same DLT
    # Use spark.read.table() if separate DLT pipelines
    
    df = dlt.read.table("twitter_pipeline.twitter_gold_nlp_preprocessed")
    
    # Apply ML predictions
    df_with_predictions = df \
        .withColumn("sentiment_raw", predict_sentiment(col("stopword"))) \
        .withColumn("prediction_timestamp", current_timestamp()) \
        .withColumn(
            "sentiment",
            # Parsing string JSON output dari UDF ke nilai yang dapat dibaca
            regexp_replace(col("sentiment_raw"), '["]', '') 
        )
    
    return df_with_predictions.select(
        # IDs
        "tweet_id",
        "keyword",
        "username",
        "created_at",
        "created_date",
        
        # Text
        "original_text",
        "cleaned_text",
        "stopword"
        "token_count",
        
        # ML Predictions
        "sentiment",
        # "emotion",
        # "topic",
        
        # Metadata
        "prediction_timestamp",
        "preprocessed_at",
        
        # Engagement
        "total_engagement",
        "like_count"
    )

# ==============================================================================
# GOLD LAYER - SENTIMENT ANALYTICS
# ==============================================================================

@dlt.table(name="ml_sentiment_analytics")
def ml_sentiment_analytics():
    """Sentiment trends over time"""
    
    return (
        dlt.read("ml_predictions")
            .groupBy("created_date", "keyword", "sentiment")
            .agg(
                count("*").alias("post_count"),
                avg("total_engagement").alias("avg_engagement"),
                sum("like_count").alias("total_likes")
            )
            .withColumn(
                "sentiment_score",
                when(col("sentiment") == "positive", 1)
                .when(col("sentiment") == "neutral", 0)
                .when(col("sentiment") == "negative", -1)
                .otherwise(0)
            )
            .orderBy(desc("created_date"), "keyword")
    )
