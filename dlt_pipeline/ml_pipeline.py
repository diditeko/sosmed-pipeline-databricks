import dlt # type: ignore
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import requests
from pyspark.sql import SparkSession
import pandas as pd



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
        if not text:
            return None
        
        try:
            headers = {
                "Authorization": f"Bearer {API_TOKEN}",
                "Content-Type": "application/json"
            }
            
            payload = {"dataframe_records": [{"cleaned_text": text}]}
            
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

def create_batch_sentiment_udf(api_url: str, api_token: str):
    """
    Create Pandas UDF for batch prediction
    
    Benefits:
    - Process multiple rows at once (faster!)
    - Better error handling
    - Reduce API calls (100 rows = 1 API call instead of 100 calls)
    """
    
    @pandas_udf(StringType())
    def predict_sentiment_batch(texts: pd.Series) -> pd.Series:
        """
        Batch predict sentiment for multiple texts
        """
        
        # Filter out empty texts
        valid_indices = texts.notna() & (texts.str.strip() != "")
        results = pd.Series([None] * len(texts), index=texts.index)
        
        if not valid_indices.any():
            return results
        
        valid_texts = texts[valid_indices].tolist()
        
        try:
            # Prepare batch payload
            headers = {
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json"
            }
            
            # Batch API call (all texts at once!)
            payload = {
                "dataframe_records": [{"cleaned_text": text} for text in valid_texts]
            }
            
            response = requests.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=30  # Longer timeout for batch
            )
            
            if response.status_code == 200:
                predictions = response.json()["predictions"]
                
                # Map predictions back to original indices
                for i, pred in enumerate(predictions):
                    original_idx = texts[valid_indices].index[i]
                    results[original_idx] = pred
                    
            else:
                print(f"❌ API Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"❌ Batch prediction error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return results
    
    return predict_sentiment_batch

# Create the UDF
predict_sentiment_batch = create_batch_sentiment_udf(SENTIMENT_API, API_TOKEN)

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
    
    df = dlt.read("sosial_media_pipeline.twitter_pipe.twitter_gold_nlp_preprocessed")
    
    # Apply ML predictions
    df_with_predictions = df \
        .withColumn("sentiment_raw", predict_sentiment(col("stopword_text"))) \
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
        "stopword_text",
        
        # ML Predictions
        "sentiment",
        # "emotion",
        # "topic",
        
        # Metadata
        "prediction_timestamp",
        
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

# ==============================================================================
# GOLD LAYER - ML PREDICTIONS (METHOD 1: Pandas UDF)
# ==============================================================================

@dlt.table(
    name="ml_predictions_batch",
    comment="ML predictions with batch processing for better performance"
)
def ml_predictions_batch():
    """
    Apply ML sentiment prediction using BATCH processing
    
    Performance improvements:
    - Pandas UDF: ~10x faster than regular UDF
    - Batch API calls: 100 rows = 1 call instead of 100 calls
    """
    
    # Read from Pipeline 1
    df = dlt.read("sosial_media_pipeline.twitter_pipe.twitter_gold_nlp_preprocessed")
    
    # Method 1: Pandas UDF (Recommended for most cases)
    df_with_predictions = df \
        .withColumn("sentiment", predict_sentiment_batch(col("stopword_text"))) \
        .withColumn("prediction_timestamp", current_timestamp())
    
    return df_with_predictions.select(
        "tweet_id",
        "keyword",
        "username",
        "created_at",
        "created_date",
        "original_text",
        "cleaned_text",
        "stopword_text",
        "sentiment",
        "prediction_timestamp",
        "total_engagement",
        "like_count"
    )

@dlt.table(name="ml_sentiment_analytics_batch")
def ml_sentiment_analytics():
    """Sentiment trends over time"""
    
    return (
        dlt.read("ml_predictions_batch")
            .filter(col("sentiment").isNotNull())  # Filter out nulls
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

