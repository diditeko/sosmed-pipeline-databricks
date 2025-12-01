repo struc

social-media-pipeline/
│
├── README.md
├── requirements.txt
├── .env
├── .gitignore
│
├── configs/
│   ├── env
│       ├── dev.yaml
│       ├── staging.yaml
│       ├── prod.yaml
│   ├── databricks_config.yaml
│   ├── kafka_config.yaml
│   ├── mongodb_config.yaml            
│   ├── elasticsearch_config.yaml      
│   └── paths.yaml                     
│
├── crawling_services/
│   ├── crawler_twitter.py
│   ├── crawler_youtube.py
│   ├── crawler_tiktok.py              
│   └── producer_kafka.py
│
├── databricks_jobs/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   ├── gold_to_mongodb.py             # NEW: Sink Gold → MongoDB
│   ├── mongodb_to_elasticsearch.py    # NEW: Sync Mongo → Elastic
│   ├── model_register.py
│   ├── utils_spark.py
│   └── __init__.py
│
├── ml_models/
│   ├── train_sentiment.py
│   ├── sentiment_wrapper.py
│   ├── evaluate_model.py
│   └── __init__.py
│
├── connectors/
│   ├── mongo_client.py                # NEW: MongoDB connector util
│   ├── elastic_client.py              # NEW: Elasticsearch connector util
│   └── __init__.py
│
├── sinks/
│   ├── sink_to_mongodb.py             # NEW: Gold layer writer
│   ├── sink_to_elasticsearch.py       # NEW: ES bulk indexer
│   └── __init__.py
│
├── notebooks/
│   ├── bronze_exploration.ipynb
│   ├── silver_cleaning.ipynb
│   ├── gold_prediction.ipynb
│   ├── sink_mongodb_test.ipynb         # NEW: Test sink to Mongo
│   └── search_test_elastic.ipynb       # NEW: Test query Elasticsearch
│
├── dags/
│   ├── etl_pipeline_dag.py
│   ├── sync_to_elastic_dag.py         # NEW: schedule Mongo → Elastic sync
│
├── infra/
│   ├── docker-compose.yaml            # NEW: MongoDB + Elastic + Kibana
│   ├── mongodb_init.js                # NEW: init user + indexes
│   ├── elastic_mappings.json          # NEW: index mapping template
│   └── README.md
│
├── tests/
│   ├── test_bronze_cleaning.py
│   ├── test_ml_inference.py
│   ├── test_mongo_connection.py        # NEW
│   ├── test_elastic_indexing.py        # NEW
│   └── __init__.py
│
└── utils/
    ├── io_helpers.py
    ├── logger.py
    └── schema_definitions.py
