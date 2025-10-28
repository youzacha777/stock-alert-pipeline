
```
stock-alert-pipeline
├─ config
│  └─ config.py
├─ dags
│  └─ stock_batch_analyzer_dag.py
├─ data_collectors
│  ├─ check_yfinance.py
│  ├─ consume_data_to_postgres.py
│  └─ stock_data_collector.py
├─ data_processors
│  └─ price_alert_processor.py
├─ db_conn
│  └─ db_conn.py
├─ docker-compose.yml
├─ Dockerfile.airflow
├─ Dockerfile.collector
├─ Dockerfile.dbconsumer
├─ Dockerfile.processor
├─ Dockerfile.spark
├─ init-scripts
│  ├─ 00-init-db.sh
│  └─ 01-create-tables.sql
├─ notifiers
│  └─ slack_notifier.py
├─ spark
│  └─ batch
│     └─ stock_batch_analyzer.py
└─ utils
   ├─ kafka_utils.py
   └─ log_utils.py

```