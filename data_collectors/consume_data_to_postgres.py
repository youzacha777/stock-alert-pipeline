from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_values
import logging
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS, DB_TABLE, STOCK_PRICES_TOPIC
from db_conn.db_conn import get_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("db_stock_collector.log")
    ]
)
logger = logging.getLogger("db_stock_collector")


# Kafka Consumer 생성

def create_kafka_consumer():
    """Kafka Consumer 생성"""
    try:
        consumer = KafkaConsumer(
            STOCK_PRICES_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="postgresql_rawdata",
            auto_offset_reset='earliest',  # 가장 최근 메시지부터 시작
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info(f"Kafka Consumer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}.postgresql_rawdata")
        return consumer
    except Exception as e:
        logger.error(f"Kafka Consumer 연결 실패: {e}")
        return None


def save_data_to_postres():
    """Kafka 메시지를 PostgreSQL에 저장"""
    consumer = create_kafka_consumer()
    if consumer is None:
        logger.error("Consumer 생성 실패")
        return None

    # PostgreSQL 연결
    conn = get_connection()
    cur = conn.cursor()

    # Kafka 메시지 처리 및 PostgreSQL 적재
    batch = []
    BATCH_SIZE = 1  # 한 번에 100개씩 INSERT

    try:
        for message in consumer:
            data = message.value
            ts = datetime.fromisoformat(data['timestamp'])
            batch.append((
                data['ticker'],     
                ts.date(),
                ts,
                data['open'],
                data['high'],
                data['low'],
                data['close'],
                data['volume']
            ))

            if len(batch) >= BATCH_SIZE:
                execute_values(cur,
                    f"INSERT INTO {DB_TABLE} (ticker, date, timestamp, open, high, low, close, volume) VALUES %s",
                    batch
                )
                conn.commit()
                logger.info(f"{len(batch)}개 데이터 INSERT 완료")
                batch = []

    except KeyboardInterrupt:
        logger.info("사용자에 의해 종료 요청")
    except Exception as e:
        logger.error(f"데이터 저장 중 오류 발생: {e}")
    finally:
        # 남은 배치 처리
        if batch:
            execute_values(cur,
                f"""
                INSERT INTO {DB_TABLE}
                    (ticker, date, timestamp, open, high, low, close, volume)
                VALUES %s
                """,
                batch
            )
            conn.commit()
            logger.info(f"남은 {len(batch)}개 데이터 INSERT 완료")

        cur.close()
        conn.close()
        consumer.close()
        logger.info("PostgreSQL 연결 및 Kafka Consumer 종료")

def main():
    logger.info("주식 데이터 수집기 시작")
    save_data_to_postres()


if __name__ == "__main__":
    save_data_to_postres()


