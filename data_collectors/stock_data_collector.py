#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 데이터 수집기 모듈
yfinance 라이브러리를 사용해 주식 데이터를 수집하고 Kafka로 전송
"""

import yfinance as yf
import json
import time
import sys
import os
import logging
import time
import random 
from datetime import datetime
from kafka import KafkaProducer
# from curl_cffi import requests

# 상위 디렉토리를 path에 추가해 다른 모듈을 import 할 수 있도록 경로 설정

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS, STOCK_PRICES_TOPIC, STOCK_TICKERS, STOCK_COLLECTION_INTERVAL

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stock_collector.log")
    ]
)
logger = logging.getLogger("stock_collector")

def create_kafka_producer():
    """Kafka Producer 생성"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"Kafka Producer 연결 성공 : {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Kafka Producer 연결 실패: {e}")
        return None
    

def collect_stock_data(producer):
    if not producer:
        logger.error("Kafka Producer가 없습니다. 데이터 수집을 중단합니다.")
        return

    logger.info(f"다음 종목들의 데이터 수집 시작: {', '.join(STOCK_TICKERS)}")
    logger.info(f"현재 티커: {STOCK_TICKERS}, 수집 인터벌: {STOCK_COLLECTION_INTERVAL}초")

    while True:
        for ticker_symbol in STOCK_TICKERS:
            try:
                ticker = yf.Ticker(ticker_symbol)
                data = ticker.history(period="1d", interval="5m")

                if not data.empty:
                    latest = data.iloc[-1]

                    message = {
                        'ticker': ticker_symbol,  # ✅ 문자열로 변경
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'open': float(latest['Open']),
                        'high': float(latest['High']),
                        'low': float(latest['Low']),
                        'close': float(latest['Close']),
                        'volume': int(latest['Volume']),
                        'change_pct': float((latest['Close'] - data.iloc[-2]['Close']) / data.iloc[-2]['Close'] * 100)
                        if len(data) > 1 else 0.0
                    }

                    producer.send(STOCK_PRICES_TOPIC, message)
                    logger.info(f"종목 {ticker_symbol} 데이터 전송 완료: 현재가 {message['close']:.2f}, 변동률 {message['change_pct']:.2f}%")
                else:
                    logger.warning(f"종목 {ticker_symbol}에 대한 데이터를 가져올 수 없습니다.")

                logger.info(f"{STOCK_COLLECTION_INTERVAL}s 대기 후 다음 티커 수집")
                time.sleep(180)

            except Exception as e:
                logger.error(f"종목 {ticker_symbol} 데이터 수집 중 오류 발생: {e}")
                time.sleep(180)

        logger.info("다음 데이터 재수집을 시작합니다.")



def main():
    """메인 함수"""
    logger.info("주식 데이터 수집기 시작")
    producer = create_kafka_producer()

    try:
        collect_stock_data(producer)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다.")
    except Exception as e:
        logger.info(f"예상치 못한 오류 발생: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka Producer 연결 종료")

if __name__ == "__main__":
    main()