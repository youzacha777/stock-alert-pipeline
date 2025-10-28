#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
설정 파일
Stock Alert Pipeline 프로젝트에서 사용되는 설정값들을 정의
"""

import os
from dotenv import load_dotenv

# .env 파일 로드 (존재하는 경우)
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
STOCK_PRICES_TOPIC = os.getenv('STOCK_PRICES_TOPIC', 'stock-prices')

# PostgreSQL 설정
DB_HOST = "postgres"
DB_NAME = "stockdb"
DB_USER = "stockuser"
DB_PASSWORD = "stockpassword"
DB_TABLE = "stock_prices"


# 데이터 수집 설정
STOCK_TICKERS = os.getenv('STOCK_TICKERS', 'AAPL,MSFT,GOOGL').split(',')
STOCK_COLLECTION_INTERVAL = int(os.getenv('STOCK_COLLECTION_INTERVAL', 60)) # 1분 간격

# 알림 설정
PRICE_CHANGE_THRESHOLD = float(os.getenv('PRICE_CHANGE_THRESHOLD', 0.01)) # 가격 변동 임계값 (%)
VOLUME_CHANGE_THRESHOLD = float(os.getenv('VOLUME_CHANGE_THRESHOLD', 1.0)) # 거래량 변동 임계값 (%)
TECHNICAL_INDICATOR_THRESHOLD = float(os.getenv('TECHNICAL_INDICATOR_THRESHOLD', 1.0)) # 기술적 지표 임계값 (%)

# 이메일 알림 설정
# EMAIL_SENDER = os.getenv('EMAIL_SENDER', '')
# EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
# EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT', '')
# EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER', 'stmp.gmail.com')
# EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', 587))

# Slack 알림 설정
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', 'your_slack_webhook_url')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', '#stock-data-pipeline')

# 일일 보고서 설정
DAILY_REPORT_TIME = os.getenv('DAILY_REPORT_TIME', '16:30') # 일일 보고서 생성 시간 (시:분)
DAILY_REPORT_TIMEZONE = os.getenv('DAILY_REPORT_TIMEZONE', 'America/New_York') # 일일 보고서 생성 시간대

# 로깅 설정
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE_MAX_BYTES = int(os.getenv('LOG_FILE_MAX_BYTES', 10485760)) # 10MB
LOG_FILE_BACKUP_COUNT = int(os.getenv('LOG_FILE_BACKUP_COUNT', 5))

# 이동평균선 설정
MA_SHORT_PERIOD = int(os.getenv('MA_SHORT_PERIOD', 10)) # 단기 이동평균선 기간
MA_LONG_PERIOD = int(os.getenv('MA_LONG_PERIOD', 50)) # 장기 이동평균선 기간

# RSI 설정
RSI_PERIOD = int(os.getenv('RSI_PERIOD', 14)) # RSI 계산 기간
RSI_OVERBOUGHT = float(os.getenv('RSI_OVERBOUGHT', 70)) # RSI 과매수 수준
RSI_OVERSOLD = float(os.getenv('RSI_OVERSOLD', 30.0)) # RSI 과매도 수준

# MACD 설정
MACD_FAST_PERIOD = int(os.getenv('MACD_FAST_PERIOD', 12)) # MACD 빠른 선 기간
MACD_SLOW_PERIOD = int(os.getenv('MACD_SLOW_PERIOD', 26)) # MACD 느린 선 기간
MACD_SIGNAL_PERIOD = int(os.getenv('MACD_SIGNAL_PERIOD', 9)) # MACD 신호선 기간

# 환경 설정
IS_PRODUCTION = os.getenv('IS_PRODUCTION', 'False').lower() in ('true', 't', '1', 'yes', 'y')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() in ('true', 't', '1', 'yes', 'y')