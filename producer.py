#!/usr/bin/env python3
from flask import Flask, jsonify
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import threading
import time
import json
import os

app = Flask(__name__)

# Konfiguracja Kafka - używa zmiennej środowiskowej
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = "btc_topic"

# Globalne zmienne
producer = None
df = None
current_simulated_time = None
sent_days = set()
INTERVAL_REAL = 1
INTERVAL_SIM = timedelta(days=1)


def load_csv_data():
    """Wczytuje i przetwarza dane CSV"""
    global df, current_simulated_time
    try:
        # Wczytaj dane z poprawnym kodowaniem i separatorem
        df = pd.read_csv("btceur.csv", encoding='windows-1250', delimiter=';')

        # Napraw format kolumn: zamień przecinki na kropki w każdej kolumnie poza 'time'
        for col in df.columns:
            if col != 'time':
                df[col] = df[col].astype(str).str.replace(",", ".", regex=False).astype(float)

        # Przekształć kolumnę 'time' na float i datetime
        df['time'] = df['time'].astype(str).str.replace(",", ".", regex=False).astype(float)
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        df = df.set_index('time')

        # Start symulacji od określonej daty
        current_simulated_time = df.index.min()

        print(f"Wczytano {len(df)} rekordów z CSV")
        print(f"Zakres dat: {df.index.min()} - {df.index.max()}")

    except Exception as e:
        print(f"Błąd podczas wczytywania CSV: {e}")
        raise


def wait_for_kafka():
    """Czeka na dostępność Kafka z retry mechanizmem"""
    max_retries = 30
    retry_interval = 5

    for attempt in range(max_retries):
        try:
            # Próba utworzenia tymczasowego klienta admin
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=5000
            )
            # Próba listowania tematów
            admin_client.list_topics()
            admin_client.close()
            print(f"Połączono z Kafka na {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except Exception as e:
            print(f"Próba {attempt + 1}/{max_retries}: Kafka niedostępna - {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)

    raise Exception(f"Nie można połączyć z Kafka po {max_retries} próbach")


def create_topic_if_not_exists():
    """Funkcja do tworzenia tematu jeśli nie istnieje"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME not in existing_topics:
            topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Temat '{TOPIC_NAME}' został utworzony.")
        else:
            print(f"Temat '{TOPIC_NAME}' już istnieje.")
        admin_client.close()
    except Exception as e:
        print(f"Błąd przy tworzeniu tematu Kafka: {e}")


def create_producer():
    """Tworzy producenta Kafka"""
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=30000,
            retry_backoff_ms=1000,
            retries=5
        )
        print("Producer Kafka został utworzony")
    except Exception as e:
        print(f"Błąd przy tworzeniu producenta Kafka: {e}")
        raise


def start_data_stream():
    """Główna pętla wysyłania danych"""
    global current_simulated_time

    if df is None or producer is None:
        print("DataFrame lub Producer nie został zainicjalizowany")
        return

    print("Rozpoczynam wysyłanie danych...")

    while True:
        try:
            current_day = current_simulated_time.date()
            previous_day = current_day - timedelta(days=1)

            if previous_day not in sent_days:
                start_of_day = pd.Timestamp(previous_day)
                end_of_day = start_of_day + timedelta(days=1)
                daily_data = df.loc[start_of_day:end_of_day - timedelta(microseconds=1)]

                if not daily_data.empty:
                    avg_row = daily_data.mean(numeric_only=True).to_dict()
                    avg_row['time'] = previous_day.isoformat()
                    print(f"Wysyłanie średniej dziennej za {previous_day}")

                    try:
                        producer.send(TOPIC_NAME, value=avg_row)
                        producer.flush()
                        print(f"Wysłano dane za {previous_day}")
                    except Exception as e:
                        print(f"Błąd podczas wysyłania do Kafki: {e}")
                else:
                    print(f"Brak danych dla {previous_day}, pomijam.")

                sent_days.add(previous_day)

            current_simulated_time += INTERVAL_SIM
            time.sleep(INTERVAL_REAL)

        except Exception as e:
            print(f"Błąd w pętli wysyłania danych: {e}")
            time.sleep(5)


def initialize_kafka_and_data():
    """Inicjalizuje Kafka i dane"""
    print("Inicjalizacja systemu...")

    # Wczytaj dane CSV
    load_csv_data()

    # Czekaj na Kafka
    wait_for_kafka()

    # Stwórz temat
    create_topic_if_not_exists()

    # Stwórz producenta
    create_producer()

    print("Inicjalizacja zakończona pomyślnie")


@app.before_request
def activate_job():
    if not hasattr(app, 'data_thread_started'):
        try:
            # Inicjalizuj system
            initialize_kafka_and_data()

            # Uruchom wątek wysyłania danych
            thread = threading.Thread(target=start_data_stream)
            thread.daemon = True
            thread.start()
            app.data_thread_started = True
            print("Wątek wysyłania danych został uruchomiony")
        except Exception as e:
            print(f"Błąd podczas inicjalizacji: {e}")


@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "service": "producer",
        "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": TOPIC_NAME
    })


@app.route('/current-time', methods=['GET'])
def get_current_time():
    if current_simulated_time:
        return jsonify({"current_simulated_time": current_simulated_time.isoformat()})
    else:
        return jsonify({"error": "System nie został jeszcze zainicjalizowany"}), 500


@app.route('/stats', methods=['GET'])
def get_stats():
    return jsonify({
        "sent_days_count": len(sent_days),
        "current_simulated_time": current_simulated_time.isoformat() if current_simulated_time else None,
        "data_loaded": df is not None,
        "producer_ready": producer is not None
    })


if __name__ == "__main__":
    print("Uruchamianie Producer...")
    print(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    app.run(host="0.0.0.0", port=5001, debug=False)