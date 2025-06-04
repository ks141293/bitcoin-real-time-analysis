from flask import Flask, jsonify
from kafka import KafkaConsumer
import threading
import json
import statistics
import os

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = "btc_topic"

received_messages = []
alerts = []
MAX_MESSAGES = 1000
open_history = []
volume_history = []
HISTORY_SIZE = 20


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer_group'
    )


def parse_message(data):
    try:
        current_open = float(data.get("open", 0))
        current_volume = float(data.get("volume", 0))
        return current_open, current_volume
    except (ValueError, TypeError):
        return None, None


def is_outlier(value, history, threshold=1.5):
    if len(history) < HISTORY_SIZE:
        return False
    mean = statistics.mean(history)
    std = statistics.stdev(history)
    if std == 0:
        return False
    return abs(value - mean) > threshold * std


def check_trend_alerts(current_open, current_volume, timestamp_str):
    divider = "-" * 60
    alert_triggered = False
    alert_msg = {
        "timestamp": timestamp_str,
        "alerts": []
    }

    is_open_outlier = is_outlier(current_open, open_history)
    is_volume_outlier = is_outlier(current_volume, volume_history)

    if is_open_outlier or is_volume_outlier:
        alert_triggered = True

        if is_open_outlier:
            mean_open = statistics.mean(open_history)
            msg = f"Cena Bitcoina odbiega od trendu — Średnia: {mean_open:.2f}€, Aktualna: {current_open:.2f}€"
            alert_msg["alerts"].append({"type": "open", "message": msg})
            print(f"{divider}\nALERT — {timestamp_str}\n• {msg}\n{divider}")

        if is_volume_outlier:
            mean_volume = statistics.mean(volume_history)
            msg = f"Wolumen odbiega od trendu — Średnia: {mean_volume:.2f}, Aktualna: {current_volume:.2f}"
            alert_msg["alerts"].append({"type": "volume", "message": msg})
            print(f"{divider}\nALERT — {timestamp_str}\n• {msg}\n{divider}")

    if alert_triggered:
        alerts.append(alert_msg)
        if len(alerts) > 500:
            alerts.pop(0)


def consume():
    consumer = create_consumer()

    for message in consumer:
        data = message.value
        current_open, current_volume = parse_message(data)

        if current_open is None or current_volume is None:
            continue

        timestamp_str = data.get("time", "brak_timestampu")
        check_trend_alerts(current_open, current_volume, timestamp_str)


        open_history.append(current_open)
        volume_history.append(current_volume)

        if len(open_history) > HISTORY_SIZE:
            open_history.pop(0)
        if len(volume_history) > HISTORY_SIZE:
            volume_history.pop(0)

        received_messages.append(data)
        if len(received_messages) > MAX_MESSAGES:
            received_messages.pop(0)


consumer_thread_started = False


def start_consumer_once():
    global consumer_thread_started
    if not consumer_thread_started:
        thread = threading.Thread(target=consume)
        thread.daemon = True
        thread.start()
        consumer_thread_started = True


@app.before_request
def before_request_func():
    start_consumer_once()

@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(received_messages)

@app.route('/alerts', methods=['GET'])
def get_alerts():
    return jsonify(alerts)


if __name__ == "__main__":
    start_consumer_once()
    app.run(host="0.0.0.0", port=5000)
