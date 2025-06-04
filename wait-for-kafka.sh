#!/bin/bash
# wait-for-kafka.sh

set -e

host="$1"
shift
cmd="$@"

echo "Czekanie na Kafka na $host..."

until python3 -c "
import socket
import sys
import time

def check_kafka_port():
    try:
        host_port = '$host'.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host_port[0], int(host_port[1])))
        sock.close()
        return result == 0
    except Exception as e:
        print(f'Błąd sprawdzania portu: {e}')
        return False

def check_kafka_topics():
    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable

        consumer = KafkaConsumer(
            bootstrap_servers='$host',
            consumer_timeout_ms=5000
        )
        consumer.topics()
        consumer.close()
        return True
    except NoBrokersAvailable:
        return False
    except Exception as e:
        print(f'Błąd sprawdzania Kafka: {e}')
        return False

# Sprawdź port
if not check_kafka_port():
    print('Port Kafka niedostępny')
    sys.exit(1)

print('Port Kafka dostępny, sprawdzanie brokerów...')

# Sprawdź brokery
max_attempts = 30
for attempt in range(max_attempts):
    if check_kafka_topics():
        print('Kafka jest gotowa!')
        sys.exit(0)
    print(f'Próba {attempt + 1}/{max_attempts}: Kafka jeszcze nie gotowa...')
    time.sleep(2)

print('Kafka nie jest gotowa po 30 próbach')
sys.exit(1)
"; do
  echo "Kafka is unavailable - sleeping"
  sleep 5
done

echo "Kafka is up - executing command"
exec $cmd