## Bitcoin Trend Alert System

System rozproszony służący do **monitorowania zmian w trendzie ceny i wolumenu Bitcoina** w czasie rzeczywistym. Wykorzystuje Apache Kafka jako system kolejkowania komunikatów oraz aplikacje Flask do produkcji i konsumpcji danych.

---

### Twórcy

* 141293
* 108591
* 140493
* 141285
* 140838

---


### Cel projektu

Celem aplikacji jest wykrywanie anomalii (odchyleń od trendu) w dziennych wartościach:

* ceny otwarcia (`open`)
* wolumenu (`volume`)

Na podstawie historii ostatnich 20 dni system automatycznie generuje alerty, jeśli aktualna wartość znacznąco odbiega od średniej — na przykład:

```json
{
  "timestamp": "2019-02-22",
  "alerts": [
    {
      "type": "open",
      "message": "Cena Bitcoina odbiega od trendu — Średnia: 3246.14€, Aktualna: 3561.60€"
    }
  ]
}
```

---

## Komponenty

### 1. `producer.py`

Aplikacja odpowiedzialna za:

* wczytanie danych z pliku CSV (`btceur.csv`)
* symulację wysyłania dziennych danych do Apache Kafka
* tworzenie tematu w Kafka, jeśli nie istnieje
* konfigurację producenta

#### Konfiguracja

Ustaw zmienną środowiskową:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

#### Uruchomienie

```bash
python3 producer.py
```

Aplikacja działa na porcie `5001` i oferuje endpointy diagnostyczne:

* `/health` – stan aplikacji
* `/current-time` – aktualna data symulacji
* `/stats` – licznik wysłanych dni, stan producenta itd.

---

### 2. `consumer.py`

Aplikacja służy do:

* odbierania wiadomości z Kafka (`btc_topic`)
* wykrywania anomalii (alertów) na podstawie trendu
* udostępniania danych i alertów przez REST API

#### Przykładowa wiadomość z Kafka

```json
{
  "time": "2019-01-01",
  "open": 3347.73,
  "close": 3347.80,
  "high": 3348.60,
  "low": 3347.00,
  "volume": 2.50
}
```

#### Przykładowy alert:

```json
{
  "timestamp": "2019-02-22",
  "alerts": [
    {
      "type": "open",
      "message": "Cena Bitcoina odbiega od trendu — Średnia: 3246.14€, Aktualna: 3561.60€"
    }
  ]
}
```

#### Konfiguracja

Ustaw zmienną środowiskową:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

#### Uruchomienie

```bash
python3 consumer.py
```

Aplikacja działa na porcie `5000` i udostępnia endpointy:

* `/messages` – lista ostatnich wiadomości z Kafka (max 1000)
* `/alerts` – wykryte alerty odchyleń od trendu (max 500)

---

## Architektura systemu

```plaintext
btceur.csv → producer.py → Kafka ("btc_topic") → consumer.py → Flask API
```

---

## Przykładowe dane

#### Endpoint: `/alerts`

```json
[
  {
    "timestamp": "2019-02-22",
    "alerts": [
      {
        "type": "open",
        "message": "Cena Bitcoina odbiega od trendu — Średnia: 3246.14€, Aktualna: 3561.60€"
      }
    ]
  }
]
```

#### Endpoint: `/messages`

```json
[
  {
    "time": "2019-01-01",
    "open": 3347.73,
    "close": 3347.80,
    "high": 3348.60,
    "low": 3347.00,
    "volume": 2.50
  }
]
```

---

## Wymagania

* Python 3.8+
* Apache Kafka (np. lokalnie przez Docker)
* Plik `btceur.csv` z historycznymi danymi BTC/EUR
* Biblioteki Python:

  * `flask`
  * `pandas`
  * `kafka-python`

---

## Uruchomienie w 3 krokach

1. Uruchom Apache Kafka.
2. Ustaw `KAFKA_BOOTSTRAP_SERVERS`.
3. Odpal:

   ```bash
   python3 producer.py
   python3 consumer.py
   ```
   
   Dla lepszej czytelności zalecane jest uruchomienie w osobnych terminalach

## Uruchomienie za pomocą Docker
1. W folderze projektu uruchom komende `docker-compose up --build`
2. Consumer oraz producer nie będą wstanie wstać, jeżeli wcześniej nie działa kafka. Skrpyt wait-for-kafka.sh pozwala na to, żeby uruchomić jedną komendą całyp projekt. Minus tego rozwiązania jest taki, że wyświetlają nam się logi trzech projektów na raz, w jednym terminalu 