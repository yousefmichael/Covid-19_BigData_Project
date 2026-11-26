import csv
import json
import time

from kafka import KafkaProducer

# ====== Settings ======
BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "covid_raw"
FILE_PATH = "data/covid_19.csv"

MAX_ROWS = None
DELAY_SECONDS = 0.05
MISSING_AS_ZERO = False
# ======================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def to_int(value):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return 0 if MISSING_AS_ZERO else None


def is_real_country(row):
    return bool(row["continent"].strip()) and row["country"] != "All"


sent = 0
skipped = 0

with open(FILE_PATH, mode="r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:
        if not is_real_country(row):
            skipped += 1
            continue

        message = {
            "date": row["day"].strip(),
            "country": row["country"].strip(),
            "confirmed": to_int(row["Cases"]),
            "deaths": to_int(row["Deaths"]),
            "recovered": to_int(row["Recovered"]),
        }

        producer.send(TOPIC, key=message["country"], value=message)
        sent += 1
        print(f"Sent {sent}: {message}")

        if MAX_ROWS is not None and sent >= MAX_ROWS:
            break

        time.sleep(DELAY_SECONDS)

producer.flush()
producer.close()

print(f"\nFinished. Sent: {sent} | Skipped (non-country rows): {skipped}")
