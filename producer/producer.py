import json
import time
import uuid
import argparse
from kafka import KafkaProducer

def now_ms() -> int:
    return int(time.time() * 1000)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="events_topic")
    parser.add_argument("--rate", type=int, default=1000, help="events per second")
    parser.add_argument("--size", type=int, default=200, help="approx payload size in bytes")
    parser.add_argument("--minutes", type=int, default=3, help="run duration in minutes")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks=1,
        retries=3,
    )

    # Create a filler string to control payload size (approx)
    filler_len = max(0, args.size - 120)
    filler = "x" * filler_len

    interval = 1.0 / max(1, args.rate)
    end_time = time.time() + args.minutes * 60

    sent = 0
    next_emit = time.time()

    while time.time() < end_time:
        event = {
            "event_id": str(uuid.uuid4()),
            "created_ts": now_ms(),
            "user_id": sent % 10000,
            "event_type": ["click", "view", "purchase", "login"][sent % 4],
            "value": float(sent % 1000) / 10.0,
            "payload": filler
        }

        producer.send(args.topic, event)
        sent += 1

        # Rate control
        next_emit += interval
        sleep_for = next_emit - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)

    producer.flush()
    print(f"Done. Sent {sent} events to topic={args.topic} at rate≈{args.rate}/s for {args.minutes} min")

if __name__ == "__main__":
    main()
