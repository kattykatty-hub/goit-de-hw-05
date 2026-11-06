import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from src.configs import client_settings, BUILDING_SENSORS


def main():
    parser = argparse.ArgumentParser(description="Simulate exactly one IoT sensor per process")
    parser.add_argument("--sensor-id", type=int, default=None)
    parser.add_argument("--building-id", default="building_A")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    sensor_id = args.sensor_id if args.sensor_id is not None else random.randint(10000, 99999)
    producer = KafkaProducer(
        **client_settings(),
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    print(f"Started sensor {sensor_id} in {args.building_id}; Ctrl+C to stop", flush=True)
    try:
        while True:
            reading = {
                "sensor_id": sensor_id,
                "building_id": args.building_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": round(random.uniform(25, 45), 2),
                "humidity": round(random.uniform(15, 85), 2),
            }
            metadata = producer.send(BUILDING_SENSORS, key=sensor_id, value=reading).get(timeout=10)
            print(f"SENT sensor={sensor_id} partition={metadata.partition} offset={metadata.offset} data={reading}", flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Sensor stopped")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
