import json
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from src.configs import client_settings, BUILDING_SENSORS, TEMPERATURE_ALERTS, HUMIDITY_ALERTS


def main():
    consumer = KafkaConsumer(
        BUILDING_SENSORS,
        **client_settings(),
        group_id="katty-sensor-processors",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    producer = KafkaProducer(
        **client_settings(),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    print("Listening for readings... Ctrl+C to stop", flush=True)
    try:
        for record in consumer:
            reading = record.value
            temperature = float(reading["temperature"])
            humidity = float(reading["humidity"])
            print(f"RECEIVED sensor={reading['sensor_id']} temp={temperature} humidity={humidity}", flush=True)
            alerts = []
            if temperature > 40:
                alerts.append((TEMPERATURE_ALERTS, "temperature", f"Temperature {temperature}°C exceeds 40°C"))
            if humidity > 80 or humidity < 20:
                alerts.append((HUMIDITY_ALERTS, "humidity", f"Humidity {humidity}% is outside 20–80%"))
            for topic, alert_type, message in alerts:
                alert = {
                    "sensor_id": reading["sensor_id"],
                    "building_id": reading["building_id"],
                    "timestamp": reading["timestamp"],
                    "alert_timestamp": datetime.now(timezone.utc).isoformat(),
                    "temperature": temperature,
                    "humidity": humidity,
                    "alert_type": alert_type,
                    "message": message,
                }
                metadata = producer.send(topic, value=alert).get(timeout=10)
                print(f"FILTERED {alert_type.upper()} | SENT to {topic} partition={metadata.partition} offset={metadata.offset} | {json.dumps(alert)}", flush=True)
            if not alerts:
                print("FILTERED: no threshold violation", flush=True)
            # Commit only after all required alerts have been acknowledged by Kafka.
            consumer.commit()
    except KeyboardInterrupt:
        print("Processor stopped")
    finally:
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
