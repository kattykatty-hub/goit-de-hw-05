import json
from kafka import KafkaConsumer
from src.configs import client_settings, TEMPERATURE_ALERTS, HUMIDITY_ALERTS


def main():
    consumer = KafkaConsumer(
        TEMPERATURE_ALERTS,
        HUMIDITY_ALERTS,
        **client_settings(),
        group_id="katty-alert-display",
        auto_offset_reset="earliest",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    print("Listening for temperature and humidity alerts... Ctrl+C to stop", flush=True)
    try:
        for record in consumer:
            alert = record.value
            print(f"ALERT RECEIVED from {record.topic} | sensor={alert['sensor_id']} | time={alert['timestamp']} | temperature={alert['temperature']}°C | humidity={alert['humidity']}% | {alert['message']}", flush=True)
    except KeyboardInterrupt:
        print("Alert consumer stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
