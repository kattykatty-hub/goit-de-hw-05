"""Kafka connection settings from the GoIT lesson; password supplied via .env."""
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).with_name(".env"))

kafka_config = {
    "bootstrap_servers": [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092")],
    "username": os.getenv("KAFKA_USERNAME", "admin"),
    "password": os.getenv("KAFKA_PASSWORD"),
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}
if not kafka_config["password"]:
    raise RuntimeError("Set KAFKA_PASSWORD in your local .env file (see .env.example)")

PREFIX = "katty"
BUILDING_SENSORS = f"{PREFIX}_building_sensors"
TEMPERATURE_ALERTS = f"{PREFIX}_temperature_alerts"
HUMIDITY_ALERTS = f"{PREFIX}_humidity_alerts"

def client_settings():
    return {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
        "sasl_mechanism": kafka_config["sasl_mechanism"],
        "sasl_plain_username": kafka_config["username"],
        "sasl_plain_password": kafka_config["password"],
    }
