from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from src.configs import client_settings, BUILDING_SENSORS, TEMPERATURE_ALERTS, HUMIDITY_ALERTS, PREFIX


def main():
    admin_client = KafkaAdminClient(**client_settings(), client_id="katty-admin")
    try:
        topics = [BUILDING_SENSORS, TEMPERATURE_ALERTS, HUMIDITY_ALERTS]
        for name in topics:
            try:
                admin_client.create_topics([NewTopic(name=name, num_partitions=3, replication_factor=1)])
                print(f"CREATED: {name}")
            except TopicAlreadyExistsError:
                print(f"EXISTS: {name}")
        print("\nTopics matching my identifier:")
        [print(topic) for topic in sorted(admin_client.list_topics()) if PREFIX in topic]
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
