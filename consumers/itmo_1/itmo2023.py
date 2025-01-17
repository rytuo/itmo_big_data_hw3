from kafka import KafkaConsumer


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(
        "itmo2023",
        group_id='itmo-1',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )

    for message in consumer:
        print(message)


if __name__ == '__main__':
    create_consumer()
