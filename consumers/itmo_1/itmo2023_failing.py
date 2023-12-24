from kafka import KafkaConsumer

from utils.backoff import backoff


@backoff(tries=5, sleep=2)
def message_handler(value):
    # send to http get (rest api) to get response
    # save to db message (kafka) + external
    raise Exception("Cannot connect to database")


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
        message_handler(message)


if __name__ == '__main__':
    create_consumer()
