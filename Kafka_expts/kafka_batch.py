from confluent_kafka import Consumer, KafkaException, KafkaError

def consume_kafka_data():
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
        'group.id': 'consumer-group-1',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'VPVOVS6AIQT3V577',
        'sasl.password': 'CscjmODsggXkRt214VwH6G7E8Pv0hpyWTKBwCsS/9zdBf92h3n6ziMffCczZUgAq',
    }

    consumer = Consumer(consumer_config)
    topic = 'Wellness_score'

    try:
        consumer.subscribe([topic])

        print("Waiting for messages...")
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll messages with a 1-second timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

consume_kafka_data()