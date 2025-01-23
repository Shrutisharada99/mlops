from confluent_kafka import Consumer, KafkaException
import json

def log_kafka_messages():
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
    log_file = 'kafka_messages.log'

    try:
        consumer.subscribe([topic])
        print("Listening for messages...")

        while True:
            msg = consumer.poll(timeout=1.0)  # Poll messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                # Decode and log the message
                message = msg.value().decode('utf-8')
                with open(log_file, 'a') as f:
                    f.write(f"{message}\n")
                print(f"Message logged: {message}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

log_kafka_messages()