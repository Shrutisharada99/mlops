from confluent_kafka import Consumer, KafkaException, KafkaError
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

        messages = []
        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=1.0)  # Timeout in seconds

            if msg is None:
                break  # Exit if no new messages
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Successfully received message
                data = {
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': msg.value().decode('utf-8'),
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                }
                messages.append(data)
                # Log or process the message
                print(f"Received message: {data}")

        # Close the consumer after processing
        consumer.close()

        # Respond with the list of consumed messages
        #return HttpResponse(
        #    body=f"Consumed messages: {messages}",
        #    status_code=200,
        #    mimetype="application/json"
        #)

    except Exception as ex:
        # Handle exceptions
        print(f"Error while consuming messages: {ex}")
        consumer.close()
        #return func.HttpResponse(
        #    body=f"Error: {str(ex)}",
        #    status_code=500
        #)
