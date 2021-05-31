from confluent_kafka.admin import NewTopic, AdminClient


kafka_advanced_listener = 'PLAINTEXT://localhost:29092'
topic_name = 'my_topic'

partition_counts = 1 


broker = AdminClient(
    {'bootstrap.servers': kafka_advanced_listener}
)

new_topics = NewTopic(
    topic = topic_name,
    num_partitions = partition_counts
)

fs = broker.create_topics([new_topics])

try:
    fs[topic_name].result()
    print(f"Topic-> {topic_name} created")
except Exception as e:
    print(f"Failed to create to {topic_name}: ", e)
