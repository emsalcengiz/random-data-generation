from confluent_kafka.admin import NewTopic, AdminClient


kafka_advanced_listener = 'PLAINTEXT://localhost:29092' # Kafka' ya erişmek için kullanılacak bağlantı.
topic_name = 'my_topic' # Kafka Broker' da oluşturulacak olan Topic

partition_counts = 1 # Oluşturacağımız Topic' in partition sayısı, bölüm sayısı


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