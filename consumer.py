from confluent_kafka import Consumer



kafka_advanced_listener = 'PLAINTEXT://localhost:29092'
topic_name = 'my_topic'

c = Consumer(
    {
    'bootstrap.servers': kafka_advanced_listener,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest' 
    }
)

c.subscribe([topic_name]) 
while True:
    
    msg = c.poll(5.0)
    
    if msg == None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    print(f"Received message: {msg.value().decode('utf-8')}, {msg.partition()}")
    """
        msg.headers(), msg.key(), msg.latency(), msg.offset(), msg.timestamp(), msg.topic(),
        msg.len() -> bir tek bu fonksiyon çalışmadı anlamadım.
    """

c.close()
