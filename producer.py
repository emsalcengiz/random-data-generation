from confluent_kafka import Producer


def delivery_report(err, msg):
    if err != None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to topic: {}, partition: [{}]'.format(msg.topic(), msg.partition()))


kafka_advanced_listener = 'PLAINTEXT://localhost:29092'
topic_name = 'my_topic'
partition = 1

p = Producer(
    {'bootstrap.servers': kafka_advanced_listener} # Producer' ın veriyi yazacağı broker
)

some_data = ['x', 'y']

for data in some_data:
    g = p.poll(0)

    p.produce(
        topic = topic_name,
        value = data.encode('utf-8'),
        callback = delivery_report
    )

p.flush()