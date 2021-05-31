from confluent_kafka import Consumer

# Niyeyse consumer.py çalıştırdıktan sonra producer.py' ı çalıştırınca böyle 10-15sn consumer hiç bir şey
#  almıyor. 10-15sn sonra kafkadaki verileri okuyor. İlginç.

kafka_advanced_listener = 'PLAINTEXT://localhost:29092'
topic_name = 'my_topic'

c = Consumer(
    {
    'bootstrap.servers': kafka_advanced_listener,
    'group.id': 'mygroup', # Her Consumer kafka' da bir group' un içindedir. Oluşturduğumuz Consumer' ın group adı
    'auto.offset.reset': 'earliest' # Ne olduğunu bilmiyorum.
    }
)

c.subscribe([topic_name]) # Bu consumer' ın bağlandığımız broker' daki hangi topic' ı okuyacağını belirtiyoruz.
a = 0
while True:
    
    msg = c.poll(5.0) # bu timeout sanırım ama ne işe yarar bilmiyorum
    
    if msg == None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    a+=1
    print(f"Received message: {msg.value().decode('utf-8')}, {a}")
    """
        msg.headers(), msg.key(), msg.latency(), msg.offset(), msg.partition(), msg.timestamp(), msg.topic(),
        msg.len() -> bir tek bu fonksiyon çalışmadı anlamadım.
    """

c.close() # Buraya hiç girmiyor.