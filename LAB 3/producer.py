# %%
import socket
import time
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='read_book'
filename = 'book.txt'

# %%
with open(filename, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            producer.produce(topic=topic, value=line)
            print(f"Sent: {line}")
            time.sleep(0.05)

producer.flush()
producer.close()