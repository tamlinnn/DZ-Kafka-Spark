from confluent_kafka import Consumer
import uuid
import json


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fast',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['subject'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    myId = uuid.uuid4()
    value=json.dumps(msg.value().decode('utf-8'))
    print(value)
        
c.close()
conn.close()
