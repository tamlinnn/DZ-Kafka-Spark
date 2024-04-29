import csv 
import json
import cv2
import cv2 as cv
import base64
import  os

from uuid import uuid4
from confluent_kafka import Producer


def connect_kafka(connection_address: str):
    p = Producer({'bootstrap.servers': connection_address,
                    'message.max.bytes': 20971520
                  }
                 )
    return p

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def string_conversion(list_could: list):
    json_list={list_could[0].strip(): dict([pair for pair in enumerate(list_could[1:13])])}
    return json_list



def main():
    p=connect_kafka("localhost:9092")
    topic_name= "subject"

    with open('tend.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            p.produce(topic=topic_name, key=str(uuid4()).encode('utf-8'), 
                      value=bytes(json.dumps(string_conversion(row)), 'utf-8'),
                      callback=delivery_report)
    p.flush()

if __name__ == '__main__':
    main()
