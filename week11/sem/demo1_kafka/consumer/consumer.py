import sys
import json
from confluent_kafka import Consumer, KafkaError
from influxdb import InfluxDBClient

client = InfluxDBClient(host='localhost', port=8086, database='logdb')
client.create_database('logdb')

c = Consumer({
    'bootstrap.servers': 'localhost:29094',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
c.subscribe(['LOG_TABLE_DEVICE_COUNT'])

try:
    while True:
        print('Waiting for message..')
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
    
        json_msg = json.loads(msg.value())
        json_msg.pop('IsActive')
        
        json_body = [
        {
            "measurement": "device-count",
            "tags": {
            },
            "fields": {'ResourceProvider': json_msg['ResourceProvider']}
        }
        ]
        print(json_body)
        client.write_points(json_body)

except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

finally:
        c.close()
