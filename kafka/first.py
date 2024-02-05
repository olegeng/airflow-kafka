from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json

admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='test')

# topic_list = []
# new_topic = NewTopic(name='atmres', num_partitions = 2, replication_factor=1)
# topic_list.append(new_topic)

# admin_client.create_topics(new_topics=topic_list)

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer.send('atmres', {'atmid':1, 'transid': 100})
producer.send('atmres', {'atmid':2, 'transid': 101})
producer.flush()
producer.close()

consumer = KafkaConsumer('atmres', group_id=None,
                            bootstrap_servers=['localhost:9092'],
                            auto_offset_reset = 'earliest')
print('Consumer is below')
print(consumer)

for msg in consumer:
    print(msg.value.decode("utf-8"))
