!!! IMPORTANT, KAFKA PASS ONLY RAW DATA, DONT'T FORGET TO ENCODE IT INTO UTF-8 !!!

Create a KafkaAdminClient object


To use KafkaAdminClient, we first need to define and create a KafkaAdminClient object:

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

bootstrap_servers="localhost:9092"  -  argument specifies the host/IP and port that the consumer should contact to bootstrap initial cluster metadata
client_id - specifies an id of current admin client


Create new topics


topic_list = []
Then we use the NewTopic class to create a topic with name equals bankbranch,
partition nums equals to 2, and replication factor equals to 1.

new_topic = NewTopic(name="bankbranch", num_partitions= 2, replication_factor=1)
topic_list.append(new_topic)

At last, we can use create_topics(...) method to create new topics:
admin_client.create_topics(new_topics=topic_list)


Describe a topic
Once new topics are created, we can easily check its configuration details using describe_configs()
method

configs = admin_client.describe_configs(
    config_resources=[ConfigResource(ConfigResourceType.TOPIC, "bankbranch")])

Above describe topic operation is equivalent to using kafka-topics.sh --describe in Kafka CLI client:
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bankbranch


KafkaProducer

For kafka-python, we will use KafkaProducer class to produce messages.
Since many real-world message values are in the format of JSON, we will show you how to publish JSON messages as an example.

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

Since Kafka produces and consumes messages in raw bytes, we need to encode our JSON messages and serialize them
into bytes.
For the value_serializer argument, we define a lambda function to take a Python dict/list object and
serialize it into bytes.

Then, with the KafkaProducer created, we can use it to produce two ATM transaction messages in JSON format as follows:
producer.send("bankbranch", {'atmid':1, 'transid':100})
producer.send("bankbranch", {'atmid':2, 'transid':101})

The first argument specifies the topic bankbranch to be sent, and the second argument represents
the message value in a Python dict format and will be serialized into bytes.

The above producing message operation is equivalent to using kafka-console-producer.sh --topic in Kafka CLI client:
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch


KafkaConsumer
In the previous step, we published two JSON messages. Now we can use the KafkaConsumer class to
consume them.
We just need to define and create a KafkaConsumer subscribing to the topic bankbranch:

consumer = KafkaConsumer('bankbranch')

Once the consumer is created, it will receive all available messages from the topic bankbranch. Then we
can iterate and print them with the following code snippet:

for msg in consumer:
    print(msg.value.decode("utf-8"))

The above consuming message operation is equivalent to using kafka-console-consumer.sh --topic in Kafka CLI client:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch