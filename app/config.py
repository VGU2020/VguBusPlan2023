# Confluent Kafka
CONSUMER_CONFIG = {
    "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "EFNEGZ54CAYOM4P7",
    "sasl.password": "AwqBCbLqDCI7MDVAaFqn7qGHVLY2RD06j63AAic8gAlBGNj8xmiVBg0p6+eSNvpw",
    "group.id": "bus",
    "auto.offset.reset": "earliest",
    "max.partition.fetch.bytes": 8388608,
}

PRODUCER_CONFIG = {
    "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "EFNEGZ54CAYOM4P7",
    "sasl.password": "AwqBCbLqDCI7MDVAaFqn7qGHVLY2RD06j63AAic8gAlBGNj8xmiVBg0p6+eSNvpw",
    "message.max.bytes": 8388608,
}


# Mongodb
URI = "mongodb+srv://kafka:1a2b3c4d@us.l7mluba.mongodb.net/?retryWrites=true&w=majority"
DB = "minnesota"
