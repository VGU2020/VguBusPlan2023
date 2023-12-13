# Confluent Kafka
CONSUMER_CONFIG = {
    "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "WJJNVHLNUDCYW3O6",
    "sasl.password": "WDC0hL9zM7IVxAFMvLftqcMuIFppxGbEFNbbukFqfT8Y3jmZLjl0iLXuzp7dWVzO",
    "group.id": "bus",
    "auto.offset.reset": "earliest"
}

PRODUCER_CONFIG = {
    "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "WJJNVHLNUDCYW3O6",
    "sasl.password": "WDC0hL9zM7IVxAFMvLftqcMuIFppxGbEFNbbukFqfT8Y3jmZLjl0iLXuzp7dWVzO",
}


# Mongodb
URI = "mongodb+srv://kafka:1a2b3c4d@us.l7mluba.mongodb.net/?retryWrites=true&w=majority"
DB = "minnesota"
