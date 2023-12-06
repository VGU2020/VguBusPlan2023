from confluent_kafka import Consumer


consumer = Consumer({
    "bootstrap.servers": "pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "WJJNVHLNUDCYW3O6",
    "sasl.password": "WDC0hL9zM7IVxAFMvLftqcMuIFppxGbEFNbbukFqfT8Y3jmZLjl0iLXuzp7dWVzO",
    "group.id": "bus",
    "auto.offset.reset": "earliest"
})