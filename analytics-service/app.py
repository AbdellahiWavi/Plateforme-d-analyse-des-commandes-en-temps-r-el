from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='kafka:19092',
    group_id='analytics-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

total_orders = 0

print("Analytics Service Started")

for message in consumer:

    total_orders += 1

    order = message.value

    print(
        f"Order received : {order['order_id']} "
        f"Total Orders : {total_orders}",
        flush=True
    )