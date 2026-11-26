import json, random, time, uuid
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PROJECT_ID = "gcp-streaming-pipelinee"
publisher = pubsub_v1.PublisherClient()
topic = publisher.topic_path(PROJECT_ID, "order-events")

products = ["Phone", "Laptop", "Headphones", "Shoes", "Watch"]
cities = ["Hyderabad", "Mumbai", "Delhi", "Bengaluru", "Chennai"]

while True:
    event = {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1, 500)}",
        "product": random.choice(products),
        "amount": round(random.uniform(5, 500), 2),
        "city": random.choice(cities),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    publisher.publish(topic, json.dumps(event).encode()).result()
    print("Sent:", event["order_id"])
    time.sleep(1)
