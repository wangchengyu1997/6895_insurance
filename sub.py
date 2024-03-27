from google.cloud import pubsub_v1
from google.cloud import bigtable
import json

# Initialize Bigtable client, instance, and table
bigtable_client = bigtable.Client(project='empirical-axon-416223')
instance = bigtable_client.instance('redditinsurance')
table = instance.table('PostData')

# Initialize Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('empirical-axon-416223', 'postdata-sub')

column_family = 'PostInfo'

def write_to_bigtable(data):
    """Write data to Bigtable. Data is a dictionary containing post or comment info."""
    row_key = data['id'].encode('utf-8')
    row = table.direct_row(row_key)

    # Assuming you have a column family named 'cf1'
    row.set_cell(column_family,
                 'text'.encode('utf-8'),
                 data['text'].encode('utf-8'),
                 timestamp=None)

    row.set_cell(column_family,
                 'createdTime'.encode('utf-8'),
                 data['createdTime'].encode('utf-8'),
                 timestamp=None)

    row.set_cell(column_family,
                 'type'.encode('utf-8'),
                 data['type'].encode('utf-8'),
                 timestamp=None)

    print(f"Writing to Bigtable: {data['id']}")

    row.commit()

def callback(message):
    print(f"Received message: {message}")
    data = json.loads(message.data.decode('utf-8'))

    # Write the message data to Bigtable
    write_to_bigtable(data)

    # Acknowledge the message
    message.ack()

# Subscribe to the topic
subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Keep the main thread running to listen to messages indefinitely
import time
try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print("Exiting...")
