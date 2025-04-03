# Databricks notebook source
from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker
import json
import time

# Event Hub connection details
connection_str = '<azure_event_hub_end_point>'
eventhub_name = '<event_data_name>'

# Initialize EventHubProducerClient
producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)

# Create a Faker instance to simulate telemetry data
fake = Faker()

# Function to simulate telemetry data
def generate_telemetry_data():
    telemetry_data = {
        'device_id': fake.uuid4(),
        'timestamp': fake.date_time_this_year().isoformat(),
        'temperature': round(fake.random_number(digits=2) + 20.0, 2),
        'humidity': round(fake.random_number(digits=2) + 50.0, 2),
        'status': fake.random_element(elements=("active", "inactive", "error"))
    }
    return telemetry_data

# Send simulated telemetry data to Event Hub
def send_data_to_event_hub():
    event_data_batch = producer.create_batch()
    
    for _ in range(10):  # Send 10 events
        telemetry_data = generate_telemetry_data()
        event_data = EventData(json.dumps(telemetry_data))
        event_data_batch.add(event_data)
    
    producer.send_batch(event_data_batch)
    print(f"Sent telemetry data batch to Event Hub.")

# Run the data simulation for a few iterations
for _ in range(5):  # Simulate and send data 5 times
    send_data_to_event_hub()
    time.sleep(10)  # Sleep for 10 seconds before sending next batch

#Close the producer client
producer.close()