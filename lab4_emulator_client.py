# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
import os

print("Current working directory:", os.getcwd())


# TODO 1: Modify the following parameters
# Starting and end index (modify this)
device_st = 1
device_end = 6

# Path to the dataset (modify this)
data_path = (
    "./data2/vehicle{}.csv"  # Assuming files are named vehicle1.csv, vehicle2.csv, etc.
)

# Path to your certificates (modify this)
certificate_formatter = "./things/Thing-{}/certificate.pem"
key_formatter = "./things/Thing-{}/private.key"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        self.device_id = str(device_id)
        self.state = 0  # Track the current row to publish
        self.client = AWSIoTMQTTClient(self.device_id)
        self.client.configureEndpoint(
            "a2p252eep51bmf-ats.iot.us-east-1.amazonaws.com", 8883
        )
        self.client.configureCredentials(
            "./keys/AmazonRootCA1.pem",
            key,
            cert,
        )
        self.client.configureOfflinePublishQueueing(-1)
        self.client.configureDrainingFrequency(2)
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)
        self.client.onMessage = self.customOnMessage

        # Load the dataset for this device
        self.data = pd.read_csv(data_path.format(self.device_id))

    def customOnMessage(self, message):
        print(
            f"Client {self.device_id} received payload '{message.payload.decode('utf-8')}' from topic '{message.topic}'"
        )

    def customPubackCallback(self, mid):
        pass  # No action needed here

    def publish(self, topic="vehicle/emission/data"):
        # Publish the current row for this client
        if self.state < len(self.data):
            row = self.data.iloc[self.state]
            payload = json.dumps(row.to_dict())
            print(f"Client {self.device_id}: Publishing {payload} to {topic}")
            self.client.publishAsync(
                topic, payload, 0, ackCallback=self.customPubackCallback
            )
            self.state += 1  # Move to the next row
        else:
            print(f"Client {self.device_id}: All data published.")


print("Initializing MQTT Clients...")
clients = []
for device_id in range(device_st, device_end):
    print(f"Attempting to connect to device {device_id}")
    client = MQTTClient(
        device_id,
        certificate_formatter.format(device_id),
        key_formatter.format(device_id),
    )
    client.client.connect()
    clients.append(client)

while True:
    print("Send now? (Press 's' to send, 'd' to disconnect)")
    x = input()
    if x.lower() == "s":
        print(f"Number of clients: {len(clients)}")
        for c in clients:
            print("--------")
            c.publish()  # Publish one message per client
    elif x.lower() == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        break
    else:
        print("Wrong key pressed")
