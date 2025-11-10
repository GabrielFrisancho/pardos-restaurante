import boto3
import json
import os

class EventBridge:
    def __init__(self):
        self.client = boto3.client('events')
        self.event_bus_name = os.environ['EVENT_BUS_NAME']
    
    def publish_event(self, source, detail_type, detail):
        return self.client.put_events(
            Entries=[
                {
                    'Source': source,
                    'DetailType': detail_type,
                    'Detail': json.dumps(detail),
                    'EventBusName': self.event_bus_name
                }
            ]
        )
