import sys
import time
import boto3
import json
import random
import uuid
from datetime import datetime

kinesis = boto3.client('kinesis', region_name="us-east-1")
kinesis_stream = "huarngpa_trade_input"

if __name__ == '__main__':
    print(kinesis.list_shards(StreamName=kinesis_stream))

