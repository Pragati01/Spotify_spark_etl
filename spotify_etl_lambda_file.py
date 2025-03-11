import json
import os
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

import os

os.environ['AWS_CONFIG_FILE'] = '/tmp/aws_config'  # Redirect to writable path
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'  # Change to your region

def lambda_handler(event, context):
    # Retrieve Spotify credentials from environment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    if not client_id or not client_secret:
        return {
            "statusCode": 500,
            "body": json.dumps("Missing Spotify API credentials")
        }

    # Authenticate with Spotify API
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

    # Search for popular tracks in India
    results = sp.search(q='tag:viral', market='IN', type='track', limit=50)

    # Initialize S3 client
    client = boto3.client('s3')

    # Upload data to S3 (ensure the filename is valid)
    file_name="spotify_raw"+ str(datetime.date(datetime.now()))+".json"

    client.put_object(
        Body=json.dumps(results),
        Bucket='spotify-data-bucket-pr',
        Key='raw_data/to_processed/'+file_name
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Data successfully uploaded to S3!")
    }

    