import json
import spotipy
import os
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    client_credentials_manager=SpotifyClientCredentials(client_id,client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    playlists=sp.user_playlists('spotify')
    playlst_lnk='https://open.spotify.com/playlist/37i9dQZEVXbLZ52XmnySJg'
    playlst_URI=playlst_lnk.split('/')[-1]
    spotify_data=sp.playlist_tracks(playlst_URI)
    filename= 'spotify_raw_'+ str(datetime.now())+'.json'
    client=boto3.client('s3')
    client.put_object(
        Bucket='spotify-data-etl-harsha',
        Key='raw_data/to_process/'+filename,
        Body=json.dumps(spotify_data)
        )
