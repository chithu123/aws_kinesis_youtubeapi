import json
import boto3
from googleapiclient.discovery import build

def connect_to_api(api_key):
    api_service_name = "youtube"
    api_version = "v3"
    return build(api_service_name, api_version, developerKey=api_key)

def get_channel_details(api_service, channel_name):
    try:
        request = api_service.channels().list(
            part="snippet,contentDetails",
            maxResults=1,
            forUsername=channel_name,
        )
        response = request.execute()
        if 'items' in response:
            return response['items'][0]
        else:
            print("Error: No channels found matching the provided channel name.")
            return None
    except Exception as e:
        print(f"Error fetching channel details: {e}")
        return None

def get_playlist_videos(api_service, playlist_id):
    try:
        request = api_service.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50
        )
        response = request.execute()
        return [item['contentDetails']['videoId'] for item in response.get('items', [])]
    except Exception as e:
        print(f"Error fetching playlist videos: {e}")
        return []

def put_record_to_kinesis(kinesis_client, stream_name, data):
    try:
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey='1'
        )
    except Exception as e:
        print(f"Error putting record to Kinesis stream: {e}")

def main(channel_name):
    # AWS Configuration
    your_kinesis_stream_name = "youtube_data_project"  # Replace with your actual stream name
    aws_region = "us-east-2"
    aws_access_key_id = "AKIA6C3RWDHYTK3NQPK6"
    aws_secret_access_key = "KqLXcI+PK+D90nyOsHh/LsmCqVWEIEn4uMutqOlH"
    kinesis_stream_arn = "arn:aws:kinesis:us-east-2:968216680945:stream/youtube_data_project"

    # Initialize Kinesis client
    kinesis_client = boto3.client('kinesis', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # YouTube API Configuration
    youtube_api_key = "AIzaSyDrn3gK6lEzC23tsPOQl4n78pAKT7teKUg"
    youtube = connect_to_api(youtube_api_key)

    # Get Channel Details
    channel_details = get_channel_details(youtube, channel_name)
    if not channel_details:
        print("Error: Channel details not found.")
        return

    try:
        channel_id = channel_details['id']
        snippet = channel_details['snippet']
        content_details = channel_details.get('contentDetails', {})
        playlist_data = content_details.get('relatedPlaylists', {})
        playlist_id = playlist_data.get('uploads')
        if playlist_id:
            # Get Playlist Videos
            video_ids = get_playlist_videos(youtube, playlist_id)
            if video_ids:
                # Put Records to Kinesis Stream
                for video_id in video_ids:
                    data = {
                        'video_id': video_id,
                        'channel_id': channel_id,
                        'channel_title': snippet['title']
                    }
                    put_record_to_kinesis(kinesis_client, your_kinesis_stream_name, data)
            else:
                print("Error: No videos found in the playlist.")
        else:
            print("Error: 'uploads' playlist ID not found in channel details.")
    except KeyError as e:
        print("Error: Missing key in channel details:", e)

if __name__ == "__main__":
    main("Google")
