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

def put_record_to_s3(s3_client, bucket_name, key, data):
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data)
        )
    except Exception as e:
        print(f"Error putting record to S3 bucket: {e}")

def rename_files_in_s3(s3_client, bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.json'):
                new_key = obj['Key'].replace('.json', '_renamed.json')
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                    Key=new_key,
                )
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
    except Exception as e:
        print(f"Error renaming files in S3 bucket: {e}")

def main(channel_name):
    # AWS Configuration
    your_s3_bucket_name = "##"  # Replace with your actual S3 bucket name
    aws_region = "us-east-2"
    aws_access_key_id = "##"
    aws_secret_access_key = "##"
    
    
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # YouTube API Configuration
    youtube_api_key = "##"
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
                # Put Records to S3 Bucket
                for video_id in video_ids:
                    key = f"{channel_name}/{video_id}.json"
                    data = {
                        'video_id': video_id,
                        'channel_id': channel_id,
                        'channel_title': snippet['title']
                    }
                    put_record_to_s3(s3_client, "newbucket-terraform", key, data)

                # Rename files in S3 Bucket
                rename_files_in_s3(s3_client, "newbucket-terraform")

                # Open and view files
                for obj in s3_client.list_objects_v2(Bucket="newbucket-terraform")['Contents']:
                    if obj['Key'].endswith('_renamed.json'):
                        file_contents = s3_client.get_object(Bucket="newbucket-terraform", Key=obj['Key'])['Body'].read().decode('utf-8')
                        print(f"Contents of {obj['Key']}: {file_contents}")
            else:
                print("Error: No videos found in the playlist.")
        else:
            print("Error: 'uploads' playlist ID not found in channel details.")
    except KeyError as e:
        print("Error: Missing key in channel details:", e)

if __name__ == "__main__":
    main("Google")
