import json
import os
import logging
import boto3
import requests
import base64
from urllib.parse import parse_qs, quote_plus
from botocore.client import Config

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Environment Variables ---
SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
S3_BUCKET_REGION = os.environ.get('S3_BUCKET_REGION')

# --- AWS S3 Client ---
s3_client = boto3.client('s3', region_name=S3_BUCKET_REGION, config=Config(signature_version='s3v4'))


def lambda_handler(event, context):
    """
    Main handler for AWS Lambda.
    Handles Slack events and slash commands.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    logger.info(f"S3 Bucket: {S3_BUCKET_NAME} | Region: {S3_BUCKET_REGION}")

    raw_body = event.get('body', '')
    if event.get('isBase64Encoded', False):
        try:
            raw_body = base64.b64decode(raw_body).decode('utf-8')
        except Exception as e:
            logger.error(f"Base64 decoding failed: {e}")
            return {'statusCode': 400, 'body': 'Bad request body.'}

    # Parse the body
    try:
        body = json.loads(raw_body)
    except (json.JSONDecodeError, TypeError):
        body = parse_qs(raw_body)

    # 1. Slack Challenge Verification
    if body.get('type') == 'url_verification':
        return {'statusCode': 200, 'body': body['challenge']}

    # 2. Handle /s3-fetch command
    if 'command' in body:
        command = body.get('command')[0]
        if command == '/s3-fetch':
            return handle_s3_fetch(body)
        elif command == '/s3-list':
            return handle_s3_list(body)

    # 3. Handle file upload events
    event_data = body.get('event', {})
    if event_data.get('type') == 'file_shared':
        return handle_file_upload(event_data)

    return {'statusCode': 200, 'body': json.dumps({'message': 'Event received.'})}


def handle_file_upload(event_data):
    """
    Uploads files shared in Slack to S3.
    """
    file_id = event_data.get('file_id')
    channel_id = event_data.get('channel_id')

    try:
        file_info = get_file_info(file_id)
        file_url = file_info['file']['url_private_download']
        file_name = file_info['file']['name']

        headers = {'Authorization': f'Bearer {SLACK_BOT_TOKEN}'}
        file_response = requests.get(file_url, headers=headers)
        file_response.raise_for_status()

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=file_response.content
        )
        post_slack_message(channel_id, f"✅ File `{file_name}` uploaded to S3.")

    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        post_slack_message(channel_id, f"❌ Error: {e}")
        return {'statusCode': 500}

    return {'statusCode': 200}


def handle_s3_fetch(body):
    """
    Fetches files from S3 using a presigned URL and posts to Slack.
    """
    try:
        file_name = body['text'][0].strip()
        channel_id = body['channel_id'][0]

        if not file_name:
            return {'statusCode': 200, 'body': 'Please provide a filename. Usage: `/s3-fetch <filename>`'}

        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': S3_BUCKET_NAME,
                'Key': file_name,
                'ResponseContentDisposition': f'attachment; filename="{quote_plus(file_name)}"'
            },
            ExpiresIn=3600
        )

        message = f"🔗 Here is your download link for `{file_name}` (valid for 1 hour):\n{presigned_url}"
        post_slack_message(channel_id, message)
        return {'statusCode': 200, 'body': f"Fetching `{file_name}`..."}

    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {'statusCode': 200, 'body': f"❌ File `{file_name}` not found in S3."}
        logger.error(f"S3 Error: {e}")
        return {'statusCode': 200, 'body': "❌ Error fetching the file."}

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {'statusCode': 500, 'body': 'Internal server error.'}


def handle_s3_list(body):
    """
    Lists all files in the S3 bucket and posts them to Slack.
    """
    try:
        channel_id = body['channel_id'][0]

        # List objects in the bucket
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' not in response:
            post_slack_message(channel_id, "📂 The S3 bucket is empty.")
            return {'statusCode': 200, 'body': "No files found."}

        files = [obj['Key'] for obj in response['Contents']]
        file_list_text = "\n".join([f"- {file}" for file in files])

        post_slack_message(channel_id, f"📂 **Files in S3:**\n{file_list_text}")
        return {'statusCode': 200, 'body': "Files listed."}

    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        return {'statusCode': 500, 'body': 'Error listing files.'}


def get_file_info(file_id):
    """
    Fetches Slack file metadata.
    """
    url = 'https://slack.com/api/files.info'
    headers = {'Authorization': f'Bearer {SLACK_BOT_TOKEN}'}
    response = requests.get(url, headers=headers, params={'file': file_id})
    response.raise_for_status()
    file_info = response.json()
    if not file_info.get('ok'):
        raise Exception(f"Slack API error: {file_info.get('error')}")
    return file_info


def post_slack_message(channel_id, text):
    """
    Sends a message to a Slack channel.
    """
    try:
        response = requests.post(
            'https://slack.com/api/chat.postMessage',
            headers={
                'Authorization': f'Bearer {SLACK_BOT_TOKEN}',
                'Content-Type': 'application/json'
            },
            json={'channel': channel_id, 'text': text}
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post to Slack: {e}")