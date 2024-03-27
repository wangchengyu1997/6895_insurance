from datetime import datetime
from google.cloud import pubsub_v1
import uuid
import praw
import pytz
import time
import json

reddit = praw.Reddit(client_id='ooC7B7QTHMRFkYs1ToW6xw',
                     client_secret='LozZbAV_TZo0hEZG8dXGb_WvMvrpTg',
                     user_agent='6895_app')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('empirical-axon-416223', 'postdata')

# Combine subreddits by names separated with '+'
subreddits = 'Insurance+Car_Insurance_Help+HealthInsurance+LifeInsurance'


def generate_id():
    return str(uuid.uuid4())


def convert_time(unix_time):
    utc_time = datetime.utcfromtimestamp(unix_time)

    # Make the UTC time timezone-aware
    utc_time = utc_time.replace(tzinfo=pytz.UTC)

    # Specify the target local time zone (e.g., 'US/Eastern')
    target_time_zone = pytz.timezone('US/Eastern')

    # Convert to the target local time zone
    local_time = utc_time.astimezone(target_time_zone)

    # Format the datetime as a string in the desired format
    formatted_time = local_time.strftime('%Y-%m-%dT%H:%M:%S')

    return formatted_time


def publish_post_data(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.new(limit=10):  # Fetch the 1000 most recent posts

        post_uuid = generate_id()

        # Publish post data
        post_data = {
            'text': submission.title + submission.selftext,
            'id': post_uuid,
            'createdTime': convert_time(submission.created_utc),
            'type': 'post'
        }
        publisher.publish(topic_path, data=json.dumps(post_data).encode('utf-8'))

        # Fetch and publish comments
        submission.comments.replace_more(limit=0)  # Load all comments; adjust limit as needed
        for comment in submission.comments.list():
            comment_uuid = generate_id()
            comment_data = {
                'text': comment.body,
                'id': comment_uuid,
                'createdTime': convert_time(comment.created_utc),
                'type': 'comment'
            }
            publisher.publish(topic_path, data=json.dumps(comment_data).encode('utf-8'))


while True:
    publish_post_data(subreddits)
    time.sleep(900)  # Wait for 15 minutes before fetching again
