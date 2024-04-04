
import praw
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from datetime import datetime
from json import dumps
import numpy as np
from nrclex import NRCLex


# initialize sentiment analyzer
nltk.download('vader_lexicon')
analyzer = SentimentIntensityAnalyzer()

reddit = praw.Reddit(client_id='ooC7B7QTHMRFkYs1ToW6xw',
                     client_secret='LozZbAV_TZo0hEZG8dXGb_WvMvrpTg',
                     user_agent='6895_app')

#vader is supposedly more accurate when blocks of text are tokenized into sentences
def analyze_sentiment(text):

    sentences = nltk.sent_tokenize(text)
    compound_scores = []

    for sentence in sentences:
        sentiment = analyzer.polarity_scores(sentence)
        compound_scores.append(sentiment['compound'])

    if compound_scores:
        avg_compound_score = np.mean(compound_scores)
    else:
        avg_compound_score = 0

    return avg_compound_score

def analyze_emotion_nrc(text):
    emotions_sum = {'anger': 0, 'anticip': 0, 'disgust': 0, 'fear': 0, 'joy': 0, 'negative': 0, 'positive': 0,
                    'sadness': 0, 'surprise': 0, 'trust': 0}

    emotion = NRCLex(text).affect_frequencies

    for key in emotions_sum.keys():
        if key in emotion:
            emotions_sum[key] = emotion[key]

    return emotions_sum

def fetch_comments(subreddit_list):
    collection = db.comments
    for subreddit_name in subreddit_list:
        subreddit = reddit.subreddit(subreddit_name)
        for comment in subreddit.stream.comments(skip_existing=True):

            sentiment_score = analyze_sentiment(comment.body)
            emotion_scores = analyze_emotion_nrc(comment.body)

            comment_json = {
                "id": comment.id,
                "name": comment.name,
                "author": comment.author.name if comment.author else "Deleted",
                "body": comment.body,
                "subreddit": comment.subreddit.display_name.lower(),
                "upvotes": comment.ups,
                "downvotes": comment.downs,
                "over_18": comment.over_18,
                "timestamp": comment.created_utc,
                "permalink": comment.permalink,
                "sentiment_score": sentiment_score
            }
            print(comment_json)
            comment_json.update(emotion_scores)
            collection.insert_one(comment_json)


if __name__ == "__main__":
    uri = "mongodb+srv://cluster0.ltaxlm0.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
    client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='./X509-cert.pem')
    db = client['reddit_insurance']
    subreddit_list = ['Insurance', 'Car_Insurance_Help', 'HealthInsurance', 'LifeInsurance']
    print("Fetching comments...")
    for comment in fetch_comments(subreddit_list):
        print(dumps(comment))