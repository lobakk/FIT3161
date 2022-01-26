'''
Python file for Tweets scraping
References: https://lucacorbucci.medium.com/how-to-scrape-tweets-using-tweepy-47f4be2b1d
            https://medium.com/analytics-vidhya/scraping-twitter-data-using-tweepy-8005d7b517a3
            https://python.plainenglish.io/scraping-tweets-with-tweepy-python-59413046e788
'''


import tweepy
import pandas as pd
import time
import os

# Setup access to API
def connect_to_twitter_OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET):
    """
    A helper function to connect to the twitter OAuth
    Args:
        ACCESS_TOKEN ([str]): Twitter API access token
        ACCESS_SECRET ([str]): Twitter API access secret key
        CONSUMER_KEY ([str]): Twitter API consumer key
        CONSUMER_SECRET ([str]): Twitter API consumer secret key

    Returns:
        [API]: Twitter API access
    """
    try:
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

        api = tweepy.API(auth)
    except:
        print("Authentication Error")

    return api

# Python Script to Extract tweets of a
# particular Hashtag using Tweepy and Pandas
 
# function to display data of each tweet
def printtweetdata(n, ith_tweet):
    """[summary]

    Args:
        n ([type]): [description]
        ith_tweet ([type]): [description]
    """
    print()
    print(f"Tweet {n}:")
    print(f"Username:{ith_tweet[0]}")
    print(f"Description:{ith_tweet[1]}")
    print(f"Location:{ith_tweet[2]}")
    print(f"Following Count:{ith_tweet[3]}")
    print(f"Follower Count:{ith_tweet[4]}")
    print(f"Total Tweets:{ith_tweet[5]}")
    print(f"Retweet Count:{ith_tweet[6]}")
    print(f"Tweet Text:{ith_tweet[7]}")
    print(f"Hashtags Used:{ith_tweet[8]}")
 
 
def search_by_hashtag(api, date_since, date_until, words):
    """[summary]

    Args:
        api ([type]): [description]
        date_since ([type]): [description]
        date_until ([type]): [description]
        words ([type]): [description]
    """
    df = pd.DataFrame(columns=['id', 'created_at', 'username', 'location', 'following', 
                               'followers', 'retweetcount', 'text']) 
    tweets = tweepy.Cursor(api.search_tweets, q=words, lang="it", wait_on_rate_limit=True,
                           since=date_since, until=date_until, tweet_mode='extended').items() 
    list_tweets = [tweet for tweet in tweets] 
         
    for tweet in list_tweets: 
        id = tweet.id
        created_at = tweet.created_at
        username = tweet.user.screen_name 
        location = tweet.user.location 
        following = tweet.user.friends_count 
        followers = tweet.user.followers_count 
        totaltweets = tweet.user.statuses_count 
        retweetcount = tweet.retweet_count 

        try: 
            text = tweet.retweeted_status.full_text 
        except AttributeError: 
            text = tweet.full_text 
  
        tweets = [id, created_at, username, location, following, 
                     followers, retweetcount, text] 

        df.loc[len(df)] = tweets 
          
    # output filename
    filename = 'scraped_hashtag_tweets_' + date_since + '_00.csv'
    df.to_csv(filename) 

def search_by_hashtag(api, date_since, date_until, words):
    """[summary]

    Args:
        api ([type]): [description]
        date_since ([type]): [description]
        date_until ([type]): [description]
        words ([type]): [description]
    """
    df = pd.DataFrame(columns=['id', 'created_at', 'username', 'location', 'following', 
                               'followers', 'retweetcount', 'text']) 
    tweets = tweepy.Cursor(api.search_tweets, q=words, lang="it", wait_on_rate_limit=True,
                           since=date_since, until=date_until, tweet_mode='extended').items() 
    list_tweets = [tweet for tweet in tweets] 
         
    for tweet in list_tweets: 
        id = tweet.id
        created_at = tweet.created_at
        username = tweet.user.screen_name 
        location = tweet.user.location 
        following = tweet.user.friends_count 
        followers = tweet.user.followers_count 
        totaltweets = tweet.user.statuses_count 
        retweetcount = tweet.retweet_count 

        try: 
            text = tweet.retweeted_status.full_text 
        except AttributeError: 
            text = tweet.full_text 
  
        tweets = [id, created_at, username, location, following, 
                     followers, retweetcount, text] 

        df.loc[len(df)] = tweets 
          
    # output filename
    filename = 'scraped_hashtag_tweets_' + date_since + '_00.csv'
    df.to_csv(filename) 

def search_by_username(api, date_since, date_until, user):
    """[summary]

    Args:
        api ([type]): [description]
        date_since ([type]): [description]
        date_until ([type]): [description]
        user ([type]): [description]
    """
    df = pd.DataFrame(columns=['id', 'created_at', 'username', 'location', 'following', 
                               'followers', 'retweetcount', 'text']) 
    tweets = tweepy.Cursor(api.user_timeline,id=user, lang="it", wait_on_rate_limit=True,
                           since=date_since, until=date_until, tweet_mode='extended').items() 
    list_tweets = [tweet for tweet in tweets] 
         
    for tweet in list_tweets: 
        id = tweet.id
        created_at = tweet.created_at
        username = tweet.user.screen_name 
        location = tweet.user.location 
        following = tweet.user.friends_count 
        followers = tweet.user.followers_count 
        totaltweets = tweet.user.statuses_count 
        retweetcount = tweet.retweet_count 

        try: 
            text = tweet.retweeted_status.full_text 
        except AttributeError: 
            text = tweet.full_text 
  
        tweets = [id, created_at, username, location, following, 
                     followers, retweetcount, text] 

        df.loc[len(df)] = tweets 
          
    # output filename
    filename = 'scraped_'+ username +'_tweets_' + date_since + '_00.csv'
    df.to_csv(filename) 


def scraptweets_weekly(search_words, date_since, numTweets, numRuns):


    # Define a for-loop to generate tweets at regular intervals
    # We cannot make large API call in one go. Hence, let's try T times

    # Define a pandas dataframe to store the date:
    db_tweets = pd.DataFrame(columns=['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    program_start = time.time()
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()

        # Collect tweets using the Cursor object
        # .Cursor() returns an object that you can iterate or loop over to access the data collected.
        # Each item in the iterator has various attributes that you can access to get information about each tweet
        tweets = tweepy.Cursor(api.search_tweets, q=search_words, lang="en",
                               since=date_since, tweet_mode='extended').items(numTweets)
# Store these tweets into a python list
        tweet_list = [tweet for tweet in tweets]
# Obtain the following info (methods to call them out):
        # user.screen_name - twitter handle
        # user.description - description of account
        # user.location - where is he tweeting from
        # user.friends_count - no. of other users that user is following (following)
        # user.followers_count - no. of other users who are following this user (followers)
        # user.statuses_count - total tweets by user
        # user.created_at - when the user account was created
        # created_at - when the tweet was created
        # retweet_count - no. of retweets
        # (deprecated) user.favourites_count - probably total no. of tweets that is favourited by user
        # retweeted_status.full_text - full text of the tweet
        # tweet.entities['hashtags'] - hashtags in the tweet
# Begin scraping the tweets individually:
        noTweets = 0


        for tweet in tweet_list:
        # Pull the values
            username = tweet.user.screen_name
            acctdesc = tweet.user.description
            location = tweet.user.location
            following = tweet.user.friends_count
            followers = tweet.user.followers_count
            totaltweets = tweet.user.statuses_count
            usercreatedts = tweet.user.created_at
            tweetcreatedts = tweet.created_at
            retweetcount = tweet.retweet_count
            hashtags = tweet.entities['hashtags']
            try:
                text = tweet.retweeted_status.full_text
            except AttributeError:  # Not a Retweet
                    text = tweet.full_text
            # Add the 11 variables to the empty list - ith_tweet:
            ith_tweet = [username, acctdesc, location, following, followers, totaltweets,
                            usercreatedts, tweetcreatedts, retweetcount, text, hashtags]
        # Append to dataframe - db_tweets
            db_tweets.loc[len(db_tweets)] = ith_tweet
        # increase counter - noTweets  
            noTweets += 1
        
        # Run ended:
        end_run = time.time()
        duration_run = round((end_run-start_run)/60, 2)
            
        print('no. of tweets scraped for run {} is {}'.format(i + 1, noTweets))
        print('time take for {} run to complete is {} mins'.format(i+1, duration_run))
        
        time.sleep(920) #15 minute sleep time

# Once all runs have completed, save them to a single csv file:
    from datetime import datetime
    
    # Obtain timestamp in a readable format
    to_csv_timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')
    # Define working path and filename
    # path = os.getcwd()
    filename = to_csv_timestamp + '_crypto_tweets.csv'
# Store dataframe in csv with creation date timestamp
    db_tweets.to_csv(filename, index = False)
    
    program_end = time.time()
    print('Scraping has completed!')
    print('Total time taken to scrap is {} minutes.'.format(round(program_end - program_start)/60, 2))
        
        

# Variables that contains the credentials to access Twitter API
ACCESS_TOKEN = '1472638099490439170-yIq47dBq8EcUuO44msP5CuggeeZBml'
ACCESS_SECRET = 'swIQSmcUdqZ8XYlzrvnF08IBuzamva5W6b8dUDuXAFxkT'
CONSUMER_KEY = '5etHstBqMcYpIDESsQPvvhyJO'
CONSUMER_SECRET = 'cgsmFFYBHPZX7QqeauuwQQgdLgM2RvLnMbvDyJoHYCuOEKo0Gd'

# Create API object
api = connect_to_twitter_OAuth(ACCESS_TOKEN,ACCESS_SECRET,CONSUMER_KEY,CONSUMER_SECRET)

# Hashtag and initial date for query
print('Enter hashtag for query: ')
words = input()
print('Enter username for query: ')
user = input()
print('Enter start date for query in the format 20xx-01-1x: ')
date_since = input()
print('Enter end date for query in the format 20xx-01-1x: ')
date_until = input()


#search_by_hashtag(api, date_since, date_until, words)
#search_by_username(api, date_since, date_until, user)
scraptweets_weekly(words, date_since, 900, 20)
print('Scraping has completed!')
